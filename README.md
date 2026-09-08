# Rate limiter

A rate limiter for the sales-channel API calls. It caps us at N calls per
second, and a single instance is meant to be shared by every thread that wants
to make a call.

```go
limiter := NewRateLimiter(100) // 100 calls per second

if limiter.AllowRequest() {
    callChannelAPI()
}
```

`AllowRequest()` returns true if we're still under the limit for the current
second, false if we're not. It doesn't block or sleep, it just answers the
question and returns.

Written in Go. Nothing outside the standard library.

## Files

There are only two. `main.go` is the rate limiter and nothing else, about
thirty lines. Everything else, the tests and the load simulation and all the
logging, is in `main_test.go`.

## Running it

```bash
go test -race ./...     # all the tests, with the race detector on
go test -v -run Load    # the load simulation, with a live log
```

The load run needs `-v`, because `go test` throws away anything a passing test
prints unless you ask for it. And if you run the same thing twice in a row Go
will replay a cached result instead of running it again, so add `-count=1` when
you want it to actually go.

## What the tests do

Three of them are the plain correctness checks: N calls get through and the
next one doesn't, the counter resets when the second rolls over, and the
concurrency one, which throws 200 goroutines at a single limiter with 50 calls
each. That's 10,000 attempts fighting over 100 slots. If the whole run lands
inside one clock second it checks that exactly 100 got through, not roughly 100.
I sanity-checked that test by deleting the lock from a copy of the code, and it
failed there (it let 102 through), so the test is actually testing what I think
it's testing.

`TestLoad` is the one worth watching. It runs 50 workers against a limit of 10
per second for three seconds, logs every call that gets allowed, and fails if
any second ever goes above the limit. Rejections aren't logged one by one,
there are about 130,000 of them and they'd bury everything, so they get counted
and summarised every 500ms instead:

```
23:23:56.000  worker 14  ALLOWED  1/10 for this second
23:23:56.000  worker 02  ALLOWED  2/10 for this second
...
23:23:56.482  [summary] allowed=20 rejected=21760 (+21760 rejected in last 500ms)

  23:23:56  allowed 10  rejected  43174
  23:23:57  allowed 10  rejected  43615
  23:23:58  allowed 10  rejected  37571

  total: 40 allowed, 130462 rejected
```

The timestamps are the interesting part. The ten allowed calls all land in the
same millisecond right after each second ticks over, and then nothing gets
through until the next one.

## How I made it safe for concurrent callers

The state is just two things: the second we're currently counting in, and how
many calls we've allowed in that second. The tricky part isn't the counter
itself, it's that deciding "is there room?" and then acting on that decision are
two separate steps. If two threads both read the counter at 99 with a limit of
100, they'll both think there's room and both go ahead, and now we've made 101
calls.

So I put a mutex around the whole thing. When a thread calls `AllowRequest`, it
takes the lock before it looks at anything, and it holds that lock for the
entire critical section: checking whether the second has rolled over, resetting
the counter if it has, comparing the count against the limit, and incrementing
it. All of that happens with the lock held. Only one thread can be inside that
section at a time, so every thread sees a counter that nobody else is halfway
through changing, and the increment always follows from the value that same
thread just read. Nothing anywhere else touches the counter, so there's no way
to read or write it outside the lock.

## What I wasn't sure about

**What "per second" is supposed to mean.** This was the main thing I got stuck
on, because it can mean two different things and they behave very differently.

One reading is fixed second boundaries: the counter resets at the start of every
clock second. The other is a rolling window: at any moment, look back one second
and count. The problem with fixed boundaries is that you can send all N of your
calls in the last millisecond of a second, and then the clock ticks over, the
window resets, and you can immediately send N more. So the channel sees 2N calls
inside about two milliseconds, even though we never technically broke the rule.
The rolling version doesn't have that hole, but it costs more, because you have
to remember the timestamps of the last N calls instead of keeping one number.

I went with the fixed-boundary version. It's the simpler implementation, and
it's usually what an API means when the docs say "N requests per second". But
it is genuinely a design decision, and which one is right depends on how the
channel on the other end is actually counting. If a channel turns out to be
strict about it, this would need to become the rolling version, which means
keeping a ring buffer of the last N timestamps instead of a counter.

**Clock jumps.** I'm using the wall clock, since that's what lines up with the
seconds the channel is counting in. But the wall clock can move, usually when
NTP corrects it. If it jumps backwards, the second we're on changes, the counter
resets, and we hand out a fresh batch of N calls earlier than we should have.
Same thing on a forward jump, we just start a new window early. I left it as is
because it's rare and the fix depends on which behaviour you'd actually want,
but it's a real hole and I'd rather flag it than pretend it isn't there.

**One process only.** This keeps the count in memory, so it's correct for
threads inside one process, which is what the question asked for. If inventory
sync runs on more than one machine, each one gets its own budget of N and the
channel sees the total. Making it work across machines means moving the counter
somewhere shared like Redis, and the same read-then-increment problem shows up
there too, just over a network instead of between threads.
