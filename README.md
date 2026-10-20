# Rate limiter

A rate limiter for the sales-channel API calls. It caps us at N calls per
second. There are two implementations: an in-process limiter for callers inside
one program, and a Redis-backed limiter that shares one budget across every
process.

```go
limiter := NewRateLimiter(100) // 100 calls per second

if limiter.AllowRequest() {
    callChannelAPI()
}
```

`AllowRequest()` returns true if we're still under the limit for the current
second, false if we're not. It doesn't block or sleep, it just answers the
question and returns.

The shared version:

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
limiter, err := NewRedisLimiter(client, RedisLimiterConfig{
    Limit:  100,
    Key:    "sales-channel",
    Policy: FailClosed, // the default
})

allowed, err := limiter.Allow(ctx)
if err != nil {
    // Redis couldn't be reached; allowed reflects the failure policy.
}
```

Every process using the same Redis and the same `Key` shares one budget of
`Limit` calls per second. A nil error means Redis answered: `false` is a normal
rate-limit denial. A non-nil error means Redis could not be reached within the
retry budget, and `allowed` is whatever the configured `FailurePolicy` says.

Written in Go. The shared limiter uses `go-redis`; the tests use `miniredis`.

## Files

`main.go` is the in-process limiter, about thirty lines. `distributed.go` is
the Redis-backed one. The correctness tests and the load simulation for the
local limiter are in `main_test.go`. The shared limiter has hermetic tests in
`distributed_test.go` and an opt-in live test in `redis_integration_test.go`.
`go.mod` and `go.sum` pin the two dependencies.

## Running it

```bash
go test -race ./...     # all the tests, with the race detector on
go test -v -run Load    # the local load simulation, with a live log
REDIS_ADDR=localhost:6379 go test -v -run Live   # optional real-Redis test
```

The default suite needs no network and no running services. The live test is
skipped unless `REDIS_ADDR` is set; it also reads `REDIS_PASSWORD` if one is
needed. The load run needs `-v`, because `go test` throws away anything a
passing test prints unless you ask for it. And if you run the same thing twice
in a row Go will replay a cached result instead of running it again, so add
`-count=1` when you want it to actually go.

## What the tests do

Three tests cover the local limiter's plain correctness: N calls get through
and the next one doesn't, the counter resets when the second rolls over, and
the concurrency one, which throws 200 goroutines at a single limiter with 50
calls each. That's 10,000 attempts fighting over 100 slots. If the whole run
lands inside one clock second it checks that exactly 100 got through, not
roughly 100.

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

The shared limiter's tests freeze the Redis clock and run four independent
clients against one key. 160 concurrent calls fight over 100 slots and exactly
100 get through, which is the property the local limiter can't provide. The
rest cover the window rolling over, key expiry, a transient Redis failure that
clears on retry, both failure policies after the retries are exhausted, the
per-attempt timeout, context cancellation, and config validation. None of it
touches a real Redis server, so it stays fast and deterministic in CI.

## How the shared limiter stays correct

Each call runs one Lua script in Redis. The script asks Redis for the current
second, increments a hash field named after that second, gives the hash a
two-second expiry when the field is new, and returns whether the count is still
inside the limit. Because Redis runs the script atomically, two instances
cannot both look at a count of 99 with a limit of 100 and both decide there is
room. That read-then-increment race was the reason the local limiter needed a
mutex; over the network, the script is the mutex.

Using Redis's clock instead of each host's clock means two machines with
slightly different clocks still agree on where the window boundary is. The
window state is a single hash key, so the script works in Redis Cluster as
well: no derived keys, no cross-slot access. The TTL is longer than one window
so the hash survives its own window, and old fields disappear when the hash
expires.

## When Redis is unavailable

The shared limiter is now on the request path, so its failure mode is part of
the design, not an afterthought. Every attempt has a timeout (25ms by default),
and a failed attempt is retried with exponential backoff plus jitter, three
attempts by default, capped at one second between tries. If every attempt
fails there are two policies:

- `FailClosed` (the default and the zero value) denies the request. We stay
  under the channel's limit, at the cost of refusing calls while Redis is down.
- `FailOpen` allows the request. Our service stays available, but the shared
  limit is not being enforced and the channel sees the full rate.

Either way `Allow` returns the underlying error, so the caller can log it or
count it instead of guessing why a decision was made. A caller whose context is
already canceled is not retried at all, and cancellation during a backoff stops
the retries immediately.

## How I made the local limiter safe for concurrent callers

The state is just two things: the second we're currently counting in, and how
many calls we've allowed in that second. The tricky part isn't the counter
itself, it's that deciding "is there room?" and then acting on that decision are
two separate steps. If two threads both read the counter at 99 with a limit of
100, they'll both think there's room and both go ahead, and now we've made 101
calls.

So I put a mutex around the whole thing. When a thread calls `AllowRequest`, it
takes the lock before it looks at anything, and it holds that lock for the
entire critical section: reading the clock, checking whether the second has
rolled over, resetting the counter if it has, comparing the count against the
limit, and incrementing it. All of that happens with the lock held. Only one
thread can be inside that section at a time, so every thread sees a counter
that nobody else is halfway through changing.

The clock read was originally outside the lock, and that was a bug. A thread
could read the time, wait for the lock while the second rolled over, and then
acquire it holding a stale second. Since the second differs from the one in the
counter, it would reset the counter and hand out another full budget, moving
the limiter's window backwards. Under enough contention at a boundary that
could happen repeatedly, and `TestLoad` caught it letting 27 calls through in a
second with a limit of 10. Reading the clock inside the lock makes the time and
the counter consistent again. The race detector is clean, and the load test now
passes repeatedly.

## What I wasn't sure about

**What "per second" is supposed to mean.** This is the main thing I got stuck
on, because it can mean two different things and they behave very differently.

One reading is fixed second boundaries: the counter resets at the start of every
clock second. The other is a rolling window: at any moment, look back one second
and count. The problem with fixed boundaries is that you can send all N of your
calls in the last millisecond of a second, and then the clock ticks over, the
window resets, and you can immediately send N more. So the channel sees 2N calls
inside about two milliseconds, even though we never technically broke the rule.
The rolling version doesn't have that hole, but it costs more, because you have
to remember the timestamps of the last N calls instead of keeping one number.

I went with the fixed-boundary version in both limiters. It's the simpler
implementation, and it's usually what an API means when the docs say "N requests
per second". The shared limiter doesn't remove the boundary burst; it only
removes the per-process multiplication of the budget. If a channel turns out to
be strict about it, both would need the rolling version, which means keeping a
ring buffer of the last N timestamps locally and a sorted set of them in Redis.

**Clock jumps.** The local limiter uses the wall clock, since that's what lines
up with the seconds the channel is counting in. But the wall clock can move,
usually when NTP corrects it. If it jumps backwards, the second we're on
changes, the counter resets, and we hand out a fresh batch of N calls earlier
than we should have. The shared limiter avoids host clock skew by taking the
time from Redis, but Redis's clock can be corrected too. I left that as is
because it's rare and the fix depends on which behaviour you'd actually want,
but it's a real hole and I'd rather flag it than pretend it isn't there.

**One process or many.** The local limiter keeps its count in memory, so it is
correct for threads inside one process and nothing more. The Redis limiter is
the answer for multiple machines: they all increment the same hash and share
one budget. That adds an operational dependency and a network round-trip to
every allowed call, and the failure policy above decides what happens when
Redis is down. It is also still one budget per `Key`, not one per tenant; if
each channel needs its own budget, that would be one limiter instance per
channel rather than a new mechanism.
