# Rate limiter

`AllowRequest()` returns true if we are still under the limit of N calls for the
current second, false otherwise. One limiter is shared by all goroutines.

```go
limiter := NewRateLimiter(100) // 100 calls per second

if limiter.AllowRequest() {
    callChannelAPI()
}
```

```bash
go test -race ./...   # tests, with the race detector
go run .              # tiny demo
```

## How it is safe for many threads at once

The state is a counter plus the second it belongs to. The bug to avoid is that
"check the counter, then increment it" is two operations: two goroutines can
both read `count == limit-1`, both decide there is room, and both proceed.

So a single `sync.Mutex` is held across the whole check-and-increment in
`AllowRequest`. Nothing reads or writes the counter outside that lock, so the
decision and the increment it is based on can never be split by another
goroutine. `go test -race` covers this with 200 goroutines making 10,000
attempts against a limit of 100.

## What I was unsure about

The limit resets on the clock second, so 100 calls at 12:00:00.999 and 100 more
at 12:00:01.001 are both legal by this rule, even though the channel sees 200
calls in 2ms. That is the standard fixed-window behaviour and matches how "N
calls per second" is usually documented. If a channel enforces a true rolling
window, this would need to keep the timestamps of the last N calls instead of a
single counter.

It also uses the wall clock, so an NTP jump backwards would start a fresh
window early. And it limits one process only — with several replicas running,
each gets its own budget, so a shared counter (Redis) would be needed.
