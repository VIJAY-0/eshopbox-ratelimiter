package ratelimiter

import (
	"fmt"
	"sync"
	"time"
)

// RateLimiter enforces a per-second request budget for callers inside one
// process. Use RedisLimiter when several processes must share a budget.
type RateLimiter struct {
	mu    sync.Mutex
	limit int
	algo  Algorithm
	now   func() time.Time

	second int64
	count  int

	tat time.Time
}

// NewRateLimiter returns a fixed-window limiter that allows limit requests per
// wall-clock second. It panics if limit is not positive.
func NewRateLimiter(limit int) *RateLimiter {
	return newRateLimiter(limit, FixedWindow, time.Now)
}

// NewGCRARateLimiter returns a limiter that paces requests smoothly at limit
// per second instead of resetting at clock boundaries. It panics if limit is
// not positive.
func NewGCRARateLimiter(limit int) *RateLimiter {
	return newRateLimiter(limit, GCRA, time.Now)
}

func newRateLimiter(limit int, algo Algorithm, now func() time.Time) *RateLimiter {
	if limit <= 0 {
		panic(fmt.Sprintf("ratelimiter: limit must be positive, got %d", limit))
	}
	if now == nil {
		now = time.Now
	}
	return &RateLimiter{limit: limit, algo: algo, now: now}
}

// AllowRequest reports whether a request may proceed. Every call that returns
// true consumes one request from the budget.
func (r *RateLimiter) AllowRequest() bool {
	return r.Decide().Allowed
}

// Decide reports whether a request may proceed and, when allowed, how much
// budget is left. When denied, RetryAfter is how long to wait before asking
// again. Decide never blocks, and every allowed call consumes one request.
func (r *RateLimiter) Decide() Decision {
	r.mu.Lock()
	defer r.mu.Unlock()

	// The clock is read under the lock so a window rollover cannot race the
	// counter reset.
	now := r.now()
	if r.algo == GCRA {
		return r.decideGCRA(now)
	}
	return r.decideFixed(now)
}

func (r *RateLimiter) decideFixed(now time.Time) Decision {
	second := now.Unix()
	if second != r.second {
		r.second = second
		r.count = 0
	}
	if r.count >= r.limit {
		return Decision{
			Limit:      r.limit,
			RetryAfter: now.Truncate(time.Second).Add(time.Second).Sub(now),
		}
	}
	r.count++
	return Decision{Allowed: true, Limit: r.limit, Remaining: r.limit - r.count}
}

// decideGCRA implements the generic cell rate algorithm. The theoretical
// arrival time (TAT) is the earliest time at which the budget is fully
// replenished; emission is the spacing between requests and tolerance is how
// far ahead of TAT a burst may run.
func (r *RateLimiter) decideGCRA(now time.Time) Decision {
	emission := (time.Second + time.Duration(r.limit) - 1) / time.Duration(r.limit)
	tolerance := time.Duration(r.limit-1) * emission

	if r.tat.Before(now) {
		r.tat = now
	}
	if allowAt := r.tat.Add(-tolerance); now.Before(allowAt) {
		return Decision{Limit: r.limit, RetryAfter: allowAt.Sub(now)}
	}
	r.tat = r.tat.Add(emission)

	remaining := int(now.Add(tolerance).Sub(r.tat)/emission) + 1
	if remaining < 0 {
		remaining = 0
	}
	if remaining > r.limit {
		remaining = r.limit
	}
	return Decision{Allowed: true, Limit: r.limit, Remaining: remaining}
}
