package ratelimiter

import "time"

// Algorithm selects how a limiter distributes a per-second budget.
type Algorithm int

const (
	// FixedWindow resets the whole budget at the start of each wall-clock
	// second. It is the zero value and the default.
	FixedWindow Algorithm = iota

	// GCRA paces requests evenly instead of resetting at second boundaries.
	// A limiter with limit N admits N requests back-to-back after an idle
	// period, then one request every 1/N seconds.
	GCRA
)

// Decision is the outcome of one rate-limit check.
//
// Allowed reports whether the request may proceed. Limit echoes the configured
// budget, and Remaining is the budget left after this check. When Allowed is
// false, RetryAfter is how long to wait before the next request is likely to
// be admitted.
//
// Remaining and RetryAfter are advisory snapshots. With a shared limiter other
// processes can consume or refill budget concurrently.
type Decision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
}
