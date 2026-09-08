package main

import (
	"fmt"
	"sync"
	"time"
)

// RateLimiter allows at most Limit calls per second.
// One instance is shared by every goroutine making calls to a channel.
type RateLimiter struct {
	mu     sync.Mutex
	limit  int
	second int64 // unix second we are currently counting in
	count  int   // calls allowed so far in that second
}

// NewRateLimiter returns a limiter allowing limit calls per second.
func NewRateLimiter(limit int) *RateLimiter {
	return &RateLimiter{limit: limit}
}

// AllowRequest reports whether a call may go out now, and counts it if so.
// Safe to call from any number of goroutines at once.
func (r *RateLimiter) AllowRequest() bool {
	now := time.Now().Unix()

	// The lock covers the whole read-check-increment, so two goroutines can
	// never both see count == limit-1 and both decide they are allowed.
	r.mu.Lock()
	defer r.mu.Unlock()

	if now != r.second {
		r.second = now
		r.count = 0
	}
	if r.count >= r.limit {
		return false
	}
	r.count++
	return true
}

func main() {
	limiter := NewRateLimiter(5)

	allowed := 0
	for i := 0; i < 20; i++ {
		if limiter.AllowRequest() {
			allowed++
		}
	}
	fmt.Printf("allowed %d of 20 requests in this second (limit 5)\n", allowed)
}
