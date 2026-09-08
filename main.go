package ratelimiter

import (
	"sync"
	"time"
)

type RateLimiter struct {
	mu     sync.Mutex
	limit  int
	second int64
	count  int
}

func NewRateLimiter(limit int) *RateLimiter {
	return &RateLimiter{limit: limit}
}

func (r *RateLimiter) AllowRequest() bool {
	now := time.Now().Unix()

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
