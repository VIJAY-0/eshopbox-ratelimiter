package main

import (
	"fmt"
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
