package main

import (
	"fmt"
	"sort"
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
	const (
		limit    = 10
		workers  = 50
		duration = 3 * time.Second
	)

	limiter := NewRateLimiter(limit)

	var mu sync.Mutex
	allowed := map[int64]int{}
	rejected := map[int64]int{}

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(start) < duration {
				ok := limiter.AllowRequest()
				second := time.Now().Unix()

				mu.Lock()
				if ok {
					allowed[second]++
				} else {
					rejected[second]++
				}
				mu.Unlock()

				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()

	seconds := make([]int64, 0, len(allowed))
	for s := range allowed {
		seconds = append(seconds, s)
	}
	sort.Slice(seconds, func(i, j int) bool { return seconds[i] < seconds[j] })

	fmt.Printf("%d goroutines sharing one limiter, limit %d requests/second\n\n", workers, limit)

	totalAllowed, totalRejected := 0, 0
	for i, s := range seconds {
		fmt.Printf("second %d: allowed %2d, rejected %d\n", i+1, allowed[s], rejected[s])
		totalAllowed += allowed[s]
		totalRejected += rejected[s]
	}

	fmt.Printf("\ntotal: %d allowed, %d rejected\n", totalAllowed, totalRejected)
}
