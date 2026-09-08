package main

import (
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
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

const (
	limit    = 10
	workers  = 50
	duration = 3 * time.Second
	tick     = 500 * time.Millisecond
	backoff  = time.Millisecond
	apiCall  = 5 * time.Millisecond
)

var logger = log.New(os.Stdout, "", 0)

func stamp() string { return time.Now().Format("15:04:05.000") }

func main() {
	limiter := NewRateLimiter(limit)

	var allowed, rejected atomic.Int64
	var mu sync.Mutex
	allowedPerSecond := map[int64]int{}
	rejectedPerSecond := map[int64]int{}

	logger.Printf("%s  starting %d workers, limit %d req/sec, running for %s",
		stamp(), workers, limit, duration)
	logger.Printf("%s  logging every allowed request; rejections are summarised every %s\n",
		stamp(), tick)

	done := make(chan struct{})
	var reporter sync.WaitGroup
	reporter.Add(1)
	go func() {
		defer reporter.Done()
		ticker := time.NewTicker(tick)
		defer ticker.Stop()

		var lastRejected int64
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				total := rejected.Load()
				logger.Printf("%s  [summary] allowed=%d rejected=%d (+%d rejected in last %s)",
					stamp(), allowed.Load(), total, total-lastRejected, tick)
				lastRejected = total
			}
		}
	}()

	var wg sync.WaitGroup
	start := time.Now()

	for id := 1; id <= workers; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Since(start) < duration {
				ok := limiter.AllowRequest()
				second := time.Now().Unix()

				mu.Lock()
				if ok {
					allowedPerSecond[second]++
					n := allowedPerSecond[second]
					mu.Unlock()
					allowed.Add(1)
					logger.Printf("%s  worker %02d  ALLOWED  %d/%d for this second", stamp(), id, n, limit)
					time.Sleep(apiCall)
				} else {
					rejectedPerSecond[second]++
					mu.Unlock()
					rejected.Add(1)
					time.Sleep(backoff)
				}
			}
		}(id)
	}
	wg.Wait()
	close(done)
	reporter.Wait()

	seconds := make([]int64, 0, len(allowedPerSecond))
	for s := range allowedPerSecond {
		seconds = append(seconds, s)
	}
	sort.Slice(seconds, func(i, j int) bool { return seconds[i] < seconds[j] })

	logger.Printf("\n%s  finished in %s\n", stamp(), time.Since(start).Round(time.Millisecond))
	for _, s := range seconds {
		status := "ok"
		if allowedPerSecond[s] > limit {
			status = "OVER LIMIT"
		}
		logger.Printf("  %s  allowed %2d  rejected %6d  %s",
			time.Unix(s, 0).Format("15:04:05"), allowedPerSecond[s], rejectedPerSecond[s], status)
	}
	logger.Printf("\n  total: %d allowed, %d rejected", allowed.Load(), rejected.Load())
}
