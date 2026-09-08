package ratelimiter

import (
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAllowsUpToLimit(t *testing.T) {
	r := NewRateLimiter(3)

	for i := 1; i <= 3; i++ {
		if !r.AllowRequest() {
			t.Fatalf("request %d was denied, want allowed", i)
		}
	}
	if r.AllowRequest() {
		t.Fatal("request 4 was allowed, want denied")
	}
}

func TestResetsNextSecond(t *testing.T) {
	r := NewRateLimiter(1)

	if !r.AllowRequest() {
		t.Fatal("first request was denied")
	}
	if r.AllowRequest() {
		t.Fatal("second request in the same second was allowed")
	}

	time.Sleep(time.Second)

	if !r.AllowRequest() {
		t.Fatal("request in the next second was denied")
	}
}

func TestConcurrentNeverExceedsLimit(t *testing.T) {
	const (
		limit      = 100
		goroutines = 200
		perRoutine = 50
	)
	r := NewRateLimiter(limit)

	var mu sync.Mutex
	allowed := 0

	startSecond := time.Now().Unix()
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perRoutine; j++ {
				if r.AllowRequest() {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	if time.Now().Unix() == startSecond {
		if allowed != limit {
			t.Fatalf("allowed %d calls within one second, want exactly %d", allowed, limit)
		}
	} else if allowed > limit*2 {
		t.Fatalf("allowed %d calls across two seconds, want at most %d", allowed, limit*2)
	}
}

const (
	loadLimit    = 10
	loadWorkers  = 50
	loadDuration = 3 * time.Second
	loadTick     = 500 * time.Millisecond
	loadBackoff  = time.Millisecond
	loadAPICall  = 5 * time.Millisecond
)

var logger = log.New(os.Stdout, "", 0)

func stamp() string { return time.Now().Format("15:04:05.000") }

func TestLoad(t *testing.T) {
	limiter := NewRateLimiter(loadLimit)

	var allowed, rejected atomic.Int64
	var mu sync.Mutex
	allowedPerSecond := map[int64]int{}
	rejectedPerSecond := map[int64]int{}

	logger.Printf("%s  starting %d workers, limit %d req/sec, running for %s",
		stamp(), loadWorkers, loadLimit, loadDuration)
	logger.Printf("%s  logging every allowed request; rejections are summarised every %s\n",
		stamp(), loadTick)

	done := make(chan struct{})
	var reporter sync.WaitGroup
	reporter.Add(1)
	go func() {
		defer reporter.Done()
		ticker := time.NewTicker(loadTick)
		defer ticker.Stop()

		var lastRejected int64
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				total := rejected.Load()
				logger.Printf("%s  [summary] allowed=%d rejected=%d (+%d rejected in last %s)",
					stamp(), allowed.Load(), total, total-lastRejected, loadTick)
				lastRejected = total
			}
		}
	}()

	var wg sync.WaitGroup
	start := time.Now()

	for id := 1; id <= loadWorkers; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for time.Since(start) < loadDuration {
				ok := limiter.AllowRequest()
				second := time.Now().Unix()

				mu.Lock()
				if ok {
					allowedPerSecond[second]++
					n := allowedPerSecond[second]
					mu.Unlock()
					allowed.Add(1)
					logger.Printf("%s  worker %02d  ALLOWED  %d/%d for this second", stamp(), id, n, loadLimit)
					time.Sleep(loadAPICall)
				} else {
					rejectedPerSecond[second]++
					mu.Unlock()
					rejected.Add(1)
					time.Sleep(loadBackoff)
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
		if allowedPerSecond[s] > loadLimit {
			t.Errorf("second %s allowed %d calls, want at most %d", time.Unix(s, 0).Format("15:04:05"), allowedPerSecond[s], loadLimit)
		}
		logger.Printf("  %s  allowed %2d  rejected %6d", time.Unix(s, 0).Format("15:04:05"), allowedPerSecond[s], rejectedPerSecond[s])
	}
	logger.Printf("\n  total: %d allowed, %d rejected", allowed.Load(), rejected.Load())
}
