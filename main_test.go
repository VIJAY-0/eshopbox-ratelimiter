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

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{t: t}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

var testStart = time.Unix(1_700_000_000, 250_000_000)

func TestAllowsUpToLimit(t *testing.T) {
	clock := newFakeClock(testStart)
	r := newRateLimiter(3, FixedWindow, clock.Now)

	for i := 1; i <= 3; i++ {
		if !r.AllowRequest() {
			t.Fatalf("request %d was denied, want allowed", i)
		}
	}
	if r.AllowRequest() {
		t.Fatal("4th request was allowed, want denied")
	}
}

func TestFixedWindowResetsNextSecond(t *testing.T) {
	clock := newFakeClock(testStart)
	r := newRateLimiter(1, FixedWindow, clock.Now)

	allowLocalOnce(t, r, true)
	allowLocalOnce(t, r, false)

	clock.Advance(time.Second)
	allowLocalOnce(t, r, true)
}

func TestFixedWindowDecisionMetadata(t *testing.T) {
	clock := newFakeClock(testStart) // 0.25s into the second
	r := newRateLimiter(3, FixedWindow, clock.Now)

	d := r.Decide()
	if !d.Allowed || d.Limit != 3 || d.Remaining != 2 || d.RetryAfter != 0 {
		t.Fatalf("first Decision = %+v, want allowed with 2 remaining", d)
	}

	r.Decide()
	r.Decide()

	d = r.Decide()
	if d.Allowed || d.Limit != 3 || d.Remaining != 0 {
		t.Fatalf("denied Decision = %+v, want denied with 0 remaining", d)
	}
	if d.RetryAfter != 750*time.Millisecond {
		t.Fatalf("RetryAfter = %s, want 750ms", d.RetryAfter)
	}

	clock.Advance(d.RetryAfter)
	d = r.Decide()
	if !d.Allowed || d.Remaining != 2 {
		t.Fatalf("Decision after retry = %+v, want allowed with 2 remaining", d)
	}
}

func TestGCRAAllowsBurstThenPaces(t *testing.T) {
	clock := newFakeClock(testStart)
	r := newRateLimiter(4, GCRA, clock.Now) // one request every 250ms

	for i, want := range []int{3, 2, 1, 0} {
		d := r.Decide()
		if !d.Allowed || d.Remaining != want {
			t.Fatalf("burst request %d = %+v, want allowed with %d remaining", i+1, d, want)
		}
	}

	d := r.Decide()
	if d.Allowed || d.RetryAfter != 250*time.Millisecond {
		t.Fatalf("5th request = %+v, want denied with 250ms retry", d)
	}

	clock.Advance(249 * time.Millisecond)
	if d = r.Decide(); d.Allowed || d.RetryAfter != time.Millisecond {
		t.Fatalf("request 1ms early = %+v, want denied with 1ms retry", d)
	}

	clock.Advance(time.Millisecond)
	if d = r.Decide(); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("request on the emission boundary = %+v, want allowed with 0 remaining", d)
	}
}

func TestGCRAIdleRefillsBurst(t *testing.T) {
	clock := newFakeClock(testStart)
	r := newRateLimiter(4, GCRA, clock.Now)

	for i := 0; i < 4; i++ {
		allowLocalOnce(t, r, true)
	}
	allowLocalOnce(t, r, false)

	clock.Advance(10 * time.Second)
	for i := 0; i < 4; i++ {
		allowLocalOnce(t, r, true)
	}
	allowLocalOnce(t, r, false)
}

func TestGCRALimitOne(t *testing.T) {
	clock := newFakeClock(testStart)
	r := newRateLimiter(1, GCRA, clock.Now)

	d := r.Decide()
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("first Decision = %+v, want allowed with 0 remaining", d)
	}

	d = r.Decide()
	if d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("second Decision = %+v, want denied with 1s retry", d)
	}

	clock.Advance(999 * time.Millisecond)
	if d = r.Decide(); d.Allowed || d.RetryAfter != time.Millisecond {
		t.Fatalf("Decision 1ms early = %+v, want denied with 1ms retry", d)
	}

	clock.Advance(time.Millisecond)
	allowLocalOnce(t, r, true)
	allowLocalOnce(t, r, false)
}

func TestConcurrentFixedNeverExceedsLimit(t *testing.T) {
	const (
		limit      = 100
		goroutines = 200
		perRoutine = 50
	)
	r := NewRateLimiter(limit)
	startSecond := time.Now().Unix()

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perRoutine; j++ {
				if r.AllowRequest() {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	got := allowed.Load()
	if time.Now().Unix() == startSecond {
		// Whole run inside one second: exactly `limit` must have got through.
		if got != limit {
			t.Fatalf("allowed %d calls within one second, want exactly %d", got, limit)
		}
	} else if got > limit*2 {
		// Crossed a second boundary, so up to two windows may have opened.
		t.Fatalf("allowed %d calls across two seconds, want at most %d", got, limit*2)
	}
}

func TestConcurrentGCRAExactlyLimit(t *testing.T) {
	const (
		limit      = 100
		goroutines = 200
		perRoutine = 50
	)
	clock := newFakeClock(testStart)
	r := newRateLimiter(limit, GCRA, clock.Now)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perRoutine; j++ {
				if r.AllowRequest() {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != limit {
		t.Fatalf("allowed %d calls at one instant, want exactly %d", got, limit)
	}
}

func TestRateLimiterRejectsNonPositiveLimit(t *testing.T) {
	tests := []struct {
		name string
		new  func()
	}{
		{"fixed zero", func() { NewRateLimiter(0) }},
		{"fixed negative", func() { NewRateLimiter(-1) }},
		{"gcra zero", func() { NewGCRARateLimiter(0) }},
		{"gcra negative", func() { NewGCRARateLimiter(-1) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("constructor did not panic")
				}
			}()
			tc.new()
		})
	}
}

func allowLocalOnce(t *testing.T, r *RateLimiter, want bool) {
	t.Helper()
	if got := r.AllowRequest(); got != want {
		t.Fatalf("AllowRequest = %v, want %v", got, want)
	}
}

// ----- TestLoad configuration and helpers -----

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

	logger.Printf("%s  starting %d workers, limit %d req/sec, running for %s", stamp(), loadWorkers, loadLimit, loadDuration)
	logger.Printf("%s  logging every allowed request; rejections are summarised every %s\n", stamp(), loadTick)

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
				logger.Printf("%s  [summary] allowed=%d rejected=%d (+%d rejected in last %s)", stamp(), allowed.Load(), total, total-lastRejected, loadTick)
				lastRejected = total
			}
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
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
					continue
				}
				rejectedPerSecond[second]++
				mu.Unlock()
				rejected.Add(1)
				time.Sleep(loadBackoff)
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
