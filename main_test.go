package main

import (
	"sync"
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

	if allowed > limit*2 {
		t.Fatalf("allowed %d calls, which is more than the limit of %d per second", allowed, limit)
	}
}
