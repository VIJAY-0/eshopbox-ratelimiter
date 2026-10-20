package ratelimiter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// The default suite is hermetic. Set REDIS_ADDR to exercise the same script
// against a real Redis server, for example REDIS_ADDR=localhost:6379.
func TestLiveRedisLimiterSharedBudget(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run the live Redis integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	opts := &redis.Options{Addr: addr, MaxRetries: -1}
	if password := os.Getenv("REDIS_PASSWORD"); password != "" {
		opts.Password = password
	}
	clientA := redis.NewClient(opts)
	clientB := redis.NewClient(opts)
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})

	if err := clientA.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}

	cfg := RedisLimiterConfig{
		Limit:       5,
		Key:         fmt.Sprintf("ratelimiter:test:%d", time.Now().UnixNano()),
		Timeout:     time.Second,
		MaxAttempts: 2,
		Backoff:     10 * time.Millisecond,
	}
	a, err := NewRedisLimiter(clientA, cfg)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}
	b, err := NewRedisLimiter(clientB, cfg)
	if err != nil {
		t.Fatalf("NewRedisLimiter: %v", err)
	}

	// Start just after a window boundary so six sequential calls cannot
	// straddle two windows on a healthy server.
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 20*time.Millisecond)))

	for i := 0; i < cfg.Limit; i++ {
		limiter := a
		if i%2 == 1 {
			limiter = b
		}
		allowed, err := limiter.Allow(ctx)
		if err != nil {
			t.Fatalf("Allow call %d: %v", i+1, err)
		}
		if !allowed {
			t.Fatalf("Allow call %d denied, want allowed", i+1)
		}
	}

	if allowed, err := b.Allow(ctx); err != nil {
		t.Fatalf("Allow over budget: %v", err)
	} else if allowed {
		t.Fatal("Allow over budget = true, want false")
	}

	time.Sleep(1100 * time.Millisecond)
	if allowed, err := a.Allow(ctx); err != nil {
		t.Fatalf("Allow next window: %v", err)
	} else if !allowed {
		t.Fatal("Allow next window = false, want true")
	}
}
