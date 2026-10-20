package ratelimiter

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var frozenRedisTime = time.Unix(1_700_000_000, 0)

func newFrozenRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(frozenRedisTime)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func testLimiter(t *testing.T, eval redisEvaler, cfg RedisLimiterConfig) *RedisLimiter {
	t.Helper()
	l, err := newRedisLimiter(eval, cfg)
	if err != nil {
		t.Fatalf("newRedisLimiter: %v", err)
	}
	return l
}

func allowOnce(t *testing.T, l *RedisLimiter, want bool) {
	t.Helper()
	allowed, err := l.Allow(context.Background())
	if err != nil {
		t.Fatalf("Allow returned error: %v", err)
	}
	if allowed != want {
		t.Fatalf("Allow = %v, want %v", allowed, want)
	}
}

type fakeEvaler struct {
	next      redisEvaler
	calls     atomic.Int64
	remaining atomic.Int64
	err       error
	block     bool
}

func newFakeEvaler(next redisEvaler, failures int64, err error) *fakeEvaler {
	f := &fakeEvaler{next: next, err: err}
	f.remaining.Store(failures)
	return f
}

func (f *fakeEvaler) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	f.calls.Add(1)
	if f.block {
		<-ctx.Done()
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(ctx.Err())
		return cmd
	}
	if f.remaining.Add(-1) >= 0 {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(f.err)
		return cmd
	}
	if f.next == nil {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(errors.New("unexpected evaluation"))
		return cmd
	}
	return f.next.Eval(ctx, script, keys, args...)
}

func TestRedisLimiterSharesBudgetAcrossInstances(t *testing.T) {
	mr, _ := newFrozenRedis(t)
	const (
		limit       = 100
		instances   = 4
		perInstance = 40
	)

	limiters := make([]*RedisLimiter, instances)
	for i := range limiters {
		client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
		t.Cleanup(func() { _ = client.Close() })
		limiters[i] = testLimiter(t, client, RedisLimiterConfig{Limit: limit, Key: "shared-budget"})
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for _, l := range limiters {
		for j := 0; j < perInstance; j++ {
			wg.Add(1)
			go func(l *RedisLimiter) {
				defer wg.Done()
				ok, err := l.Allow(context.Background())
				if err != nil {
					t.Errorf("Allow returned error: %v", err)
					return
				}
				if ok {
					allowed.Add(1)
				}
			}(l)
		}
	}
	wg.Wait()

	if got := allowed.Load(); got != limit {
		t.Fatalf("allowed %d requests across instances, want exactly %d", got, limit)
	}

	mr.SetTime(frozenRedisTime.Add(time.Second))
	allowOnce(t, limiters[0], true)
}

func TestRedisLimiterResetsOnNextWindow(t *testing.T) {
	mr, client := newFrozenRedis(t)
	l := testLimiter(t, client, RedisLimiterConfig{Limit: 2, Key: "reset"})

	allowOnce(t, l, true)
	allowOnce(t, l, true)
	allowOnce(t, l, false)

	mr.SetTime(frozenRedisTime.Add(time.Second))
	allowOnce(t, l, true)
}

func TestRedisLimiterExpiresInactiveWindow(t *testing.T) {
	mr, client := newFrozenRedis(t)
	key := "ttl-check"
	l := testLimiter(t, client, RedisLimiterConfig{Limit: 1, Key: key})

	allowOnce(t, l, true)
	if !mr.Exists(key) {
		t.Fatalf("key %q missing after an allowed request", key)
	}
	if ttl := mr.TTL(key); ttl <= 0 || ttl > redisWindowTTL {
		t.Fatalf("TTL = %s, want within (0, %s]", ttl, redisWindowTTL)
	}

	mr.FastForward(redisWindowTTL + time.Second)
	if mr.Exists(key) {
		t.Fatalf("key %q still exists after its TTL", key)
	}
}

func TestRedisLimiterRetriesTransientFailures(t *testing.T) {
	_, client := newFrozenRedis(t)
	fault := newFakeEvaler(client, 2, errors.New("connection reset by peer"))
	l := testLimiter(t, fault, RedisLimiterConfig{
		Limit:       1,
		Key:         "retry",
		Timeout:     100 * time.Millisecond,
		MaxAttempts: 3,
		Backoff:     time.Millisecond,
	})

	allowOnce(t, l, true)
	if calls := fault.calls.Load(); calls != 3 {
		t.Fatalf("Eval called %d times, want 3", calls)
	}
}

func TestRedisLimiterFailurePolicies(t *testing.T) {
	sentinel := errors.New("redis unavailable")
	for _, tc := range []struct {
		name   string
		policy FailurePolicy
		want   bool
	}{
		{"fail open", FailOpen, true},
		{"fail closed", FailClosed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fault := newFakeEvaler(nil, math.MaxInt64, sentinel)
			l := testLimiter(t, fault, RedisLimiterConfig{
				Limit:       1,
				Timeout:     100 * time.Millisecond,
				MaxAttempts: 3,
				Backoff:     time.Millisecond,
				Policy:      tc.policy,
			})

			allowed, err := l.Allow(context.Background())
			if allowed != tc.want {
				t.Fatalf("Allow = %v, want %v", allowed, tc.want)
			}
			if !errors.Is(err, sentinel) {
				t.Fatalf("Allow error = %v, want it to wrap %v", err, sentinel)
			}
			if calls := fault.calls.Load(); calls != 3 {
				t.Fatalf("Eval called %d times, want 3", calls)
			}
		})
	}
}

func TestRedisLimiterHonorsCanceledContext(t *testing.T) {
	fault := newFakeEvaler(nil, math.MaxInt64, errors.New("unexpected call"))
	l := testLimiter(t, fault, RedisLimiterConfig{Limit: 1})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	allowed, err := l.Allow(ctx)
	if allowed {
		t.Fatal("Allow = true with a canceled context, want false")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Allow error = %v, want context.Canceled", err)
	}
	if calls := fault.calls.Load(); calls != 0 {
		t.Fatalf("Eval called %d times, want 0", calls)
	}
}

func TestRedisLimiterStopsRetryingWhenContextEnds(t *testing.T) {
	fault := newFakeEvaler(nil, 1, errors.New("boom"))
	l := testLimiter(t, fault, RedisLimiterConfig{
		Limit:       1,
		Timeout:     100 * time.Millisecond,
		MaxAttempts: 3,
		Backoff:     time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	allowed, err := l.Allow(ctx)
	elapsed := time.Since(start)

	if allowed {
		t.Fatal("Allow = true, want false")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Allow error = %v, want context.DeadlineExceeded", err)
	}
	if calls := fault.calls.Load(); calls != 1 {
		t.Fatalf("Eval called %d times, want 1", calls)
	}
	if elapsed < 20*time.Millisecond || elapsed > time.Second {
		t.Fatalf("Allow took %s, want between 20ms and 1s", elapsed)
	}
}

func TestRedisLimiterAppliesPerAttemptTimeout(t *testing.T) {
	fault := &fakeEvaler{block: true}
	l := testLimiter(t, fault, RedisLimiterConfig{
		Limit:       1,
		Timeout:     20 * time.Millisecond,
		MaxAttempts: 2,
		Backoff:     time.Millisecond,
	})

	start := time.Now()
	allowed, err := l.Allow(context.Background())
	elapsed := time.Since(start)

	if allowed {
		t.Fatal("Allow = true, want false")
	}
	if err == nil {
		t.Fatal("Allow returned nil error, want a timeout error")
	}
	if calls := fault.calls.Load(); calls != 2 {
		t.Fatalf("Eval called %d times, want 2", calls)
	}
	if elapsed < 35*time.Millisecond || elapsed > time.Second {
		t.Fatalf("Allow took %s, want at least two 20ms attempts and under 1s", elapsed)
	}
}

func TestRedisLimiterConfigValidation(t *testing.T) {
	_, client := newFrozenRedis(t)

	tests := []struct {
		name    string
		cfg     RedisLimiterConfig
		wantErr bool
	}{
		{"zero limit", RedisLimiterConfig{}, true},
		{"negative limit", RedisLimiterConfig{Limit: -1}, true},
		{"negative timeout", RedisLimiterConfig{Limit: 1, Timeout: -time.Millisecond}, true},
		{"negative attempts", RedisLimiterConfig{Limit: 1, MaxAttempts: -1}, true},
		{"negative backoff", RedisLimiterConfig{Limit: 1, Backoff: -time.Millisecond}, true},
		{"unknown policy", RedisLimiterConfig{Limit: 1, Policy: FailurePolicy(9)}, true},
		{"defaults", RedisLimiterConfig{Limit: 1}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l, err := NewRedisLimiter(client, tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewRedisLimiter(%+v) succeeded, want error", tc.cfg)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRedisLimiter(%+v): %v", tc.cfg, err)
			}
			if l.key != defaultRedisKey {
				t.Errorf("key = %q, want %q", l.key, defaultRedisKey)
			}
			if l.timeout != defaultRedisTimeout {
				t.Errorf("timeout = %s, want %s", l.timeout, defaultRedisTimeout)
			}
			if l.maxAttempts != defaultRedisMaxAttempts {
				t.Errorf("maxAttempts = %d, want %d", l.maxAttempts, defaultRedisMaxAttempts)
			}
			if l.backoff != defaultRedisBackoff {
				t.Errorf("backoff = %s, want %s", l.backoff, defaultRedisBackoff)
			}
			if l.policy != FailClosed {
				t.Errorf("policy = %v, want FailClosed", l.policy)
			}
		})
	}

	if _, err := NewRedisLimiter(nil, RedisLimiterConfig{Limit: 1}); err == nil {
		t.Fatal("NewRedisLimiter with a nil client succeeded, want error")
	}
}
