package ratelimiter

import (
	"context"
	"errors"
	"math"
	"net"
	"strconv"
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
	client := redis.NewClient(&redis.Options{
		Addr:                  mr.Addr(),
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func windowCount(t *testing.T, client *redis.Client, key string) int {
	t.Helper()
	start := frozenRedisTime.Unix() * 1_000_000
	end := start + 1_000_000
	count, err := client.ZCount(context.Background(), key,
		strconv.FormatInt(start, 10), strconv.FormatInt(end-1, 10)).Result()
	if err != nil {
		t.Fatalf("ZCount %s: %v", key, err)
	}
	return int(count)
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

type lossyEvaler struct {
	next   redisEvaler
	losses atomic.Int64
	calls  atomic.Int64
}

// Eval runs the real script, then loses the reply for the configured number
// of calls, imitating a failure where Redis applied the write but the client
// never saw the answer.
func (f *lossyEvaler) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	f.calls.Add(1)
	cmd := f.next.Eval(ctx, script, keys, args...)
	if f.losses.Add(-1) >= 0 {
		lost := redis.NewCmd(ctx)
		lost.SetErr(errors.New("reply lost after the script ran"))
		return lost
	}
	return cmd
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
		client := redis.NewClient(&redis.Options{
			Addr:                  mr.Addr(),
			MaxRetries:            -1,
			ContextTimeoutEnabled: true,
		})
		t.Cleanup(func() { _ = client.Close() })
		limiters[i] = testLimiter(t, client, RedisLimiterConfig{
			Limit:   limit,
			Key:     "shared-budget",
			Timeout: 2 * time.Second,
		})
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
	if ttl := mr.TTL(key); ttl <= 0 || ttl > redisKeyTTL {
		t.Fatalf("TTL = %s, want within (0, %s]", ttl, redisKeyTTL)
	}

	mr.FastForward(redisKeyTTL + time.Second)
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

func TestRedisLimiterRetriesDoNotConsumeExtraBudget(t *testing.T) {
	_, client := newFrozenRedis(t)
	key := "idempotent-retry"
	fault := &lossyEvaler{next: client}
	fault.losses.Store(1)
	l := testLimiter(t, fault, RedisLimiterConfig{
		Limit:       5,
		Key:         key,
		Timeout:     100 * time.Millisecond,
		MaxAttempts: 3,
		Backoff:     time.Millisecond,
	})

	allowOnce(t, l, true)
	if calls := fault.calls.Load(); calls != 2 {
		t.Fatalf("Eval called %d times, want 2", calls)
	}
	if count := windowCount(t, client, key); count != 1 {
		t.Fatalf("window count = %d after one allowed call with a lost reply, want 1", count)
	}

	for i := 0; i < 4; i++ {
		allowOnce(t, l, true)
	}
	allowOnce(t, l, false)
}

func TestRedisLimiterDeniedRequestsDoNotConsumeBudget(t *testing.T) {
	_, client := newFrozenRedis(t)
	key := "denied"
	l := testLimiter(t, client, RedisLimiterConfig{Limit: 1, Key: key})

	allowOnce(t, l, true)
	allowOnce(t, l, false)
	allowOnce(t, l, false)

	if count := windowCount(t, client, key); count != 1 {
		t.Fatalf("window count = %d after one allowed and two denied calls, want 1", count)
	}
}

func TestRedisLimiterDecisionMetadata(t *testing.T) {
	_, client := newFrozenRedis(t)
	l := testLimiter(t, client, RedisLimiterConfig{Limit: 3, Key: "metadata"})

	for i, want := range []int{2, 1, 0} {
		d, err := l.Decide(context.Background())
		if err != nil {
			t.Fatalf("Decide call %d: %v", i+1, err)
		}
		if !d.Allowed || d.Limit != 3 || d.Remaining != want || d.RetryAfter != 0 {
			t.Fatalf("Decide call %d = %+v, want allowed with %d remaining", i+1, d, want)
		}
	}

	d, err := l.Decide(context.Background())
	if err != nil {
		t.Fatalf("Decide over budget: %v", err)
	}
	if d.Allowed || d.Limit != 3 || d.Remaining != 0 {
		t.Fatalf("Decide over budget = %+v, want denied with 0 remaining", d)
	}
	if d.RetryAfter != time.Second {
		t.Fatalf("RetryAfter = %s, want 1s", d.RetryAfter)
	}
}

func TestRedisLimiterStateStaysBounded(t *testing.T) {
	mr, client := newFrozenRedis(t)
	const limit = 10
	key := "bounded"
	l := testLimiter(t, client, RedisLimiterConfig{Limit: limit, Key: key})

	now := frozenRedisTime
	for second := 0; second < 10; second++ {
		for i := 0; i < limit; i++ {
			allowOnce(t, l, true)
		}
		allowOnce(t, l, false)
		now = now.Add(time.Second)
		mr.SetTime(now)
	}

	allowOnce(t, l, true) // trims members older than the token TTL
	card := client.ZCard(context.Background(), key).Val()
	if maxMembers := int64(3 * limit); card > maxMembers {
		t.Fatalf("sorted set has %d members after 10 windows, want at most %d", card, maxMembers)
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

func TestRedisLimiterEnforcesTimeoutWithoutClientSupport(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	// ContextTimeoutEnabled is false here, as it is by default. The limiter
	// must still bound the attempt.
	client := redis.NewClient(&redis.Options{Addr: ln.Addr().String(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	l := testLimiter(t, client, RedisLimiterConfig{
		Limit:       1,
		Timeout:     25 * time.Millisecond,
		MaxAttempts: 1,
		Backoff:     time.Millisecond,
	})

	start := time.Now()
	allowed, err := l.Allow(context.Background())
	elapsed := time.Since(start)

	if allowed {
		t.Fatal("Allow = true against an unresponsive Redis, want false")
	}
	if err == nil {
		t.Fatal("Allow returned nil error, want a timeout error")
	}
	if elapsed > time.Second {
		t.Fatalf("Allow took %s with a 25ms timeout, want under 1s", elapsed)
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

func TestNewRedisLimiterRejectsClientsItCannotBound(t *testing.T) {
	mr := miniredis.RunT(t)

	plain := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = plain.Close() })
	if _, err := NewRedisLimiter(plain, RedisLimiterConfig{Limit: 1}); err == nil {
		t.Fatal("NewRedisLimiter accepted a client without ContextTimeoutEnabled, want error")
	}

	var typedNil *redis.Client
	if _, err := NewRedisLimiter(typedNil, RedisLimiterConfig{Limit: 1}); err == nil {
		t.Fatal("NewRedisLimiter accepted a typed nil client, want error")
	}
}
