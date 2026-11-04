package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisKey         = "ratelimit"
	defaultRedisTimeout     = 25 * time.Millisecond
	defaultRedisMaxAttempts = 3
	defaultRedisBackoff     = 10 * time.Millisecond
	maxRedisBackoff         = time.Second
	redisTokenTTL           = 2 * time.Second
	redisKeyTTL             = redisTokenTTL + time.Second
)

// The whole check-and-increment runs inside Redis, so concurrent instances
// cannot both see room for the last slot. TIME comes from the Redis server,
// which keeps all instances on the same window despite host clock skew.
//
// The key is a sorted set with one member per allowed call: the member is a
// per-call token and the score is the microsecond it was allowed. Counting the
// current window is a ZCOUNT, and keeping tokens as members makes the retry
// idempotency check a single ZSCORE. Members older than the token TTL are
// trimmed on every allow, so the set is bounded by a few windows' worth of
// requests instead of growing with total traffic.
//
// ARGV[3] is a per-call token: a retry after an ambiguous failure within the
// token TTL returns the original decision without consuming another slot.
// ARGV[1] is the token TTL in microseconds, ARGV[2] the limit and ARGV[4] the
// key TTL in milliseconds.
const redisAllowScript = `
redis.replicate_commands()
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local startUs = tonumber(t[1]) * 1000000
local endUs = startUs + 1000000
local limit = tonumber(ARGV[2])
local tokenField = 't:' .. ARGV[3]
if redis.call('ZSCORE', KEYS[1], tokenField) then
	local count = redis.call('ZCOUNT', KEYS[1], startUs, endUs - 1)
	return {1, limit - count, 0}
end
local count = redis.call('ZCOUNT', KEYS[1], startUs, endUs - 1)
if count >= limit then
	return {0, 0, endUs - now}
end
redis.call('ZADD', KEYS[1], now, tokenField)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - tonumber(ARGV[1]))
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return {1, limit - count - 1, 0}
`

// FailurePolicy decides what Allow returns when Redis cannot be reached.
type FailurePolicy int

const (
	// FailClosed denies the request. It is the zero value.
	FailClosed FailurePolicy = iota
	// FailOpen allows the request, trading the shared limit for availability.
	FailOpen
)

// RedisLimiterConfig configures a RedisLimiter.
//
// Limit must be positive. Zero values for Key, Timeout, MaxAttempts and
// Backoff select defaults. The zero value of Policy is FailClosed.
type RedisLimiterConfig struct {
	Limit       int
	Key         string
	Timeout     time.Duration
	MaxAttempts int
	Backoff     time.Duration
	Policy      FailurePolicy
}

// RedisLimiter enforces a fixed-window, per-second budget in Redis so that
// every process sharing the same Key shares one budget.
type RedisLimiter struct {
	eval        redisEvaler
	limit       int
	key         string
	timeout     time.Duration
	maxAttempts int
	backoff     time.Duration
	policy      FailurePolicy
}

type redisEvaler interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
}

// NewRedisLimiter returns a limiter backed by client.
//
// A *redis.Client or *redis.ClusterClient must set ContextTimeoutEnabled so
// per-attempt deadlines cancel in-flight commands. Other UniversalClient
// implementations are accepted; the limiter still bounds each attempt itself.
func NewRedisLimiter(client redis.UniversalClient, cfg RedisLimiterConfig) (*RedisLimiter, error) {
	if isNilClient(client) {
		return nil, errors.New("ratelimiter: nil redis client")
	}
	if err := checkContextTimeout(client); err != nil {
		return nil, err
	}
	return newRedisLimiter(client, cfg)
}

func isNilClient(client redis.UniversalClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	}
	return false
}

func checkContextTimeout(client redis.UniversalClient) error {
	var enabled bool
	switch c := client.(type) {
	case *redis.Client:
		enabled = c.Options().ContextTimeoutEnabled
	case *redis.ClusterClient:
		enabled = c.Options().ContextTimeoutEnabled
	default:
		return nil
	}
	if !enabled {
		return errors.New("ratelimiter: redis client must set ContextTimeoutEnabled to enforce per-attempt timeouts")
	}
	return nil
}

func newRedisLimiter(eval redisEvaler, cfg RedisLimiterConfig) (*RedisLimiter, error) {
	cfg, err := normalizeRedisConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &RedisLimiter{
		eval:        eval,
		limit:       cfg.Limit,
		key:         cfg.Key,
		timeout:     cfg.Timeout,
		maxAttempts: cfg.MaxAttempts,
		backoff:     cfg.Backoff,
		policy:      cfg.Policy,
	}, nil
}

func normalizeRedisConfig(cfg RedisLimiterConfig) (RedisLimiterConfig, error) {
	if cfg.Limit <= 0 {
		return cfg, fmt.Errorf("ratelimiter: limit must be positive, got %d", cfg.Limit)
	}
	if cfg.Key == "" {
		cfg.Key = defaultRedisKey
	}
	if cfg.Timeout < 0 {
		return cfg, fmt.Errorf("ratelimiter: timeout must not be negative, got %s", cfg.Timeout)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultRedisTimeout
	}
	if cfg.MaxAttempts < 0 {
		return cfg, fmt.Errorf("ratelimiter: max attempts must not be negative, got %d", cfg.MaxAttempts)
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = defaultRedisMaxAttempts
	}
	if cfg.Backoff < 0 {
		return cfg, fmt.Errorf("ratelimiter: backoff must not be negative, got %s", cfg.Backoff)
	}
	if cfg.Backoff == 0 {
		cfg.Backoff = defaultRedisBackoff
	}
	if cfg.Policy != FailClosed && cfg.Policy != FailOpen {
		return cfg, fmt.Errorf("ratelimiter: unknown failure policy %d", cfg.Policy)
	}
	return cfg, nil
}

// Decide reports whether a request may proceed and, when denied, how long to
// wait before retrying.
//
// A nil error means Redis answered: Allowed is the normal rate-limit decision
// and the rest of the Decision describes the budget. A non-nil error means
// Redis could not be reached within the retry budget; Allowed then reflects
// the configured policy and the other fields are not meaningful.
func (l *RedisLimiter) Decide(ctx context.Context) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return l.onFailure(err)
	}

	token := newRequestToken()
	var lastErr error
	for attempt := 1; attempt <= l.maxAttempts; attempt++ {
		decision, err := l.attempt(ctx, token)
		if err == nil {
			return decision, nil
		}
		lastErr = err
		if attempt == l.maxAttempts || ctx.Err() != nil {
			break
		}
		if err := wait(ctx, l.backoffFor(attempt)); err != nil {
			lastErr = err
			break
		}
	}
	return l.onFailure(lastErr)
}

// Allow reports whether a request may proceed. It is a shorthand for
// Decide(ctx).Allowed and has the same error semantics.
func (l *RedisLimiter) Allow(ctx context.Context) (bool, error) {
	decision, err := l.Decide(ctx)
	return decision.Allowed, err
}

type evalResult struct {
	decision Decision
	err      error
}

func (l *RedisLimiter) attempt(parent context.Context, token string) (Decision, error) {
	ctx, cancel := context.WithTimeout(parent, l.timeout)
	defer cancel()

	// The deadline is enforced here as well: a go-redis client with
	// ContextTimeoutEnabled left at its default false ignores context deadlines.
	results := make(chan evalResult, 1)
	go func() {
		nums, err := l.eval.Eval(ctx, redisAllowScript, []string{l.key},
			redisTokenTTL.Microseconds(), l.limit, token, redisKeyTTL.Milliseconds()).Int64Slice()
		if err != nil {
			results <- evalResult{err: err}
			return
		}
		decision, err := decisionFromReply(nums, l.limit)
		results <- evalResult{decision: decision, err: err}
	}()

	select {
	case result := <-results:
		return result.decision, result.err
	case <-ctx.Done():
		return Decision{Limit: l.limit}, ctx.Err()
	}
}

func decisionFromReply(nums []int64, limit int) (Decision, error) {
	if len(nums) != 3 {
		return Decision{}, fmt.Errorf("ratelimiter: unexpected script reply length %d", len(nums))
	}
	return Decision{
		Allowed:    nums[0] == 1,
		Limit:      limit,
		Remaining:  int(nums[1]),
		RetryAfter: time.Duration(nums[2]) * time.Microsecond,
	}, nil
}

// Request tokens only need to be unique, not unguessable.
func newRequestToken() string {
	return strconv.FormatUint(rand.Uint64(), 16) + strconv.FormatUint(rand.Uint64(), 16)
}

func (l *RedisLimiter) onFailure(err error) (Decision, error) {
	wrapped := fmt.Errorf("ratelimiter: redis limiter unavailable: %w", err)
	return Decision{Limit: l.limit, Allowed: l.policy == FailOpen}, wrapped
}

func (l *RedisLimiter) backoffFor(attempt int) time.Duration {
	d := l.backoff
	for i := 1; i < attempt && d < maxRedisBackoff; i++ {
		d *= 2
	}
	if d > maxRedisBackoff {
		d = maxRedisBackoff
	}
	if d <= 1 {
		return d
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)))
}

func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
