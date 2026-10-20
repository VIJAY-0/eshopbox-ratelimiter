package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisKey         = "ratelimit"
	defaultRedisTimeout     = 25 * time.Millisecond
	defaultRedisMaxAttempts = 3
	defaultRedisBackoff     = 10 * time.Millisecond
	maxRedisBackoff         = time.Second
	redisWindowTTL          = 2 * time.Second
)

// The whole check-and-increment runs inside Redis, so concurrent instances
// cannot both see room for the last slot. TIME comes from the Redis server,
// which keeps all instances on the same window despite host clock skew.
const redisAllowScript = `
redis.replicate_commands()
local t = redis.call('TIME')
local count = redis.call('HINCRBY', KEYS[1], t[1], 1)
if count == 1 then
	redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
if count <= tonumber(ARGV[2]) then
	return 1
end
return 0
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

// NewRedisLimiter returns a limiter backed by client. It returns an error if
// the configuration is invalid or the client is nil.
func NewRedisLimiter(client redis.UniversalClient, cfg RedisLimiterConfig) (*RedisLimiter, error) {
	if client == nil {
		return nil, errors.New("ratelimiter: nil redis client")
	}
	return newRedisLimiter(client, cfg)
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

// Allow reports whether a request may proceed.
//
// A nil error means Redis answered: true is an allowed request and false is a
// normal rate-limit denial. A non-nil error means Redis could not be reached
// within the retry budget; the boolean then reflects the configured policy.
func (l *RedisLimiter) Allow(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return l.onFailure(err)
	}

	var lastErr error
	for attempt := 1; attempt <= l.maxAttempts; attempt++ {
		allowed, err := l.attempt(ctx)
		if err == nil {
			return allowed, nil
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

func (l *RedisLimiter) attempt(parent context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, l.timeout)
	defer cancel()

	n, err := l.eval.Eval(ctx, redisAllowScript, []string{l.key}, redisWindowTTL.Milliseconds(), l.limit).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (l *RedisLimiter) onFailure(err error) (bool, error) {
	wrapped := fmt.Errorf("ratelimiter: redis limiter unavailable: %w", err)
	return l.policy == FailOpen, wrapped
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
