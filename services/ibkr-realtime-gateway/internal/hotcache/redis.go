package hotcache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// RedisConfig configures the Redis connection. The user is an ACL user; the password comes from a secret file.
type RedisConfig struct {
	Addr     string
	Username string
	Password string
	Timeout  time.Duration // dial, read, write and pool-wait timeout
}

// RedisStore implements Store with go-redis. Commands used: SET (with EX) and GET, which the ACL user may
// run without any extra permission.
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore creates the store. It does not connect until the first command.
func NewRedisStore(cfg RedisConfig) *RedisStore {
	return &RedisStore{client: redis.NewClient(&redis.Options{
		Addr:                  cfg.Addr,
		Username:              cfg.Username,
		Password:              cfg.Password,
		DialTimeout:           cfg.Timeout,
		ReadTimeout:           cfg.Timeout,
		WriteTimeout:          cfg.Timeout,
		PoolTimeout:           cfg.Timeout,
		ContextTimeoutEnabled: true,
		MaxRetries:            -1, // fail fast: the guard's cool-down replaces retries
		PoolSize:              8,
		DisableIdentity:       true, // no CLIENT SETINFO on connect
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})}
}

// SetMany writes all entries in one pipeline.
func (s *RedisStore) SetMany(ctx context.Context, entries []Entry) error {
	pipe := s.client.Pipeline()
	for _, e := range entries {
		pipe.Set(ctx, e.Key, e.Value, e.TTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Get returns the value of key or ErrMiss.
func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, error) {
	v, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrMiss
	}
	return v, err
}

// Close releases the connection pool.
func (s *RedisStore) Close() error { return s.client.Close() }
