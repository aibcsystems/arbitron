// ============================================================
//  internal/redis/client.go
//  Arbitron v4 — Redis Client Constructor
//
//  ★ NOTE: main.go calls redis.NewClient(cfg.RedisAddr) separately
//  from redis.NewStreamPipeline(...). The P0-patched pipeline.go
//  (the version main.go actually requires — it's the one with
//  PriceTick/SubscribeTicks/the persistence.Copier constructor)
//  doesn't itself define NewClient; that lived in the older,
//  superseded pipeline.go variant. Keeping it as its own file
//  here so it isn't lost when the older pipeline.go is dropped.
// ============================================================

package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func NewClient(addr string) (*goredis.Client, error) {
	client := goredis.NewClient(&goredis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		PoolSize:     20,
		MinIdleConns: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("Redis ping failed: %w", err)
	}

	log.Println("[Redis] ✅ Connected to Redis at", addr)
	return client, nil
}
