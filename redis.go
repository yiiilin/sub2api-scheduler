package main

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient 清 sub2api 调度缓存 sched:acc:<id>
// 注意：sched:acc 的 TTL 是 -1（持久），DB 改 extra 后必须删掉，调度器才会重建
type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr, password string) *RedisClient {
	c := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	return &RedisClient{client: c}
}

func (r *RedisClient) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// DelSchedAcc 删除账号调度缓存
func (r *RedisClient) DelSchedAcc(ctx context.Context, accountID int64) {
	key := "sched:acc:" + strconv.FormatInt(accountID, 10)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		log.Printf("[redis] DEL %s: %v", key, err)
		return
	}
	log.Printf("[redis] DEL %s", key)
}
