package main

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisDB struct {
	client *redis.ClusterClient
}

// redisIndexName là tên index RediSearch trên HASH. Dùng tên mới (khác "idx"
// của bản RedisJSON cũ) để FT.CREATE không bị bỏ qua vì "Index already exists".
const redisIndexName = "idx_hash"

func NewRedisDB(addrs []string, maxConcurrency int) (*RedisDB, error) {
	// PoolSize trên mỗi node được set bằng maxConcurrency (tối thiểu 10)
	// Đảm bảo không worker nào bị nghẽn do chờ connection pool,
	// đồng thời không nhân bản connection quá mức (tránh maxConcurrency * 2).
	poolSize := maxConcurrency
	if poolSize < 10 {
		poolSize = 10
	}
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        addrs,
		PoolSize:     poolSize,
		MinIdleConns: poolSize / 2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	return &RedisDB{client: rdb}, nil
}

// Get đọc toàn bộ record (HGETALL), tương đương Aerospike Get đọc tất cả bins.
// HGETALL trả về map rỗng khi key không tồn tại → ErrNotFound.
func (r *RedisDB) Get(ctx context.Context, key string) error {
	m, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return err
	}
	if len(m) == 0 {
		return ErrNotFound
	}
	return nil
}

// Set ghi record bằng HSET với danh sách field/value đã dựng sẵn.
// Mọi SET đều ghi đủ 13 field nên HSET tương đương ghi đè toàn bộ record.
func (r *RedisDB) Set(ctx context.Context, key string, payload *Payload) error {
	return r.client.HSet(ctx, key, payload.Hash...).Err()
}

// Delete xoá key khỏi Redis. Trả về ErrNotFound nếu key không tồn tại.
func (r *RedisDB) Delete(ctx context.Context, key string) error {
	n, err := r.client.Del(ctx, key).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// Close đóng kết nối tới Redis Cluster.
func (r *RedisDB) Close() error {
	return r.client.Close()
}

// Ping kiểm tra kết nối tới Redis Cluster.
func (r *RedisDB) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Name trả về tên hiển thị.
func (r *RedisDB) Name() string {
	return "Redis Cluster"
}

// Init khởi tạo index RediSearch trên HASH cho Redis.
func (r *RedisDB) Init(ctx context.Context) error {
	return r.client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
		if err := client.Ping(ctx).Err(); err != nil {
			return nil // Bỏ qua node bị lỗi kết nối
		}

		// Đặt ON_TIMEOUT policy là FAIL trên server nếu hỗ trợ
		_ = client.Do(ctx, "FT.CONFIG", "SET", "ON_TIMEOUT", "FAIL").Err()

		err := client.Do(ctx, "FT.CREATE", redisIndexName, "ON", "HASH", "PREFIX", "1", "bench:key:", "SCHEMA",
			"Dnn", "TAG",
			"IPType", "TAG",
			"State", "TAG",
			"Timer", "NUMERIC",
			"SEID", "TAG",
			"startTime", "NUMERIC",
			"attachTime", "NUMERIC",
			"FTEIDC", "TAG",
			"IP", "TAG",
			"SNSSAI", "TAG",
			"UpfSelection", "NUMERIC",
			"TEIDC", "TAG").Err()

		if err != nil && strings.Contains(err.Error(), "Index already exists") {
			return nil
		}
		return err
	})
}

// searchArgs tạo tham số FT.SEARCH. NOCONTENT: chỉ trả về id, giảm I/O mạng.
func searchArgs(query string) []interface{} {
	return []interface{}{
		"FT.SEARCH", redisIndexName, query,
		"TIMEOUT", strconv.Itoa(queryTimeoutMs),
		"NOCONTENT",
		"LIMIT", "0", strconv.Itoa(queryResultLimit),
	}
}

// Query thực thi truy vấn RediSearch.
// Từ Redis 8.0 OSS trở đi (hoặc khi có server-side coordinator),
// server sẽ tự động fan-out query tới các shard và tổng hợp kết quả.
// Tool benchmark chỉ cần gửi 1 lệnh duy nhất qua ClusterClient.
func (r *RedisDB) Query(ctx context.Context, query string) error {
	return r.client.Do(ctx, searchArgs(query)...).Err()
}
