package main

import (
	"context"
	"math/rand"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RedisDB là adapter implement Database interface cho Redis Cluster.
// Sử dụng thư viện go-redis/v9 để giao tiếp với cụm Redis.
type RedisDB struct {
	client  *redis.ClusterClient
	masters []*redis.Client
}

func NewRedisDB(addrs []string, maxConcurrency int) (*RedisDB, error) {
	poolSize := maxConcurrency * 2
	if poolSize < 100 {
		poolSize = 100
	}
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		PoolSize: poolSize,
	})
	return &RedisDB{client: rdb}, nil
}

// Get đọc giá trị của key từ Redis (dưới dạng JSON).
func (r *RedisDB) Get(ctx context.Context, key string) error {
	err := r.client.Do(ctx, "JSON.GET", key).Err()
	if err == redis.Nil {
		return nil
	}
	return err
}

// Set ghi giá trị value vào key trong Redis dưới định dạng JSON.
func (r *RedisDB) Set(ctx context.Context, key string, value []byte) error {
	return r.client.Do(ctx, "JSON.SET", key, "$", string(value)).Err()
}

// Delete xoá key khỏi Redis.
func (r *RedisDB) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
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

// Init khởi tạo index RediSearch cho Redis.
func (r *RedisDB) Init(ctx context.Context) error {
	var mu sync.Mutex
	r.masters = nil
	return r.client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
		// Ping thử để lọc bỏ các IP "bóng ma" (stale nodes) do Topology của Redis Cluster bị lỗi
		if err := client.Ping(ctx).Err(); err != nil {
			return nil // Bỏ qua node này, không đưa vào danh sách r.masters
		}

		mu.Lock()
		r.masters = append(r.masters, client)
		mu.Unlock()

		err := client.Do(ctx, "FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "bench:key:", "SCHEMA",
			"$.Dnn", "AS", "Dnn", "TAG",
			"$.IPType", "AS", "IPType", "TAG",
			"$.State", "AS", "State", "TAG",
			"$.Timer", "AS", "Timer", "NUMERIC",
			"$.SEID", "AS", "SEID", "TAG",
			"$.startTime", "AS", "startTime", "NUMERIC",
			"$.attachTime", "AS", "attachTime", "NUMERIC",
			"$.FTEIDC", "AS", "FTEIDC", "TAG",
			"$.IP", "AS", "IP", "TAG",
			"$.SNSSAI", "AS", "SNSSAI", "TAG",
			"$.UpfSelection", "AS", "UpfSelection", "NUMERIC",
			"$.TEIDC", "AS", "TEIDC", "TAG").Err()

		if err != nil && strings.Contains(err.Error(), "Index already exists") {
			return nil // Bỏ qua nếu index đã tồn tại trên node này
		}
		return err
	})
}

// Query thực thi truy vấn RediSearch.
// Chọn một master node ngẫu nhiên để làm coordinator, giúp cân bằng tải.
func (r *RedisDB) Query(ctx context.Context, query string) error {
	if len(r.masters) > 0 {
		target := r.masters[rand.Intn(len(r.masters))]
		return target.Do(ctx, "FT.SEARCH", "idx", query, "LIMIT", 0, 10).Err()
	}
	return r.client.Do(ctx, "FT.SEARCH", "idx", query, "LIMIT", 0, 10).Err()
}
