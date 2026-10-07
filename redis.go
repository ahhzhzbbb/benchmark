package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisDB là adapter implement Database interface cho Redis Cluster.
//
// Lưu ý về RediSearch trên Redis Cluster (OSS, không có coordinator):
// mỗi master chỉ index phần dữ liệu thuộc các hash slot của nó. Vì vậy
// FT.SEARCH phải được gửi tới TẤT CẢ master (scatter-gather) thì mới có
// kết quả đầy đủ trên toàn bộ dataset.
type RedisDB struct {
	client  *redis.ClusterClient
	masters []*redis.Client
}

// redisIndexName là tên index RediSearch trên HASH. Dùng tên mới (khác "idx"
// của bản RedisJSON cũ) để FT.CREATE không bị bỏ qua vì "Index already exists".
const redisIndexName = "idx_hash"

func NewRedisDB(addrs []string, maxConcurrency int) (*RedisDB, error) {
	poolSize := maxConcurrency * 2
	if poolSize < 100 {
		poolSize = 100
	}
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		PoolSize: poolSize,
		// TỐI ƯU 1: Warm-up connection pool
		// Nếu không có MinIdleConns, các request benchmark đầu tiên sẽ bị delay
		// do phải liên tục thiết lập kết nối TCP (3-way handshake).
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
	var mu sync.Mutex
	// Reset danh sách masters mỗi lần init
	r.masters = make([]*redis.Client, 0)

	return r.client.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
		if err := client.Ping(ctx).Err(); err != nil {
			return nil // Bỏ qua node bị lỗi kết nối
		}

		mu.Lock()
		r.masters = append(r.masters, client)
		mu.Unlock()

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

// Query thực thi truy vấn RediSearch theo mô hình scatter-gather:
// gửi song song tới tất cả master và chờ tất cả trả lời.
// Latency đo được = latency của shard chậm nhất, đúng với chi phí thực tế
// mà client phải trả để có kết quả đầy đủ.
func (r *RedisDB) Query(ctx context.Context, query string) error {
	switch len(r.masters) {
	case 0:
		// Chưa lấy được danh sách master (Init lỗi) — fallback, kết quả có thể không đầy đủ.
		return r.client.Do(ctx, searchArgs(query)...).Err()
	case 1:
		return r.masters[0].Do(ctx, searchArgs(query)...).Err()
	}

	errs := make([]error, len(r.masters))
	var wg sync.WaitGroup
	// Gửi tới master[1..n-1] trong goroutine, master[0] chạy ngay trên goroutine hiện tại.
	for i := 1; i < len(r.masters); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = r.masters[i].Do(ctx, searchArgs(query)...).Err()
		}(i)
	}
	errs[0] = r.masters[0].Do(ctx, searchArgs(query)...).Err()
	wg.Wait()

	return errors.Join(errs...)
}
