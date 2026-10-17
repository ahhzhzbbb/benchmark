package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Đọc cấu hình từ file YAML (hoặc dùng giá trị mặc định nếu không chỉ định --config)
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Lỗi đọc cấu hình: %v", err)
	}

	// Tạo database adapter dựa trên loại database trong config
	db, err := createDatabase(cfg)
	if err != nil {
		log.Fatalf("Lỗi tạo database adapter: %v", err)
	}
	defer db.Close()

	// Xử lý tín hiệu Ctrl+C / SIGTERM để tắt gracefully
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived interrupt signal. Shutting down...")
		cancel()
	}()

	// Khởi chạy benchmark
	runner := NewRunner(cfg, db)
	runner.Run(ctx)
}

// createDatabase tạo database adapter phù hợp dựa trên config.
// Hiện tại chỉ hỗ trợ Redis. Aerospike sẽ được thêm ở Phase 2.
func createDatabase(cfg Config) (Database, error) {
	switch cfg.Database.Type {
	case "redis":
		return NewRedisDB(cfg.Redis.Addresses, cfg.Load.MaxConcurrency)
	case "aerospike":
		return NewAerospikeDB(cfg.Aerospike, cfg.Data.KeyCount)
	default:
		return nil, fmt.Errorf("database type không hỗ trợ: %s", cfg.Database.Type)
	}
}
