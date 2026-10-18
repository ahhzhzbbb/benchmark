package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	// Đọc cấu hình từ file YAML (hoặc dùng giá trị mặc định nếu không chỉ định --config)
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Lỗi đọc cấu hình: %v", err)
	}

	// Cấu hình GOMAXPROCS nếu người dùng chỉ định (giảm contention trên máy nhiều core)
	if cfg.Load.GOMAXPROCS > 0 {
		runtime.GOMAXPROCS(cfg.Load.GOMAXPROCS)
		log.Printf("GOMAXPROCS set to %d (NumCPU: %d)", cfg.Load.GOMAXPROCS, runtime.NumCPU())
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
func createDatabase(cfg Config) (Database, error) {
	switch cfg.Database.Type {
	case "redis":
		return NewRedisDB(cfg.Redis.Addresses, cfg.Load.MaxConcurrency)
	case "aerospike":
		return NewAerospikeDB(cfg.Aerospike, cfg.Data.KeyCount, cfg.Load.MaxConcurrency)
	default:
		return nil, fmt.Errorf("database type không hỗ trợ: %s", cfg.Database.Type)
	}
}
