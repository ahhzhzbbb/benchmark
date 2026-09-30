package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// Runner điều phối toàn bộ lifecycle của một phiên benchmark.
//
// Lifecycle:
//
//	Connect → Warmup → Reset metrics → Measure → Shutdown → Report
//
// Runner sở hữu và phối hợp 4 thành phần chính:
//   - Database adapter (giao tiếp với DB)
//   - WorkloadGenerator (sinh request theo tỉ lệ cấu hình)
//   - RateController (điều khiển tốc độ phát request - open-loop)
//   - WorkerPool (thực thi request lên database)
type Runner struct {
	cfg Config
	db  Database
}

// NewRunner tạo Runner mới với cấu hình và database adapter đã cho.
func NewRunner(cfg Config, db Database) *Runner {
	return &Runner{
		cfg: cfg,
		db:  db,
	}
}

// Run thực thi toàn bộ benchmark theo lifecycle đã định nghĩa.
// Đây là method duy nhất cần gọi từ main().
//
// Flow:
//  1. Kiểm tra kết nối database
//  2. In cấu hình
//  3. Warmup phase (kết quả bị bỏ qua)
//  4. Measurement phase (kết quả được thu thập)
//  5. In báo cáo
func (r *Runner) Run(ctx context.Context) {
	// Bước 1: Kiểm tra kết nối
	fmt.Printf("Connecting to %s...\n", r.db.Name())
	if err := r.db.Ping(ctx); err != nil {
		log.Fatalf("Không thể kết nối tới database: %v", err)
	}
	if err := r.db.Init(ctx); err != nil {
		fmt.Printf("Lưu ý khi Init DB: %v\n", err)
	}
	fmt.Println("Connected and Initialized.")

	// Bước 2: In cấu hình
	r.printConfig()

	if r.cfg.Mode == "load" {
		fmt.Printf("\nStarting data load for %d keys...\n", r.cfg.Data.KeyCount)
		r.runLoad(ctx)
		fmt.Println("Data load completed.")
		PrintDataStats(r.cfg)
		return
	}

	// Bước 3: Warmup phase - chạy benchmark nhưng bỏ kết quả
	// Mục đích: làm ấm connection pool, cache, v.v. để measurement
	// phase phản ánh đúng hiệu năng steady-state.
	if r.cfg.Warmup.Duration.Duration > 0 {
		fmt.Printf("\nStarting warmup (%v)...\n", r.cfg.Warmup.Duration.Duration)
		r.runPhase(ctx, r.cfg.Warmup.Duration.Duration)
		fmt.Println("Warmup completed. Resetting metrics.")
	}

	// Bước 4: Measurement phase
	fmt.Printf("\nStarting benchmark (%v)...\n", r.cfg.Benchmark.Duration.Duration)
	result := r.runMeasurement(ctx)
	fmt.Println("Benchmark completed.")

	// Bước 5: Report
	PrintResults(r.cfg, result)
}

// runLoad chạy phase đổ dữ liệu (Load mode).
// Không áp dụng rate-limit — đẩy dữ liệu nhanh nhất có thể.
// Dùng LoadGenerator sinh tuần tự đủ key_count request SET, sau đó dừng.
//
// Khác biệt so với benchmark mode:
//   - Không dùng RateController (không rate-limit)
//   - Producer đẩy request blocking (không drop)
//   - Có progress reporting mỗi 2 giây
func (r *Runner) runLoad(ctx context.Context) {
	requests := make(chan Request, r.cfg.Load.QueueSize)

	gen := NewLoadGenerator(r.cfg)

	// Load mode sử dụng concurrency riêng, mặc định cao hơn benchmark mode
	// để tận dụng tối đa throughput khi đổ dữ liệu.
	concurrency := r.cfg.Load.MaxConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	wp := NewWorkerPool(r.db, concurrency)

	// Producer goroutine: sinh request và đẩy vào queue KHÔNG rate-limit.
	// Blocking send đảm bảo không drop request nào — nếu queue đầy, chờ worker xử lý.
	producerDone := make(chan struct{})
	go func() {
		defer close(requests)
		defer close(producerDone)
		for {
			req, hasMore := gen.Next()
			if !hasMore {
				return
			}
			select {
			case <-ctx.Done():
				return
			case requests <- req:
			}
		}
	}()

	// Progress reporter: in tiến độ mỗi 2 giây để người dùng theo dõi.
	progressDone := make(chan struct{})
	loadStart := time.Now()
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-producerDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(loadStart)
				loaded := gen.Current()
				total := r.cfg.Data.KeyCount
				tps := float64(loaded) / elapsed.Seconds()
				pct := float64(loaded) * 100 / float64(total)
				fmt.Printf("  Progress: %d/%d (%.1f%%) | %.0f keys/s | elapsed %v\n",
					loaded, total, pct, tps, elapsed.Round(time.Second))
			}
		}
	}()

	results := wp.Run(ctx, requests)
	<-producerDone
	<-progressDone

	elapsed := time.Since(loadStart)
	avgTPS := float64(r.cfg.Data.KeyCount) / elapsed.Seconds()

	// Tính tổng số lỗi trong load phase để báo cáo ngắn gọn
	var errors int64
	for _, res := range results {
		for i := 0; i < 4; i++ {
			errors += res.Ops[i].Errors
		}
	}
	fmt.Printf("\nLoad completed: %d keys in %v (avg %.0f keys/s)\n",
		r.cfg.Data.KeyCount, elapsed.Round(time.Millisecond), avgTPS)
	if errors > 0 {
		fmt.Printf("⚠ Data load finished with %d errors.\n", errors)
	} else {
		fmt.Println("✓ All data loaded successfully without errors.")
	}
}

// runPhase chạy một phase (warmup) trong khoảng thời gian duration.
// Kết quả metrics bị bỏ qua - dùng cho warmup.
//
// Flow bên trong giống measurement nhưng không thu thập kết quả:
//   - RateController phát request vào queue theo target TPS
//   - WorkerPool nhận và thực thi request
//   - Khi hết duration, tất cả dừng lại
func (r *Runner) runPhase(ctx context.Context, duration time.Duration) {
	phaseCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	requests := make(chan Request, r.cfg.Load.QueueSize)

	gen := NewWorkloadGenerator(r.cfg)
	rc := NewRateController(r.cfg.Load.TargetTPS)
	wp := NewWorkerPool(r.db, r.cfg.Load.MaxConcurrency)

	// Chạy rate controller trong goroutine riêng
	rcDone := make(chan struct{})
	go func() {
		rc.Run(phaseCtx, gen, requests)
		close(requests) // Đóng channel để worker biết không còn request mới
		close(rcDone)
	}()

	// Chạy worker pool (blocking đến khi tất cả worker hoàn thành)
	wp.Run(phaseCtx, requests)
	<-rcDone
}

// runMeasurement chạy measurement phase và trả về kết quả benchmark.
//
// Sử dụng hai context riêng biệt:
//   - genCtx: cho RateController, hết hạn sau benchmark duration
//   - workerCtx: cho WorkerPool, hết hạn sau benchmark duration + grace period
//
// Lý do tách: sau khi RateController dừng phát request, WorkerPool cần
// thêm thời gian (grace period) để drain hết request còn trong queue.
// Nếu grace period hết mà vẫn còn request in-flight, worker sẽ bị force-stop.
func (r *Runner) runMeasurement(ctx context.Context) *BenchmarkResult {
	requests := make(chan Request, r.cfg.Load.QueueSize)

	gen := NewWorkloadGenerator(r.cfg)
	rc := NewRateController(r.cfg.Load.TargetTPS)
	wp := NewWorkerPool(r.db, r.cfg.Load.MaxConcurrency)

	var issued int64
	var genDuration time.Duration // thời gian thực tế rate controller chạy (measurement window)
	start := time.Now()

	// Context cho rate controller: hết hạn sau benchmark duration
	genCtx, genCancel := context.WithTimeout(ctx, r.cfg.Benchmark.Duration.Duration)
	defer genCancel()

	// Context cho worker: benchmark duration + grace period
	// Cho phép worker drain hết request còn trong queue trước khi dừng
	totalDuration := r.cfg.Benchmark.Duration.Duration + r.cfg.Shutdown.GracePeriod.Duration
	workerCtx, workerCancel := context.WithTimeout(ctx, totalDuration)
	defer workerCancel()

	// Chạy rate controller trong goroutine riêng
	rcDone := make(chan struct{})
	go func() {
		issued = rc.Run(genCtx, gen, requests)
		genDuration = time.Since(start) // Đo measurement window = thời gian rate controller active
		close(requests)                 // Đóng channel → worker biết không còn request mới
		close(rcDone)
	}()

	// Chạy worker pool (blocking đến khi tất cả worker hoàn thành hoặc ctx hết hạn)
	results := wp.Run(workerCtx, requests)
	<-rcDone // sync: đảm bảo genDuration đã được ghi trước khi đọc (happens-before)
	workerDuration := time.Since(start) // thời gian thực tế workers chạy (bao gồm grace period)

	// Tổng hợp kết quả với 2 duration riêng biệt:
	// - genDuration: cho Issued TPS (measurement window)
	// - workerDuration: cho Completed/Success/Error TPS (tránh inflate)
	return MergeWorkerResults(results, genDuration, workerDuration, r.cfg.Load.TargetTPS, issued, rc.Dropped())
}

// printConfig in tóm tắt cấu hình benchmark ra stdout.
func (r *Runner) printConfig() {
	total := r.cfg.Workload.Get + r.cfg.Workload.Set + r.cfg.Workload.Delete + r.cfg.Workload.Query
	getPct, setPct, delPct, qryPct := 0, 0, 0, 0
	if total > 0 {
		getPct = r.cfg.Workload.Get * 100 / total
		setPct = r.cfg.Workload.Set * 100 / total
		delPct = r.cfg.Workload.Delete * 100 / total
		qryPct = 100 - getPct - setPct - delPct
	}

	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Printf("  Mode:           %s\n", r.cfg.Mode)
	fmt.Printf("  Database:       %s\n", r.cfg.Database.Type)
	fmt.Printf("  Target TPS:     %d\n", r.cfg.Load.TargetTPS)
	fmt.Printf("  Concurrency:    %d\n", r.cfg.Load.MaxConcurrency)
	fmt.Printf("  Queue Size:     %d\n", r.cfg.Load.QueueSize)
	fmt.Printf("  Key Count:      %d\n", r.cfg.Data.KeyCount)
	fmt.Printf("  Payload Size:   %d bytes\n", r.cfg.Data.PayloadSize)
	fmt.Printf("  Seed:           %d\n", r.cfg.Data.Seed)
	fmt.Printf("  Workload:       GET=%d%% SET=%d%% DELETE=%d%% QUERY=%d%%\n", getPct, setPct, delPct, qryPct)
	fmt.Printf("  Warmup:         %v\n", r.cfg.Warmup.Duration.Duration)
	fmt.Printf("  Benchmark:      %v\n", r.cfg.Benchmark.Duration.Duration)
	fmt.Printf("  Grace Period:   %v\n", r.cfg.Shutdown.GracePeriod.Duration)
}
