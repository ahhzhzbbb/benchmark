package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
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
	cfg  Config
	db   Database
	keys *KeySpace // trạng thái key dùng chung giữa warmup và measurement
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

	// KeySpace giả định toàn bộ key_count record đã được nạp bằng mode load.
	r.keys = NewKeySpace(r.cfg.Data.KeyCount)

	if r.cfg.Mode == "open-loop" {
		fmt.Println("\nStarting open-loop continuous benchmark. Press Ctrl+C to stop...")
		r.runOpenLoop(ctx)
		return
	}

	// Bước 3: Warmup phase - chạy benchmark nhưng bỏ kết quả
	// Mục đích: làm ấm connection pool, cache, v.v. để measurement
	// phase phản ánh đúng hiệu năng steady-state.
	// Warmup dùng seed khác measurement → chuỗi request khác nhau.
	if r.cfg.Warmup.Duration.Duration > 0 {
		fmt.Printf("\nStarting warmup (%v)...\n", r.cfg.Warmup.Duration.Duration)
		r.runPhase(ctx, r.cfg.Warmup.Duration.Duration, r.cfg.Data.Seed+warmupSeedOffset)
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

	results := wp.Run(ctx, requests, false)
	<-producerDone
	<-progressDone

	elapsed := time.Since(loadStart)
	avgTPS := float64(r.cfg.Data.KeyCount) / elapsed.Seconds()

	// Tính tổng số lỗi trong load phase để báo cáo ngắn gọn
	var errors int64
	for _, res := range results {
		for i := 0; i < len(res.Ops); i++ {
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
func (r *Runner) runPhase(ctx context.Context, duration time.Duration, streamSeed int64) {
	phaseCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	concurrency := r.cfg.Load.MaxConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	queueSizePerWorker := r.cfg.Load.QueueSize / concurrency
	if queueSizePerWorker < 2 {
		queueSizePerWorker = 2
	}

	rcs := NewRateControllerGroup(r.cfg.Load.TargetTPS, concurrency)
	reqChans := make([]chan Request, concurrency)
	gens := make([]*WorkloadGenerator, concurrency)

	for i := 0; i < concurrency; i++ {
		reqChans[i] = make(chan Request, queueSizePerWorker)
		gens[i] = NewWorkloadGenerator(r.cfg, streamSeed+int64(i)*10007, r.keys)
	}

	var rcWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		rcWg.Add(1)
		go func(idx int) {
			defer rcWg.Done()
			rcs[idx].Run(phaseCtx, gens[idx], reqChans[idx])
			close(reqChans[idx])
		}(i)
	}

	dummyRes := &WorkerResult{}
	var workerWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workerWg.Add(1)
		go func(idx int) {
			defer workerWg.Done()
			for {
				select {
				case <-phaseCtx.Done():
					return
				case req, ok := <-reqChans[idx]:
					if !ok {
						return
					}
					executeRequest(r.db, req, dummyRes, false, false)
				}
			}
		}(i)
	}

	rcWg.Wait()
	workerWg.Wait()
}

// runMeasurement chạy measurement phase và trả về kết quả benchmark.
// Sử dụng phân tán RateController theo từng worker để triệt tiêu 100% lock contention trên channel.
func (r *Runner) runMeasurement(ctx context.Context) *BenchmarkResult {
	concurrency := r.cfg.Load.MaxConcurrency
	if concurrency < 1 {
		concurrency = 1
	}

	queueSizePerWorker := r.cfg.Load.QueueSize / concurrency
	if queueSizePerWorker < 2 {
		queueSizePerWorker = 2
	}

	rcs := NewRateControllerGroup(r.cfg.Load.TargetTPS, concurrency)
	reqChans := make([]chan Request, concurrency)
	gens := make([]*WorkloadGenerator, concurrency)
	results := make([]*WorkerResult, concurrency)

	for i := 0; i < concurrency; i++ {
		reqChans[i] = make(chan Request, queueSizePerWorker)
		gens[i] = NewWorkloadGenerator(r.cfg, r.cfg.Data.Seed+int64(i)*10007, r.keys)
		res := &WorkerResult{
			windowLatencies: make([]time.Duration, 0, 2048),
		}
		for j := 0; j < len(res.Ops); j++ {
			res.Ops[j].Latencies = make([]time.Duration, 0, 256)
		}
		results[i] = res
	}

	start := time.Now()

	// Context cho rate controller: hết hạn sau benchmark duration
	genCtx, genCancel := context.WithTimeout(ctx, r.cfg.Benchmark.Duration.Duration)
	defer genCancel()

	// Context cho worker: benchmark duration + grace period
	totalDuration := r.cfg.Benchmark.Duration.Duration + r.cfg.Shutdown.GracePeriod.Duration
	workerCtx, workerCancel := context.WithTimeout(ctx, totalDuration)
	defer workerCancel()

	var rcWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		rcWg.Add(1)
		go func(idx int) {
			defer rcWg.Done()
			rcs[idx].Run(genCtx, gens[idx], reqChans[idx])
			close(reqChans[idx])
		}(i)
	}

	var workerWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workerWg.Add(1)
		go func(idx int) {
			defer workerWg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case req, ok := <-reqChans[idx]:
					if !ok {
						return
					}
					executeRequest(r.db, req, results[idx], true, true)
				}
			}
		}(i)
	}

	// Đợi RateControllers xong (sau benchmark duration)
	rcWg.Wait()
	genDuration := time.Since(start)

	// Đợi Workers drain hết queue hoặc hết grace period
	workerWg.Wait()
	workerDuration := time.Since(start)

	issued := rcs.Issued()
	dropped := rcs.Dropped()

	return MergeWorkerResults(results, genDuration, workerDuration, r.cfg.Load.TargetTPS, issued, dropped)
}

// printConfig in tóm tắt cấu hình benchmark ra stdout.
func (r *Runner) printConfig() {
	total := r.cfg.Workload.Get + r.cfg.Workload.Set + r.cfg.Workload.Delete + r.cfg.Workload.QueryTag + r.cfg.Workload.QueryNumeric
	getPct, setPct, delPct, qtPct, qnPct := 0, 0, 0, 0, 0
	if total > 0 {
		getPct = r.cfg.Workload.Get * 100 / total
		setPct = r.cfg.Workload.Set * 100 / total
		delPct = r.cfg.Workload.Delete * 100 / total
		qtPct = r.cfg.Workload.QueryTag * 100 / total
		qnPct = 100 - getPct - setPct - delPct - qtPct
	}

	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Printf("  Mode:           %s\n", r.cfg.Mode)
	fmt.Printf("  Database:       %s\n", r.cfg.Database.Type)
	fmt.Printf("  Target TPS:     %d\n", r.cfg.Load.TargetTPS)
	fmt.Printf("  Concurrency:    %d\n", r.cfg.Load.MaxConcurrency)
	fmt.Printf("  Queue Size:     %d total (%d per worker)\n", r.cfg.Load.QueueSize, r.cfg.Load.QueueSize/r.cfg.Load.MaxConcurrency)
	if r.cfg.Load.GOMAXPROCS > 0 {
		fmt.Printf("  GOMAXPROCS:     %d\n", r.cfg.Load.GOMAXPROCS)
	} else {
		fmt.Printf("  GOMAXPROCS:     %d (system default)\n", runtime.GOMAXPROCS(0))
	}
	fmt.Printf("  Key Count:      %d\n", r.cfg.Data.KeyCount)
	fmt.Printf("  Payload Size:   %d bytes\n", r.cfg.Data.PayloadSize)
	fmt.Printf("  Seed:           %d\n", r.cfg.Data.Seed)
	fmt.Printf("  Workload:       GET=%d%% SET=%d%% DELETE=%d%% QUERY_TAG=%d%% QUERY_NUMERIC=%d%%\n", getPct, setPct, delPct, qtPct, qnPct)
	fmt.Printf("  Warmup:         %v\n", r.cfg.Warmup.Duration.Duration)
	fmt.Printf("  Benchmark:      %v\n", r.cfg.Benchmark.Duration.Duration)
	fmt.Printf("  Grace Period:   %v\n", r.cfg.Shutdown.GracePeriod.Duration)

	if r.cfg.Mode == "load" {
		return
	}

	// Cảnh báo: DELETE nhiều hơn SET → dataset sẽ teo dần trong lúc benchmark.
	if r.cfg.Workload.Delete > r.cfg.Workload.Set {
		fmt.Printf("  ⚠ WARNING: delete (%d) > set (%d): số record trong DB sẽ giảm dần trong lúc benchmark.\n"+
			"    Đặt set >= delete để dataset ổn định (mỗi SET sẽ tái tạo một key đã bị xoá).\n",
			r.cfg.Workload.Delete, r.cfg.Workload.Set)
	}

	// Cảnh báo: queue quá nhỏ so với TPS → chỉ một đợt chậm rất ngắn cũng gây drop.
	// Ngưỡng: queue phải chứa được ít nhất ~10ms tải.
	if minQueue := r.cfg.Load.TargetTPS / 100; r.cfg.Load.QueueSize < minQueue {
		fmt.Printf("  ⚠ WARNING: queue_size (%d) chỉ chứa được < 10ms tải ở %d TPS, dễ bị drop. Nên >= %d.\n",
			r.cfg.Load.QueueSize, r.cfg.Load.TargetTPS, minQueue)
	}
}

// runOpenLoop chạy chế độ đo lường liên tục (infinite).
// Phát request với TPS mục tiêu, báo cáo real-time sau mỗi giây.
// Tự động dừng khi nhận ctx.Done() (thường là do Ctrl+C).
func (r *Runner) runOpenLoop(ctx context.Context) {
	concurrency := r.cfg.Load.MaxConcurrency
	if concurrency < 1 {
		concurrency = 1
	}

	queueSizePerWorker := r.cfg.Load.QueueSize / concurrency
	if queueSizePerWorker < 2 {
		queueSizePerWorker = 2
	}

	rcs := NewRateControllerGroup(r.cfg.Load.TargetTPS, concurrency)
	reqChans := make([]chan Request, concurrency)
	gens := make([]*WorkloadGenerator, concurrency)
	results := make([]*WorkerResult, concurrency)

	for i := 0; i < concurrency; i++ {
		reqChans[i] = make(chan Request, queueSizePerWorker)
		gens[i] = NewWorkloadGenerator(r.cfg, r.cfg.Data.Seed+int64(i)*10007, r.keys)
		results[i] = &WorkerResult{
			windowLatencies: make([]time.Duration, 0, 2048),
		}
	}

	// Chạy các rate controller phân tán
	var rcWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		rcWg.Add(1)
		go func(idx int) {
			defer rcWg.Done()
			rcs[idx].Run(ctx, gens[idx], reqChans[idx])
			close(reqChans[idx])
		}(i)
	}

	// Chạy các worker tương ứng với từng channel riêng biệt (zero lock contention)
	var workerWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workerWg.Add(1)
		go func(idx int) {
			defer workerWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case req, ok := <-reqChans[idx]:
					if !ok {
						return
					}
					executeRequest(r.db, req, results[idx], true, false)
				}
			}
		}(i)
	}

	// Bắt đầu Reporter loop
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastSuccess, lastNotFound, lastErrors, lastIssued, lastScheduled, lastDropped int64
	startTime := time.Now()
	lastTick := startTime

	fmt.Println("\n--- Open-Loop Mode (Partitioned Workers) ---")
	fmt.Printf("Target TPS:     %d\n", r.cfg.Load.TargetTPS)
	fmt.Printf("Concurrency:    %d workers (zero centralized channel contention)\n", r.cfg.Load.MaxConcurrency)
	fmt.Printf("Queue Size:     %d total (%d per worker)\n\n", r.cfg.Load.QueueSize, queueSizePerWorker)

reporterLoop:
	for {
		select {
		case <-ctx.Done():
			break reporterLoop
		case <-ticker.C:
			var currentSuccess, currentNotFound, currentErrors int64
			var currentLatencies []time.Duration

			// Tổng hợp metrics và trích xuất latency của đúng 1 giây vừa qua (thread-safe qua SwapWindowLatencies)
			for _, res := range results {
				for i := 0; i < len(res.Ops); i++ {
					currentSuccess += atomic.LoadInt64(&res.Ops[i].Success)
					currentNotFound += atomic.LoadInt64(&res.Ops[i].NotFound)
					currentErrors += atomic.LoadInt64(&res.Ops[i].Errors)
				}
				currentLatencies = append(currentLatencies, res.SwapWindowLatencies()...)
			}

			currentScheduled := rcs.Scheduled()
			currentDropped := rcs.Dropped()
			currentIssued := rcs.Issued()
			currentCompleted := currentSuccess + currentNotFound + currentErrors
			inFlight := currentIssued - currentCompleted
			if inFlight < 0 {
				inFlight = 0
			}

			now := time.Now()
			deltaSec := now.Sub(lastTick).Seconds()
			lastTick = now

			var schedTPS, sentTPS, ackTPS, errorTPS, dropTPS int64
			if deltaSec > 0 {
				schedTPS = int64(float64(currentScheduled-lastScheduled) / deltaSec)
				sentTPS = int64(float64(currentIssued-lastIssued) / deltaSec)
				lastCompleted := lastSuccess + lastNotFound + lastErrors
				ackTPS = int64(float64(currentCompleted-lastCompleted) / deltaSec)
				errorTPS = int64(float64(currentErrors-lastErrors) / deltaSec)
				dropTPS = int64(float64(currentDropped-lastDropped) / deltaSec)
			}

			errorRate := 0.0
			if ackTPS > 0 {
				errorRate = float64(errorTPS) / float64(ackTPS) * 100
			}

			// Tính latency percentiles chuẩn xác của 1 giây vừa qua (Realtime Window)
			var p50, p95, p99 time.Duration
			if len(currentLatencies) > 0 {
				sort.Slice(currentLatencies, func(i, j int) bool {
					return currentLatencies[i] < currentLatencies[j]
				})
				p50 = percentile(currentLatencies, 50)
				p95 = percentile(currentLatencies, 95)
				p99 = percentile(currentLatencies, 99)
			}

			elapsed := time.Since(startTime)
			fmt.Printf("[%02d:%02d:%02d] Sched: %5d/s | Sent: %5d/s | Ack: %5d/s (Err: %4d, %5.2f%%) | Drop: %4d/s (tot %d) | InFlight: %4d | P50: %v | P95: %v | P99: %v\n",
				int(elapsed.Hours()), int(elapsed.Minutes())%60, int(elapsed.Seconds())%60,
				schedTPS, sentTPS, ackTPS, errorTPS, errorRate, dropTPS, currentDropped, inFlight, p50, p95, p99)

			lastScheduled = currentScheduled
			lastSuccess = currentSuccess
			lastNotFound = currentNotFound
			lastErrors = currentErrors
			lastIssued = currentIssued
			lastDropped = currentDropped
		}
	}

	fmt.Println("\nWaiting for in-flight requests to finish (grace period)...")
	rcWg.Wait()
	workerWg.Wait()
	fmt.Println("Open-loop benchmark stopped.")

	// In bảng tổng kết toàn bộ phiên chạy open-loop
	totalElapsed := time.Since(startTime)
	var finalSuccess, finalNotFound, finalErrors int64
	for _, res := range results {
		for i := 0; i < len(res.Ops); i++ {
			finalSuccess += atomic.LoadInt64(&res.Ops[i].Success)
			finalNotFound += atomic.LoadInt64(&res.Ops[i].NotFound)
			finalErrors += atomic.LoadInt64(&res.Ops[i].Errors)
		}
	}
	finalScheduled := rcs.Scheduled()
	finalIssued := rcs.Issued()
	finalDropped := rcs.Dropped()
	finalCompleted := finalSuccess + finalNotFound + finalErrors

	fmt.Println("\n============================================")
	fmt.Println("       Open-Loop Summary Report")
	fmt.Println("============================================")
	fmt.Printf("Duration:         %v\n", totalElapsed.Round(time.Millisecond))
	fmt.Printf("Target TPS:       %d\n", r.cfg.Load.TargetTPS)
	if totalElapsed.Seconds() > 0 {
		fmt.Printf("Scheduled TPS:    %.2f\n", float64(finalScheduled)/totalElapsed.Seconds())
		fmt.Printf("Issued TPS:       %.2f\n", float64(finalIssued)/totalElapsed.Seconds())
		fmt.Printf("Completed TPS:    %.2f\n", float64(finalCompleted)/totalElapsed.Seconds())
		fmt.Printf("Success TPS:      %.2f\n", float64(finalSuccess)/totalElapsed.Seconds())
		fmt.Printf("Error TPS:        %.2f\n", float64(finalErrors)/totalElapsed.Seconds())
	}
	fmt.Println()
	fmt.Printf("Total Scheduled:  %d\n", finalScheduled)
	fmt.Printf("Total Issued:     %d\n", finalIssued)
	fmt.Printf("Total Completed:  %d\n", finalCompleted)
	fmt.Printf("Success:          %d\n", finalSuccess)
	if finalNotFound > 0 {
		fmt.Printf("Not Found:        %d\n", finalNotFound)
	}
	fmt.Printf("Errors:           %d\n", finalErrors)
	fmt.Printf("Dropped:          %d\n", finalDropped)
	if finalCompleted > 0 {
		fmt.Printf("Error Rate:       %.2f%%\n", float64(finalErrors)/float64(finalCompleted)*100)
	}
	fmt.Println("============================================")
}
