package main

import (
	"fmt"
	"sort"
	"time"
)

// BenchmarkResult tổng hợp toàn bộ kết quả benchmark.
// Được tạo bằng cách merge kết quả từ tất cả worker sau khi benchmark kết thúc.
type BenchmarkResult struct {
	GenDuration    time.Duration   // Thời gian rate controller active (measurement window)
	WorkerDuration time.Duration   // Thời gian workers chạy thực tế (bao gồm grace period drain)
	TargetTPS      int             // TPS mục tiêu (cấu hình)
	Issued         int64           // Số request đã phát bởi RateController
	Dropped        int64           // Số request bị drop do queue đầy
	Completed      int64           // Tổng request đã hoàn thành (success + not found + error)
	Success        int64           // Số request thành công
	NotFound       int64           // Số GET/DELETE nhắm vào key không tồn tại
	Errors         int64           // Số request lỗi
	Abandoned      int64           // Số request còn trong queue khi benchmark kết thúc (Issued - Completed)
	Latencies      []time.Duration // Danh sách latency của request thành công
	Ops            [5]OpMetrics    // Chi tiết metrics theo từng loại operation (GET, SET, DELETE, QUERY_TAG, QUERY_NUMERIC)
}

// MergeWorkerResults gộp kết quả từ tất cả worker thành một BenchmarkResult.
// genDuration: thời gian rate controller active (measurement window) - dùng cho Issued TPS
// workerDuration: thời gian workers chạy thực tế bao gồm drain - dùng cho Completed TPS
// Pre-allocate slice latency dựa trên tổng capacity để tránh re-allocation.
func MergeWorkerResults(results []*WorkerResult, genDuration, workerDuration time.Duration, targetTPS int, issued int64, dropped int64) *BenchmarkResult {
	br := &BenchmarkResult{
		GenDuration:    genDuration,
		WorkerDuration: workerDuration,
		TargetTPS:      targetTPS,
		Issued:         issued,
		Dropped:        dropped,
	}

	// Ước lượng tổng capacity để pre-allocate
	totalCap := 0
	opCap := [5]int{0, 0, 0, 0, 0}
	for _, r := range results {
		for i := 0; i < len(br.Ops); i++ {
			l := len(r.Ops[i].Latencies)
			totalCap += l
			opCap[i] += l
		}
	}
	br.Latencies = make([]time.Duration, 0, totalCap)
	for i := 0; i < len(br.Ops); i++ {
		br.Ops[i].Latencies = make([]time.Duration, 0, opCap[i])
	}

	for _, r := range results {
		for i := 0; i < len(br.Ops); i++ {
			br.Ops[i].Completed += r.Ops[i].Completed
			br.Ops[i].Success += r.Ops[i].Success
			br.Ops[i].NotFound += r.Ops[i].NotFound
			br.Ops[i].Errors += r.Ops[i].Errors
			br.Ops[i].Latencies = append(br.Ops[i].Latencies, r.Ops[i].Latencies...)

			br.Completed += r.Ops[i].Completed
			br.Success += r.Ops[i].Success
			br.NotFound += r.Ops[i].NotFound
			br.Errors += r.Ops[i].Errors
			br.Latencies = append(br.Latencies, r.Ops[i].Latencies...)
		}
	}

	// Abandoned = requests đã vào queue nhưng không được worker xử lý
	// (do workers không kịp drain trước khi workerCtx hết hạn)
	br.Abandoned = issued - br.Completed

	return br
}

// percentile trả về giá trị latency tại phân vị pct (0-100)
// từ một mảng đã được sắp xếp tăng dần.
func percentile(sorted []time.Duration, pct int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := len(sorted) * pct / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// PrintResults in kết quả benchmark ra stdout.
// Format theo yêu cầu trong TASKS.md section 33.
func PrintResults(cfg Config, result *BenchmarkResult) {
	fmt.Println()
	fmt.Println("============================================")
	fmt.Println("          Benchmark Results")
	fmt.Println("============================================")
	fmt.Println()

	// Thông tin chung
	fmt.Printf("Database:         %s\n", cfg.Database.Type)
	fmt.Printf("Measurement:      %v\n", result.GenDuration.Round(time.Millisecond))
	fmt.Printf("Total Duration:   %v\n", result.WorkerDuration.Round(time.Millisecond))
	fmt.Println()

	// TPS metrics
	// Issued TPS: dùng GenDuration (measurement window - thời gian rate controller chạy)
	issuedTPS := float64(result.Issued) / result.GenDuration.Seconds()
	// Completed/Success/Error TPS: dùng WorkerDuration (bao gồm grace period drain)
	// để phản ánh throughput thực tế, không bị inflated bởi completions trong grace period
	wSeconds := result.WorkerDuration.Seconds()
	completedTPS := float64(result.Completed) / wSeconds
	successTPS := float64(result.Success) / wSeconds
	errorTPS := float64(result.Errors) / wSeconds

	fmt.Printf("Target TPS:       %d\n", result.TargetTPS)
	fmt.Printf("Issued TPS:       %.2f\n", issuedTPS)
	fmt.Printf("Completed TPS:    %.2f\n", completedTPS)
	fmt.Printf("Success TPS:      %.2f\n", successTPS)
	fmt.Printf("Error TPS:        %.2f\n", errorTPS)
	fmt.Println()

	// Tổng số
	fmt.Printf("Total Issued:     %d\n", result.Issued)
	fmt.Printf("Total Completed:  %d\n", result.Completed)
	fmt.Printf("Success:          %d\n", result.Success)
	if result.NotFound > 0 {
		fmt.Printf("Not Found:        %d   (GET/DELETE vào key không tồn tại)\n", result.NotFound)
	}
	fmt.Printf("Errors:           %d\n", result.Errors)
	if result.Dropped > 0 {
		fmt.Printf("Dropped (queue):  %d\n", result.Dropped)
	}
	if result.Abandoned > 0 {
		fmt.Printf("Abandoned:        %d\n", result.Abandoned)
	}

	if result.Completed > 0 {
		errorRate := float64(result.Errors) / float64(result.Completed) * 100
		fmt.Printf("Error Rate:       %.2f%%\n", errorRate)
	}
	fmt.Println()

	// Latency
	if len(result.Latencies) == 0 {
		fmt.Println("No successful requests to calculate latency.")
		return
	}

	sort.Slice(result.Latencies, func(i, j int) bool {
		return result.Latencies[i] < result.Latencies[j]
	})

	var totalLatency time.Duration
	for _, l := range result.Latencies {
		totalLatency += l
	}

	n := len(result.Latencies)
	avg := totalLatency / time.Duration(n)

	fmt.Println("Overall Latency:")
	fmt.Printf("  Avg:            %v\n", avg)
	fmt.Printf("  P50:            %v\n", percentile(result.Latencies, 50))
	fmt.Printf("  P95:            %v\n", percentile(result.Latencies, 95))
	fmt.Printf("  P99:            %v\n", percentile(result.Latencies, 99))
	fmt.Printf("  Max:            %v\n", result.Latencies[n-1])
	fmt.Println()

	opNames := []string{"GET", "SET", "DELETE", "QUERY_TAG", "QUERY_NUMERIC"}
	fmt.Println("Per-Operation Latency:")
	for i := 0; i < len(result.Ops); i++ {
		if len(result.Ops[i].Latencies) == 0 {
			continue
		}

		sort.Slice(result.Ops[i].Latencies, func(a, b int) bool {
			return result.Ops[i].Latencies[a] < result.Ops[i].Latencies[b]
		})

		var tot time.Duration
		for _, l := range result.Ops[i].Latencies {
			tot += l
		}
		nOp := len(result.Ops[i].Latencies)
		avgOp := tot / time.Duration(nOp)

		fmt.Printf("  %s (%d requests):\n", opNames[i], nOp)
		if nf := result.Ops[i].NotFound; nf > 0 {
			fmt.Printf("    Not Found: %d (không tính vào latency)\n", nf)
		}
		fmt.Printf("    Avg: %v, P50: %v, P95: %v, P99: %v, Max: %v\n",
			avgOp,
			percentile(result.Ops[i].Latencies, 50),
			percentile(result.Ops[i].Latencies, 95),
			percentile(result.Ops[i].Latencies, 99),
			result.Ops[i].Latencies[nOp-1])
	}
	fmt.Println()
	fmt.Println("============================================")
}
