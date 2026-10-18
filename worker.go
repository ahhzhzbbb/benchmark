package main

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var errLogger *log.Logger

func init() {
	f, err := os.OpenFile("benchmark_errors.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err == nil {
		errLogger = log.New(f, "", log.LstdFlags)
	} else {
		errLogger = log.New(os.Stderr, "", log.LstdFlags)
	}
}

// OpMetrics lưu trữ metrics cho một loại operation cụ thể
type OpMetrics struct {
	Completed int64 // = Success + NotFound + Errors
	Success   int64
	NotFound  int64 // GET/DELETE nhắm vào key không tồn tại (không phải lỗi, không tính là success)
	Errors    int64
	Latencies []time.Duration // chỉ gồm latency của request Success
}

// WorkerResult chứa kết quả thu thập từ một worker trong suốt quá trình benchmark.
type WorkerResult struct {
	Ops [5]OpMetrics // 0: GET, 1: SET, 2: DELETE, 3: QUERY_TAG, 4: QUERY_NUMERIC

	mu              sync.Mutex
	windowLatencies []time.Duration // Latency của window hiện tại (dùng cho open-loop realtime per-second)
}

// RecordLatency ghi nhận latency một cách thread-safe.
// cumulative = true: lưu toàn bộ latency vào Ops[op].Latencies (dùng cho mode run).
// cumulative = false: chỉ lưu vào windowLatencies (dùng cho mode open-loop, tránh rò rỉ RAM).
func (r *WorkerResult) RecordLatency(op int, latency time.Duration, cumulative bool) {
	r.mu.Lock()
	if cumulative {
		r.Ops[op].Latencies = append(r.Ops[op].Latencies, latency)
	}
	r.windowLatencies = append(r.windowLatencies, latency)
	r.mu.Unlock()
}

// SwapWindowLatencies trích xuất toàn bộ latency trong 1 giây vừa qua và hoán đổi buffer mới.
// Thao tác lock cực nhanh (<50ns) chỉ để swap con trỏ slice, loại bỏ 100% Data Race.
func (r *WorkerResult) SwapWindowLatencies() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := r.windowLatencies
	r.windowLatencies = make([]time.Duration, 0, 2048)
	return res
}

// WorkerPool quản lý một nhóm worker goroutine.
// Worker chỉ có nhiệm vụ lấy request từ queue và thực thi lên database.
// Worker KHÔNG quyết định tốc độ gửi request (đó là việc của RateController).
type WorkerPool struct {
	db          Database
	concurrency int
}

// NewWorkerPool tạo worker pool với số lượng worker (concurrency) cho trước.
func NewWorkerPool(db Database, concurrency int) *WorkerPool {
	return &WorkerPool{
		db:          db,
		concurrency: concurrency,
	}
}

// Start khởi chạy tất cả worker dưới nền và trả về danh sách WorkerResult cùng hàm wait.
// cumulative=true: lưu giữ toàn bộ latency (dùng cho mode run).
// cumulative=false: chỉ dùng windowLatencies (dùng cho open-loop mode, tránh phình bộ nhớ).
func (wp *WorkerPool) Start(ctx context.Context, requests <-chan Request, trackLatency bool, cumulative bool) ([]*WorkerResult, func()) {
	results := make([]*WorkerResult, wp.concurrency)
	var wg sync.WaitGroup

	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		res := &WorkerResult{
			windowLatencies: make([]time.Duration, 0, 1024),
		}
		if trackLatency && cumulative {
			for j := 0; j < len(res.Ops); j++ {
				res.Ops[j].Latencies = make([]time.Duration, 0, 256)
			}
		}
		results[i] = res

		go func(workerID int, result *WorkerResult) {
			defer wg.Done()
			workerLoop(ctx, wp.db, requests, result, trackLatency, cumulative)
		}(i, results[i])
	}

	return results, func() { wg.Wait() }
}

// Run khởi chạy tất cả worker và chờ chúng hoàn thành.
func (wp *WorkerPool) Run(ctx context.Context, requests <-chan Request, trackLatency bool) []*WorkerResult {
	results, wait := wp.Start(ctx, requests, trackLatency, true)
	wait()
	return results
}

// workerLoop là vòng lặp chính của mỗi worker.
func workerLoop(ctx context.Context, db Database, requests <-chan Request, result *WorkerResult, trackLatency bool, cumulative bool) {
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-requests:
			if !ok {
				return
			}
			executeRequest(db, req, result, trackLatency, cumulative)
		}
	}
}

// executeRequest thực thi một request lên database và ghi nhận kết quả.
// DB Latency (Service Time): Đo từ lúc bắt đầu gọi DB đến khi nhận response (loại trừ hoàn toàn thời gian chờ trong queue).
func executeRequest(db Database, req Request, result *WorkerResult, trackLatency bool, cumulative bool) {
	ctx := context.Background()
	start := time.Now() // DB Latency (Service Time)
	var err error

	switch req.Operation {
	case OpGet:
		err = db.Get(ctx, req.Key)
	case OpSet:
		err = db.Set(ctx, req.Key, req.Payload)
	case OpDelete:
		err = db.Delete(ctx, req.Key)
	case OpQueryTag, OpQueryNumeric:
		err = db.Query(ctx, req.Query)
	}

	latency := time.Since(start)
	op := int(req.Operation)

	atomic.AddInt64(&result.Ops[op].Completed, 1)

	if err == nil {
		atomic.AddInt64(&result.Ops[op].Success, 1)
		if trackLatency {
			result.RecordLatency(op, latency, cumulative)
		}
		return
	}

	if errors.Is(err, ErrNotFound) {
		// Miss nhanh hơn nhiều so với hit → không đưa vào phân bố latency để tránh làm lệch percentiles.
		atomic.AddInt64(&result.Ops[op].NotFound, 1)
		return
	}

	errCount := atomic.AddInt64(&result.Ops[op].Errors, 1)
	if errCount%100 == 1 {
		log.Printf("[DB ERROR SAMPLE] %s err=%v", req.Operation, err)
	}
	// Lấy mẫu ghi log để tránh tranh chấp khóa toàn cục log.Logger và nghẽn disk I/O khi lỗi dồn dập
	if errCount <= 10 || errCount%100 == 1 {
		if req.Operation >= OpQueryTag {
			errLogger.Printf("[ERROR #%d] %s query=%v err=%v", errCount, req.Operation, req.Query, err)
		} else {
			errLogger.Printf("[ERROR #%d] %s key=%q err=%v", errCount, req.Operation, req.Key, err)
		}
	}
}
