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
// Mỗi worker có WorkerResult riêng, không chia sẻ → không cần lock/mutex.
type WorkerResult struct {
	Ops [5]OpMetrics // 0: GET, 1: SET, 2: DELETE, 3: QUERY_TAG, 4: QUERY_NUMERIC
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

// Run khởi chạy tất cả worker và chờ chúng hoàn thành.
// Mỗi worker đọc request từ channel requests, thực thi lên database,
// đo latency và lưu kết quả cục bộ.
//
// Run trả về danh sách WorkerResult từ tất cả worker sau khi channel
// requests bị đóng hoặc ctx bị cancel.
func (wp *WorkerPool) Start(ctx context.Context, requests <-chan Request, trackLatency bool) ([]*WorkerResult, func()) {
	results := make([]*WorkerResult, wp.concurrency)
	var wg sync.WaitGroup

	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		res := &WorkerResult{}
		for j := 0; j < len(res.Ops); j++ {
			// Pre-allocate slice latency để giảm re-allocation khi benchmark đang chạy
			if trackLatency {
				res.Ops[j].Latencies = make([]time.Duration, 0, 256)
			}
		}
		results[i] = res

		go func(workerID int, result *WorkerResult) {
			defer wg.Done()
			workerLoop(ctx, wp.db, requests, result, trackLatency)
		}(i, results[i])
	}

	return results, func() { wg.Wait() }
}

// Run khởi chạy tất cả worker và chờ chúng hoàn thành.
// Mỗi worker đọc request từ channel requests, thực thi lên database,
// đo latency và lưu kết quả cục bộ.
//
// Run trả về danh sách WorkerResult từ tất cả worker sau khi channel
// requests bị đóng hoặc ctx bị cancel.
func (wp *WorkerPool) Run(ctx context.Context, requests <-chan Request, trackLatency bool) []*WorkerResult {
	results, wait := wp.Start(ctx, requests, trackLatency)
	wait()
	return results
}

// workerLoop là vòng lặp chính của mỗi worker.
// Worker lấy request từ channel, thực thi lên database và ghi nhận kết quả.
//
// Latency chỉ đo thời gian thực thi database operation,
// KHÔNG bao gồm thời gian request nằm chờ trong queue.
func workerLoop(ctx context.Context, db Database, requests <-chan Request, result *WorkerResult, trackLatency bool) {
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-requests:
			if !ok {
				// Channel đã đóng, không còn request mới
				return
			}
			executeRequest(db, req, result, trackLatency)
		}
	}
}

// executeRequest thực thi một request lên database và ghi nhận kết quả.
// Đo latency từ lúc bắt đầu gọi database đến lúc nhận response.
//
// Sử dụng context.Background() thay vì workerCtx để tránh context cancellation
// cascade: khi workerCtx hết hạn, DB calls đang in-flight sẽ KHÔNG bị cancel,
// cho phép đo latency chính xác. go-redis built-in timeouts (ReadTimeout=3s)
// cung cấp safety net chống hang.
func executeRequest(db Database, req Request, result *WorkerResult, trackLatency bool) {
	ctx := context.Background()
	start := req.CreatedAt
	if start.IsZero() {
		start = time.Now()
	}
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
	if trackLatency {
		result.Ops[op].Latencies = append(result.Ops[op].Latencies, latency)
	}

	if err == nil {
		atomic.AddInt64(&result.Ops[op].Success, 1)
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
	if req.Operation >= OpQueryTag {
		errLogger.Printf("[ERROR] %s query=%v err=%v", req.Operation, req.Query, err)
	} else {
		errLogger.Printf("[ERROR] %s key=%q err=%v", req.Operation, req.Key, err)
	}
}
