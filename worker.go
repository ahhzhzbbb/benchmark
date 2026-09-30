package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// OpMetrics lưu trữ metrics cho một loại operation cụ thể
type OpMetrics struct {
	Completed int64
	Success   int64
	Errors    int64
	Latencies []time.Duration
}

// WorkerResult chứa kết quả thu thập từ một worker trong suốt quá trình benchmark.
// Mỗi worker có WorkerResult riêng, không chia sẻ → không cần lock/mutex.
type WorkerResult struct {
	Ops [4]OpMetrics // 0: GET, 1: SET, 2: DELETE, 3: QUERY
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
func (wp *WorkerPool) Run(ctx context.Context, requests <-chan Request) []*WorkerResult {
	results := make([]*WorkerResult, wp.concurrency)
	var wg sync.WaitGroup

	for i := 0; i < wp.concurrency; i++ {
		wg.Add(1)
		res := &WorkerResult{}
		for j := 0; j < 4; j++ {
			// Pre-allocate slice latency để giảm re-allocation khi benchmark đang chạy
			res.Ops[j].Latencies = make([]time.Duration, 0, 256)
		}
		results[i] = res

		go func(workerID int, result *WorkerResult) {
			defer wg.Done()
			workerLoop(ctx, wp.db, requests, result)
		}(i, results[i])
	}

	wg.Wait()
	return results
}

// workerLoop là vòng lặp chính của mỗi worker.
// Worker lấy request từ channel, thực thi lên database và ghi nhận kết quả.
//
// Latency chỉ đo thời gian thực thi database operation,
// KHÔNG bao gồm thời gian request nằm chờ trong queue.
func workerLoop(ctx context.Context, db Database, requests <-chan Request, result *WorkerResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-requests:
			if !ok {
				// Channel đã đóng, không còn request mới
				return
			}
			executeRequest(db, req, result)
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
func executeRequest(db Database, req Request, result *WorkerResult) {
	ctx := context.Background()
	start := time.Now()
	var err error

	switch req.Operation {
	case OpGet:
		err = db.Get(ctx, req.Key)
	case OpSet:
		err = db.Set(ctx, req.Key, req.Value)
	case OpDelete:
		err = db.Delete(ctx, req.Key)
	case OpQuery:
		err = db.Query(ctx, req.QueryStr)
	}

	latency := time.Since(start)
	op := int(req.Operation)
	
	result.Ops[op].Completed++

	if err != nil {
		result.Ops[op].Errors++
		if req.Operation == OpQuery {
			log.Printf("[ERROR] %s query=%q err=%v", req.Operation, req.QueryStr, err)
		} else {
			log.Printf("[ERROR] %s key=%q err=%v", req.Operation, req.Key, err)
		}
	} else {
		result.Ops[op].Success++
		result.Ops[op].Latencies = append(result.Ops[op].Latencies, latency)
	}
}
