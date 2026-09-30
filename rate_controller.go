package main

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	// pregenBufferSize là kích thước buffer cho việc sinh request trước.
	// Goroutine sinh request chạy song song, luôn đi trước vòng lặp dispatch,
	// đảm bảo vòng lặp dispatch không bao giờ bị block bởi chi phí gen.Next().
	pregenBufferSize = 4096

	// spinThreshold: khi thời gian chờ < spinThreshold, dùng busy-wait
	// thay vì timer để đạt độ chính xác timing cao hơn.
	// Timer trên Linux có thể có độ phân giải tối thiểu ~1ms trên một số kernel/VM,
	// busy-wait cho phép timing chính xác đến micro-giây.
	spinThreshold = 100 * time.Microsecond
)

// RateController điều khiển tốc độ phát request theo Target TPS.
//
// Nguyên tắc hoạt động:
//   - Sử dụng absolute deadline: next = start + N * interval
//   - Tránh drift tích luỹ: nếu iteration trước bị chậm, iteration tiếp
//     vẫn được tính từ deadline lý thuyết, không phải thời điểm thực tế.
//   - Rate Controller là thành phần DUY NHẤT quyết định tốc độ phát request.
//     Worker KHÔNG có ticker riêng.
//   - Sử dụng kiến trúc pre-generate: request được sinh trước trong goroutine
//     riêng, tách chi phí gen.Next() ra khỏi vòng lặp timing-critical.
//
// Đây là trái tim của mô hình open-loop benchmark.
type RateController struct {
	interval  time.Duration // khoảng cách giữa hai request liên tiếp
	targetTPS int
	dropped   int64 // số request bị drop do queue đầy (atomic counter)
}

// NewRateController tạo rate controller cho target TPS đã cho.
// interval = 1s / targetTPS, ví dụ: 50000 TPS → interval = 20µs.
func NewRateController(targetTPS int) *RateController {
	interval := time.Duration(float64(time.Second) / float64(targetTPS))
	return &RateController{
		interval:  interval,
		targetTPS: targetTPS,
	}
}

// Run bắt đầu phát request vào channel requests theo tốc độ Target TPS.
//
// Kiến trúc pre-generate:
//  1. Goroutine riêng gọi gen.Next() liên tục, đẩy request vào buffer trước
//  2. Vòng lặp dispatch chỉ lấy request từ buffer (gần như instant),
//     không bị ảnh hưởng bởi chi phí gen.Next() (fmt.Sprintf, RNG, v.v.)
//  3. Timer được tái sử dụng (Reset) thay vì tạo mới mỗi lần, giảm GC pressure
//  4. Khi interval ngắn (< spinThreshold), dùng busy-wait cho timing chính xác
//
// Run chạy blocking trong goroutine riêng. Khi ctx bị cancel, Run thoát
// và trả về tổng số request đã phát thành công (issued).
func (rc *RateController) Run(ctx context.Context, gen Generator, requests chan<- Request) int64 {
	var issued int64

	// --- Pre-generate goroutine ---
	// Sinh request trước vào buffer, chạy song song với vòng lặp dispatch.
	// Chi phí gen.Next() (RNG, string formatting, v.v.) được chuyển sang goroutine này,
	// giải phóng vòng lặp dispatch để chỉ tập trung vào timing và dispatch.
	pregenBuf := make(chan Request, pregenBufferSize)
	go func() {
		defer close(pregenBuf)
		for {
			req, hasMore := gen.Next()
			if !hasMore {
				return
			}
			select {
			case pregenBuf <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	start := time.Now()

	// Tái sử dụng timer thay vì tạo time.NewTimer() mỗi iteration.
	// Mỗi time.NewTimer() allocate trên heap và tạo GC pressure.
	// Với timer.Reset(), chỉ cần 1 allocation duy nhất cho toàn bộ vòng đời.
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()

	for {
		// Tính deadline tuyệt đối: tránh drift tích luỹ
		// Nếu request thứ N bị chậm (ví dụ: expected 100µs, actual 130µs),
		// request N+1 vẫn được schedule tại thời điểm lý thuyết.
		nextDeadline := start.Add(time.Duration(issued+1) * rc.interval)
		waitDuration := time.Until(nextDeadline)

		if waitDuration > spinThreshold {
			// Dùng timer cho khoảng chờ dài (> spinThreshold)
			timer.Reset(waitDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				return issued
			case <-timer.C:
			}
		} else if waitDuration > 0 {
			// Busy-wait cho khoảng chờ ngắn (≤ spinThreshold)
			// Timer có thể không chính xác ở mức micro-giây,
			// busy-wait đảm bảo timing chính xác cho TPS cao.
			for time.Now().Before(nextDeadline) {
				runtime.Gosched()
			}
			// Kiểm tra ctx sau khi spin-wait
			select {
			case <-ctx.Done():
				return issued
			default:
			}
		} else {
			// Đã quá deadline (đang bị trễ), kiểm tra ctx trước khi tiếp tục
			select {
			case <-ctx.Done():
				return issued
			default:
			}
		}

		// Lấy request đã được sinh sẵn từ buffer (gần như instant, ~50ns channel read)
		// thay vì gọi gen.Next() trực tiếp (có thể mất hàng trăm ns đến µs)
		var req Request
		select {
		case r, ok := <-pregenBuf:
			if !ok {
				return issued // generator đã hết (LoadGenerator exhausted)
			}
			req = r
		case <-ctx.Done():
			return issued
		}

		// Thử đẩy request vào queue
		select {
		case requests <- req:
			issued++
		default:
			// Queue đầy → backpressure
			// Ghi nhận dropped, không block rate controller
			atomic.AddInt64(&rc.dropped, 1)
		}
	}
}

// Dropped trả về số request bị drop do queue đầy.
// Giá trị này phản ánh mức độ saturation của benchmark generator.
func (rc *RateController) Dropped() int64 {
	return atomic.LoadInt64(&rc.dropped)
}
