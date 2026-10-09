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
	scheduled int64 // số request đã lên lịch phát (atomic counter)
	dropped   int64 // số request bị drop do queue đầy (atomic counter)
	issued    int64 // số request đã đẩy thành công vào queue (atomic counter)
	totalSlip int64 // tổng độ trễ lập lịch (nanoseconds) của các request đã issued
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
	// --- Pre-generate goroutine ---
	// Sinh request trước vào buffer, chạy song song với vòng lặp dispatch.
	// Chi phí gen.Next() (RNG, string formatting, v.v.) được chuyển sang goroutine này,
	// giải phóng vòng lặp dispatch để chỉ tập trung vào timing và dispatch.
	pregenBuf := make(chan Request, pregenBufferSize)
	pregenDone := make(chan struct{})
	go func() {
		defer close(pregenDone)
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
	// Chờ goroutine pre-generate thoát hẳn trước khi Run trả về: đảm bảo
	// generator (và KeySpace dùng chung) không còn bị truy cập khi phase tiếp theo bắt đầu.
	// Mọi nhánh return của Run đều xảy ra khi ctx đã Done hoặc pregenBuf đã đóng,
	// nên goroutine chắc chắn thoát.
	defer func() { <-pregenDone }()

	start := time.Now()

	// Tái sử dụng timer thay vì tạo time.NewTimer() mỗi iteration.
	// Mỗi time.NewTimer() allocate trên heap và tạo GC pressure.
	// Với timer.Reset(), chỉ cần 1 allocation duy nhất cho toàn bộ vòng đời.
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()

	var scheduled int64 // Số request ĐÃ LÊN LỊCH thành công hoặc thất bại

	for {
		// Tính deadline tuyệt đối: tránh drift tích luỹ
		// Dựa trên biến scheduled thay vì issued để luôn giữ đúng nhịp độ open-loop
		// dù queue có bị đầy (dropped) hay không.
		nextDeadline := start.Add(time.Duration(scheduled+1) * rc.interval)
		waitDuration := time.Until(nextDeadline)

		if waitDuration > spinThreshold {
			// Dùng timer cho khoảng chờ dài (> spinThreshold)
			timer.Reset(waitDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				return rc.Issued()
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
				return rc.Issued()
			default:
			}
		} else {
			// Đã quá deadline (đang bị trễ), kiểm tra ctx trước khi tiếp tục
			select {
			case <-ctx.Done():
				return rc.Issued()
			default:
			}
		}

		// Lấy request đã được sinh sẵn từ buffer (gần như instant, ~50ns channel read)
		// thay vì gọi gen.Next() trực tiếp (có thể mất hàng trăm ns đến µs)
		var req Request
		select {
		case r, ok := <-pregenBuf:
			if !ok {
				return rc.Issued() // generator đã hết (LoadGenerator exhausted)
			}
			req = r
		case <-ctx.Done():
			return rc.Issued()
		}

		scheduled++
		atomic.AddInt64(&rc.scheduled, 1)

		now := time.Now()
		slip := now.Sub(nextDeadline)
		if slip < 0 {
			slip = 0
		}

		// Thử đẩy request vào queue
		req.CreatedAt = time.Now()
		select {
		case requests <- req:
			atomic.AddInt64(&rc.issued, 1)
			atomic.AddInt64(&rc.totalSlip, int64(slip))
		default:
			// Queue đầy → backpressure
			// Ghi nhận dropped, không block rate controller
			atomic.AddInt64(&rc.dropped, 1)
		}
	}
}

func (rc *RateController) Scheduled() int64 {
	return atomic.LoadInt64(&rc.scheduled)
}

func (rc *RateController) Dropped() int64 {
	return atomic.LoadInt64(&rc.dropped)
}

// Issued trả về số request thực sự đã gửi vào queue thành công.
func (rc *RateController) Issued() int64 {
	return atomic.LoadInt64(&rc.issued)
}

// TotalSlip trả về tổng thời gian slip (nanoseconds).
func (rc *RateController) TotalSlip() int64 {
	return atomic.LoadInt64(&rc.totalSlip)
}
