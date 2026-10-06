package main

import (
	"context"
	"errors"
)

// ErrNotFound được adapter trả về khi GET/DELETE nhắm vào key không tồn tại.
// Worker ghi nhận riêng loại kết quả này (NotFound) thay vì tính là Success,
// để báo cáo phản ánh đúng số thao tác thực sự chạm vào dữ liệu.
var ErrNotFound = errors.New("key not found")

// Payload là dữ liệu ghi đã được chuẩn bị sẵn cho từng loại database.
// Payload được dựng NGOÀI vùng đo latency (trong generator), nên adapter
// chỉ cần gửi đi mà không phải serialize/parse — đảm bảo công bằng giữa các DB.
type Payload struct {
	JSON string                 // Dùng cho Redis (JSON.SET)
	Bins map[string]interface{} // Dùng cho Aerospike (Put BinMap)
}

// OperationType định nghĩa loại thao tác database mà benchmark hỗ trợ.
// Phiên bản MVP chỉ cần GET, SET, DELETE.
// Các loại query phức tạp (Equality, Range) sẽ được thêm ở Phase 2.
type OperationType int

const (
	OpGet          OperationType = iota // Đọc dữ liệu theo key
	OpSet                               // Ghi/cập nhật dữ liệu
	OpDelete                            // Xoá dữ liệu theo key
	OpQueryTag                          // Truy vấn dữ liệu theo tag
	OpQueryNumeric                      // Truy vấn dữ liệu theo dải số (numeric range)
	OpQueryOr                           // Truy vấn hoán vị OR (Union)
	OpQueryPrefix                       // Truy vấn tiền tố Prefix/Wildcard
)

// String trả về tên hiển thị của loại thao tác.
func (op OperationType) String() string {
	switch op {
	case OpGet:
		return "GET"
	case OpSet:
		return "SET"
	case OpDelete:
		return "DELETE"
	case OpQueryTag:
		return "QUERY_TAG"
	case OpQueryNumeric:
		return "QUERY_NUMERIC"
	case OpQueryOr:
		return "QUERY_OR"
	case OpQueryPrefix:
		return "QUERY_PREFIX"
	default:
		return "UNKNOWN"
	}
}

// Request chứa thông tin một thao tác benchmark cần thực thi.
// Request được tạo bởi WorkloadGenerator và đẩy vào queue bởi RateController.
// Worker nhận Request từ queue và gọi Database adapter tương ứng.
type Request struct {
	Operation OperationType
	Key       string
	Payload   *Payload // Chỉ dùng cho OpSet, nil cho OpGet/OpDelete/OpQuery...
	QueryStr  string   // Dùng cho OpQueryTag, OpQueryNumeric
}

// Database là interface trừu tượng cho mọi database backend.
// Benchmark engine chỉ tương tác qua interface này, không phụ thuộc
// trực tiếp vào Redis hay Aerospike. Mỗi database cần implement
// một adapter riêng (ví dụ: RedisDB, AerospikeDB).
//
// Thiết kế này cho phép chạy cùng bộ benchmark engine với
// nhiều database khác nhau, chỉ cần thay adapter.
type Database interface {
	// Get đọc giá trị của key. Trả về ErrNotFound nếu key không tồn tại.
	Get(ctx context.Context, key string) error

	// Set ghi payload (đã chuẩn bị sẵn) vào key.
	Set(ctx context.Context, key string, payload *Payload) error

	// Delete xoá key. Trả về ErrNotFound nếu key không tồn tại.
	Delete(ctx context.Context, key string) error

	// Close đóng kết nối tới database.
	Close() error

	// Ping kiểm tra kết nối tới database còn sống không.
	Ping(ctx context.Context) error

	// Init khởi tạo các setup cần thiết trên DB (như đánh index)
	Init(ctx context.Context) error

	// Query thực hiện tìm kiếm bằng truy vấn (cú pháp RediSearch), lấy tối đa
	// queryResultLimit kết quả. Kết quả phải bao phủ TOÀN BỘ dataset.
	Query(ctx context.Context, query string) error

	// Name trả về tên hiển thị của database (ví dụ: "Redis Cluster").
	Name() string
}

const (
	// queryResultLimit: số kết quả tối đa mỗi query (giống nhau cho mọi DB).
	queryResultLimit = 10
	// queryTimeoutMs: timeout phía server cho mỗi query (giống nhau cho mọi DB).
	queryTimeoutMs = 2000
)
