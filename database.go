package main

import "context"

// OperationType định nghĩa loại thao tác database mà benchmark hỗ trợ.
// Phiên bản MVP chỉ cần GET, SET, DELETE.
// Các loại query phức tạp (Equality, Range) sẽ được thêm ở Phase 2.
type OperationType int

const (
	OpGet    OperationType = iota // Đọc dữ liệu theo key
	OpSet                         // Ghi/cập nhật dữ liệu
	OpDelete                      // Xoá dữ liệu theo key
	OpQuery                       // Truy vấn dữ liệu
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
	case OpQuery:
		return "QUERY"
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
	Value     []byte // Chỉ dùng cho OpSet, nil cho OpGet/OpDelete/OpQuery
	QueryStr  string // Dùng cho OpQuery
}

// Database là interface trừu tượng cho mọi database backend.
// Benchmark engine chỉ tương tác qua interface này, không phụ thuộc
// trực tiếp vào Redis hay Aerospike. Mỗi database cần implement
// một adapter riêng (ví dụ: RedisDB, AerospikeDB).
//
// Thiết kế này cho phép chạy cùng bộ benchmark engine với
// nhiều database khác nhau, chỉ cần thay adapter.
type Database interface {
	// Get đọc giá trị của key. Trả về nil nếu key không tồn tại
	// (key-not-found không phải lỗi trong ngữ cảnh benchmark).
	Get(ctx context.Context, key string) error

	// Set ghi giá trị value vào key.
	Set(ctx context.Context, key string, value []byte) error

	// Delete xoá key. Không trả lỗi nếu key không tồn tại.
	Delete(ctx context.Context, key string) error

	// Close đóng kết nối tới database.
	Close() error

	// Ping kiểm tra kết nối tới database còn sống không.
	Ping(ctx context.Context) error

	// Init khởi tạo các setup cần thiết trên DB (như đánh index)
	Init(ctx context.Context) error

	// Query thực hiện tìm kiếm bằng truy vấn (ví dụ RediSearch)
	Query(ctx context.Context, query string) error

	// Name trả về tên hiển thị của database (ví dụ: "Redis Cluster").
	Name() string
}
