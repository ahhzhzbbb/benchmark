# BenchmarkDB - Công cụ đo lường hiệu năng Database

## 1. Giới thiệu
`BenchmarkDB` là một công cụ mã nguồn mở nhỏ gọn được viết bằng ngôn ngữ Go, chuyên dụng để đo lường hiệu năng (benchmark) của các hệ thống database. Hiện tại hỗ trợ **Redis Cluster**, với khả năng mở rộng cho **Aerospike** (Phase 2).

Công cụ sử dụng mô hình **open-loop benchmark**: tốc độ phát request được điều khiển bởi Rate Controller trung tâm, **độc lập với database latency**. Điều này đảm bảo kết quả benchmark phản ánh đúng khả năng của database, không bị ảnh hưởng bởi closed-loop bias.

## 2. Kiến trúc

```text
                  Config (YAML)
                     |
                     v
              Workload Generator
                     |
                     v
              Rate Controller
               Target TPS
                     |
                     v
               Request Queue
                     |
           +---------+---------+
           |         |         |
           v         v         v
        Worker 1  Worker 2  Worker N
           |         |         |
           +---------+---------+
                     |
                     v
              Database Adapter
               /           \
              /             \
       Redis Cluster      Aerospike (Phase 2)
              \             /
               \           /
                     |
                     v
                  Response
                     |
                     v
                  Metrics
                     |
                     v
                   Report
```

**Nguyên tắc kiến trúc quan trọng:**
> Rate Controller quyết định tốc độ phát request. Worker chỉ thực thi request được dispatch. Worker KHÔNG có ticker riêng.

## 3. Cấu trúc mã nguồn

| File | Mô tả |
|------|--------|
| `main.go` | Điểm khởi chạy: đọc config, tạo DB adapter, chạy Runner |
| `config.go` | Đọc cấu hình từ file YAML, định nghĩa tất cả config struct |
| `config.yaml` | File cấu hình mẫu |
| `database.go` | Interface `Database` trừu tượng + `Request`, `OperationType` |
| `redis.go` | Redis Cluster adapter implement `Database` |
| `workload.go` | Sinh request theo tỉ lệ cấu hình (GET/SET/DELETE/QUERY) hoặc nạp dữ liệu tuần tự |
| `rate_controller.go` | Điều khiển tốc độ phát request (absolute deadline, tránh drift) |
| `worker.go` | Worker pool nhận request từ queue và thực thi lên database |
| `metrics.go` | Thu thập, tổng hợp, tính toán và in kết quả benchmark |
| `runner.go` | Điều phối lifecycle và quản lý các mode chạy (Load / Benchmark) |
| `Dockerfile` | Đóng gói Docker (multi-stage build) |

## 4. Cách hoạt động chi tiết

BenchmarkDB được thiết kế để chạy theo **2 Giai đoạn (Phases)** riêng biệt:

### Giai đoạn 1: Nạp dữ liệu (Load Mode)
Sử dụng cờ `--mode load`. Trong giai đoạn này, công cụ sẽ nạp toàn bộ số lượng bản ghi (được cấu hình trong `data.key_count`) tuần tự vào database. Nó sẽ chỉ thực hiện lệnh SET cho đến khi đẩy đủ toàn bộ dữ liệu rồi tự dừng.
```bash
./benchmarkDB --config config.yaml --mode load
```

### Giai đoạn 2: Đo lường thực tế (Run Mode)
Sử dụng cờ `--mode run` (đây là mặc định). Giai đoạn này thực hiện test hiệu năng trên tập dữ liệu đã nạp ở Giai đoạn 1. Các lệnh (GET, SET, DELETE, QUERY) sẽ được sinh ngẫu nhiên theo tỉ lệ phần trăm bạn đã cấu hình trong mục `workload`.
Quy trình (Lifecycle) trong giai đoạn Run:
```text
Connect → Warmup → Reset Metrics → Measurement → Shutdown → Report
```
1. **Connect**: Kiểm tra kết nối database (Ping).
2. **Warmup**: Chạy làm ấm (warmup) connection pool dựa theo thời gian `warmup.duration`. Kết quả bị bỏ qua.
3. **Measurement**: Chạy test chính thức dựa theo `benchmark.duration`. Thu thập đầy đủ metrics.
4. **Shutdown**: Chờ cho in-flight requests hoàn thành trong `shutdown.grace_period`.
5. **Report**: In kết quả ra stdout.

### Tính năng cốt lõi khác:
- **Open-loop Rate Control**: Dùng **absolute deadline**, ngăn tình trạng trôi nhịp (drift) khi DB phản hồi chậm.
- **Backpressure**: Khi DB bị quá tải, Request Queue sẽ đầy. Số request bị vứt bỏ (dropped) sẽ được ghi nhận vào báo cáo.
- **Reproducibility**: Có Random Seed, đảm bảo có thể chạy lại một kịch bản với thứ tự request chính xác như cũ.

## 5. Hướng dẫn Xây dựng (Build)

### Build trực tiếp bằng Go
```bash
go mod download
go build -o benchmarkDB .
```

### Build bằng Docker
```bash
docker build -t benchmarkdb:latest .
```

## 6. Hướng dẫn Sử dụng (Usage)

Khuyến nghị chạy theo đúng 2 phase để kết quả chính xác nhất:

**Bước 1: Nạp dữ liệu vào Database:**
```bash
./benchmarkDB --config config.yaml --mode load
```

**Bước 2: Chạy Benchmark:**
```bash
# Mặc định là mode run
./benchmarkDB --config config.yaml
# Hoặc truyền --mode rõ ràng:
./benchmarkDB --config config.yaml --mode run
```

*(Bạn có thể thay đổi số lượng Target TPS trực tiếp mà không cần sửa file config bằng cờ `--tps`)*
```bash
./benchmarkDB --config config.yaml --tps 2000
```

## 7. Cấu hình (Configuration)

Xem file `config.yaml` mẫu. Các section chính:

| Section | Mô tả |
|---------|--------|
| `database.type` | Loại database: `redis` hoặc `aerospike` |
| `redis.addresses` | Danh sách node Redis Cluster |
| `data.key_count` | Số lượng key trong hệ thống (Giai đoạn Load sẽ chèn đủ chừng này key) |
| `data.payload_size` | Kích thước value cho lệnh SET (bytes) |
| `data.seed` | Random seed cho reproducibility |
| `load.target_tps` | Số request/giây mục tiêu |
| `load.max_concurrency` | Số lượng connection (worker) |
| `load.queue_size` | Sức chứa tối đa của hàng đợi request |
| `workload.get/set/delete/query` | Trọng số phần trăm cho từng loại thao tác ở mode `run` |
| `warmup.duration` | Thời gian warmup (ví dụ: "5s") |
| `benchmark.duration` | Thời gian benchmark chính thức (ví dụ: "10s") |
| `shutdown.grace_period` | Thời gian chờ request hoàn tất khi kết thúc |

## 8. Hiểu Kết quả Đầu ra (Output)

```text
============================================
          Benchmark Results
============================================

Database:         redis
Duration:         10.003s

Target TPS:       1000
Issued TPS:       999.70
Completed TPS:    985.30
Success TPS:      985.30
Error TPS:        0.00

Total Issued:     9997
Total Completed:  9853
Success:          9853
Errors:           0
Error Rate:       0.00%

Latency:
  Avg:            823µs
  P50:            712µs
  P95:            1.91ms
  P99:            4.27ms
  Max:            31.52ms

============================================
```

| Metric | Ý nghĩa |
|--------|---------|
| **Target TPS** | Tốc độ mục tiêu (cấu hình) |
| **Issued TPS** | Tốc độ thực tế công cụ phát request đi. Gần bằng Target = rate controller chạy tốt |
| **Completed TPS** | Tốc độ nhận response thành công từ DB |
| **Success/Error TPS** | Tốc độ request thành công / bị lỗi |
| **Dropped** | Số request bị bỏ đi do hàng đợi đẩy lên DB bị đầy (Dấu hiệu DB quá tải) |
| **P50/P95/P99** | Phân vị latency. Ví dụ: P99 là 4.27ms nghĩa là 99% request nhanh hơn 4.27ms |
| **Max** | Request bị kẹt lâu nhất |

## 9. Roadmap

- **Phase 1 (Hiện tại)**: Hỗ trợ GET / SET / DELETE / QUERY trên Redis Cluster. Cấu trúc Load vs Run Mode.
- **Phase 2**: Tích hợp Aerospike adapter + Cải thiện Querying.
- **Phase 3**: Xuất Prometheus metrics, HDR Histogram, Distributed benchmark (chạy trên nhiều máy đo đạc cùng lúc).
