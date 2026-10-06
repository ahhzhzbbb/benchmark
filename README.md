# BenchmarkDB - Công cụ đo lường hiệu năng Database (Open-Loop)

## 1. Giới thiệu
`BenchmarkDB` là một công cụ mã nguồn mở được viết bằng ngôn ngữ Go, chuyên dụng để đo lường hiệu năng (benchmark) của các hệ thống cơ sở dữ liệu. Điểm đặc biệt của công cụ này là sử dụng mô hình **open-loop benchmark**: tốc độ phát request được điều khiển bởi Rate Controller trung tâm, **độc lập với database latency**. Điều này đảm bảo kết quả benchmark phản ánh đúng khả năng chịu tải thực tế của database, không bị ảnh hưởng bởi closed-loop bias (tình trạng công cụ test tự động chạy chậm lại khi DB phản hồi chậm).

Hiện tại, công cụ hỗ trợ:
- **Redis Cluster** (Hỗ trợ RedisJSON và RediSearch cho truy vấn phức tạp).
- **Aerospike** (Hỗ trợ Secondary Indexes cho truy vấn).

Mô hình dữ liệu giả lập được thiết kế tối ưu để mô phỏng dữ liệu mạng lõi viễn thông (5G SMF Session).

## 2. Kiến trúc & Tối ưu hoá

```text
                  Config (YAML / CLI Args)
                             |
                             v
                    Workload Generator 
           (Pre-generated diverse payload pool)
                             |
                             v
                      Rate Controller
              (Absolute deadline timing, spin-wait)
                        Target TPS
                             |
                             v
                       Request Queue
                   (Backpressure handling)
                             |
            +----------------+----------------+
            |                |                |
            v                v                v
         Worker 1         Worker 2         Worker N
            |                |                |
            +----------------+----------------+
                             |
                             v
                      Database Adapter
                       /           \
                      /             \
               Redis Cluster      Aerospike
                     \             /
                      \           /
                            |
                            v
                    Metrics Aggregator
                            |
                            v
                      Console Report
```

**Các kỹ thuật tối ưu hoá (Optimizations) nổi bật:**
1. **Zero-allocation trong vòng lặp chính**: Generator sinh sẵn chuỗi keys và đa dạng payload (pool) trước khi chạy benchmark, loại bỏ hoàn toàn `fmt.Sprintf` và `json.Marshal` ở đường dẫn thực thi (hot path).
2. **Absolute Deadline Timer & Busy-wait**: Rate Controller dùng deadline tuyệt đối để ngăn trôi nhịp (drift) và dùng vòng lặp `runtime.Gosched()` (spin-wait) cho các khoảng nghỉ dưới 100µs nhằm đạt độ chính xác ở cấp độ micro-giây.
3. **Lock-free worker**: Mỗi Worker có một bộ đếm metric cục bộ (`WorkerResult`), không chia sẻ trạng thái, không cần dùng Mutex, tối đa hoá throughput.
4. **Cân bằng tải Redis Master**: Round-robin thông qua bộ đếm atomic lock-free cho các truy vấn RediSearch (`FT.SEARCH`) tránh nghẽn.
5. **NOCONTENT RediSearch**: Giảm thiểu chi phí truyền dẫn I/O mạng khi thực hiện truy vấn.

## 3. Cấu trúc mã nguồn

| File | Mô tả |
|------|--------|
| `main.go` | Điểm khởi chạy: parse config, tạo DB adapter, quản lý tín hiệu thoát và chạy Runner |
| `config.go` | Xử lý cấu hình từ file YAML và CLI Flags |
| `database.go` | Định nghĩa interface `Database` trừu tượng và các loại `OperationType` |
| `redis.go` | Redis Cluster adapter (RedisJSON, RediSearch) |
| `aerospike.go` | Aerospike adapter (Secondary Indexes, Filters) |
| `workload.go` | Sinh request ngẫu nhiên theo tỉ lệ phân bổ (GET/SET/DELETE/QUERY) bằng pre-generated pool |
| `rate_controller.go` | Scheduler chính điều khiển tốc độ (Open-loop) |
| `worker.go` | Goroutine pool nhận job và giao tiếp với database, đo đạc latency |
| `metrics.go` | Tổng hợp kết quả, tính toán các phân vị (Percentiles) và in báo cáo |
| `runner.go` | Điều phối Lifecycle (Connect → Warmup → Measurement → Graceful Shutdown) |

## 4. Cách hoạt động chi tiết

BenchmarkDB chạy theo **2 Giai đoạn (Phases)** thiết kế rõ ràng:

### Giai đoạn 1: Nạp dữ liệu (Load Mode)
Lấp đầy cơ sở dữ liệu bằng các bản ghi giả lập trước khi đo đạc. Không áp dụng Rate Limit ở chế độ này, các worker sẽ "đẩy" (SET) dữ liệu nhanh nhất có thể cho tới khi đạt `data.key_count`.
```bash
./benchmarkDB --config config.yaml --mode load
```

### Giai đoạn 2: Đo lường thực tế (Run Mode)
Giai đoạn này thực hiện tải với Rate Controller dựa trên tập dữ liệu đã nạp. Các lệnh (GET, SET, DELETE, QUERY_TAG, QUERY_NUMERIC) sinh ngẫu nhiên theo cấu hình `workload`.
Quy trình (Lifecycle):
1. **Connect & Init**: Ping database, thiết lập Index nếu cần (RediSearch/Aerospike SI).
2. **Warmup**: Chạy làm nóng connection pool (không ghi nhận kết quả).
3. **Measurement**: Chạy chính thức thu thập toàn bộ latency, errors, success.
4. **Shutdown**: Grace period chờ các request đang in-flight (trong queue) xử lý xong.
5. **Report**: Tổng hợp và in báo cáo.

## 5. Hướng dẫn Xây dựng (Build)

**Sử dụng Go (Cần Go 1.21+):**
```bash
go mod download
go build -o benchmarkDB .
```

**Sử dụng Docker:**
```bash
docker build -t benchmarkdb:latest .
```

## 6. Hướng dẫn Sử dụng (Usage)

**Bước 1: Nạp dữ liệu vào Database**
```bash
./benchmarkDB --config config.yaml --mode load
```

**Bước 2: Chạy Benchmark**
```bash
# Mặc định mode là "run"
./benchmarkDB --config config.yaml

# Bạn có thể ghi đè (override) cấu hình trực tiếp từ CLI flags:
./benchmarkDB --config config.yaml --tps 50000 --concurrency 200 --get 40 --set 20 --delete 10 --query-tag 10 --query-numeric 5 --query-or 10 --query-prefix 5
```

## 7. Cấu hình (Configuration)

File cấu hình mặc định là `config.yaml`. Một số mục quan trọng:

| Section | Tham số | Ý nghĩa |
|---------|----------|---------|
| `database`| `type` | Chọn `redis` hoặc `aerospike` |
| `redis` / `aerospike` | `addresses` / `hosts` | Danh sách node kết nối của DB |
| `data` | `key_count` | Số lượng records tổng cộng để test |
| `data` | `payload_size` | Dung lượng byte của mỗi bản ghi sinh ra |
| `load` | `target_tps` | Số Request phát ra mỗi giây (mục tiêu) |
| `load` | `max_concurrency` | Số lượng Worker / connection mở đồng thời |
| `workload`| `get`, `set`... | Trọng số tỉ lệ % xuất hiện của loại request đó |
| `warmup` | `duration` | Thời gian chạy rà trơn (VD: "5s", "1m") |
| `benchmark` | `duration` | Thời gian ghi nhận số liệu (VD: "10s", "2m") |

## 8. Hiểu Kết quả Đầu ra (Output)

```text
============================================
          Benchmark Results
============================================

Database:         redis
Measurement:      10.003s
Total Duration:   10.045s

Target TPS:       10000
Issued TPS:       9998.50  <-- Tốc độ thực tế phát request
Completed TPS:    9853.30
Success TPS:      9853.30
Error TPS:        0.00

Total Issued:     99985
Total Completed:  98533
Success:          98533
Errors:           0
Dropped (queue):  1400     <-- Số request bị vứt đi do hàng đợi đầy
Abandoned:        52       <-- Request còn kẹt chưa xử lý xong sau Grace Period

Latency:
  Avg:            1.23ms
  P50:            812µs
  P95:            3.91ms
  P99:            9.27ms
  Max:            45.52ms
```

- **Target TPS vs Issued TPS**: Nếu máy benchmark theo kịp, 2 con số này phải gần bằng nhau. 
- **Latency (P50, P95, P99)**: Phản ánh thời gian thực tế để gọi DB (không tính thời gian nằm trong queue của công cụ test). P99 = 9.27ms nghĩa là 99% request có thời gian xử lý dưới mức này.
- **Dropped**: Lượng request chủ động bị drop vì hàng đợi đã đầy. Đây là dấu hiệu cơ sở dữ liệu đã bị quá tải (saturated).

## 9. Roadmap

- **Phase 1**: Hỗ trợ lệnh GET / SET / DELETE / QUERY trên Redis Cluster.
- **Phase 2**: Hoàn thiện Aerospike adapter (Secondary Indexes/Filter expressions).
- **Phase 3**: Đẩy dữ liệu ra Prometheus metrics, HDR Histogram, và hỗ trợ Distributed benchmark (clustering nhiều máy đẩy tải).
