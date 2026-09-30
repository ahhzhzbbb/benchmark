# Task: Build a Simple Benchmark Data Generator

## 1. Mục tiêu

Hiện tại `benchmarkDB` đang sử dụng một JSON mẫu duy nhất rồi lặp lại để tạo nhiều records.

JSON mẫu hiện tại:

```json
{
  "Dnn": "Dnn_95",
  "IPType": "IPType_0",
  "State": "State_2",
  "Timer": 3,
  "SEID": "SEID_995",
  "startTime": 1704449815,
  "attachTime": 1707063660,
  "FTEIDC": "FTEIDC_865",
  "IP": "IP_158",
  "SNSSAI": "SNSSAI_2",
  "UpfSelection": 1709374722,
  "payload": "...",
  "TEIDC": "TEIDC_196"
}
```

Điều này làm dataset bị trùng quá nhiều.

Ví dụ hiện tại:

```text
State_0 = 0
State_1 = 0
State_2 = 97891

IPType_0 = 97891
IPType_1 = 0
```

Khi benchmark query:

```text
@State:{State_2}
```

thì gần như toàn bộ records đều match.

Cần xây dựng lại data generator để tạo ra dataset đa dạng hơn.

---

## 2. Yêu cầu chính

Không cần tạo dữ liệu 5G hoàn toàn realistic.

Chỉ cần:

* đúng JSON format;
* các field có nhiều giá trị khác nhau;
* có một số field unique;
* deterministic bằng seed;
* dễ cấu hình số lượng records;
* dùng được cho cả Redis và Aerospike.

Không cần quá quan tâm dữ liệu có thực sự tồn tại trong mạng 5G hay không.

Đây chỉ là benchmark dataset.

---

## 3. Số lượng record

Generator phải cho phép cấu hình:

```text
count
seed
payload size
```

Ví dụ:

```bash
benchmarkDB data generate --count 100000 --seed 12345
```

Mặc định có thể là:

```text
count = 100000
seed = 12345
```

---

## 4. Generate các field

Mỗi record vẫn phải có các field:

```text
Dnn
IPType
State
Timer
SEID
startTime
attachTime
FTEIDC
IP
SNSSAI
UpfSelection
payload
TEIDC
```

### Dnn

Tạo nhiều giá trị:

```text
Dnn_0
Dnn_1
...
Dnn_99
```

Có thể random:

```go
Dnn = fmt.Sprintf("Dnn_%d", rng.Intn(100))
```

### IPType

Chỉ cần:

```text
IPType_0
IPType_1
```

Random giữa hai giá trị.

### State

Tạo:

```text
State_0
State_1
State_2
```

Random giữa ba giá trị.

### SNSSAI

Tạo:

```text
SNSSAI_0
SNSSAI_1
SNSSAI_2
```

Random giữa ba giá trị.

Mục tiêu là tất cả các giá trị đều xuất hiện trong dataset.

### FTEIDC

Có thể:

```text
FTEIDC_0 ... FTEIDC_999
```

### TEIDC

Có thể:

```text
TEIDC_0 ... TEIDC_999
```

### IP

Có thể đơn giản:

```text
IP_0 ... IP_999
```

Không cần generate IPv4 thật.

### SEID

Nên unique theo record:

```text
SEID_0
SEID_1
SEID_2
...
```

### Timer

Random integer, ví dụ:

```text
0 - 100
```

### startTime / attachTime

Có thể random integer trong một range hợp lý.

Không cần mô phỏng timestamp thật quá chính xác.

### UpfSelection

Có thể random integer.

### payload

Payload có kích thước configurable.

Ví dụ:

```text
payload_size = 64
```

Không cần random nội dung phức tạp. Có thể generate deterministic string dựa trên seed.

---

## 5. Redis key

Redis key phải unique:

```text
bench:key:0
bench:key:1
bench:key:2
...
bench:key:99999
```

Không được vô tình overwrite record cũ.

---

## 6. Random và seed

Phải sử dụng seeded random generator.

Ví dụ:

```text
seed = 12345
```

Nếu chạy generator hai lần với cùng:

```text
count = 100000
seed = 12345
```

thì kết quả phải giống nhau.

Điều này giúp benchmark có thể reproduce.

---

## 7. Output

Có thể giữ nguyên cách output hiện tại của project nếu đã có data loading pipeline.

Nếu cần file trung gian, ưu tiên JSONL:

```text
dataset.jsonl
```

Mỗi dòng là một JSON object:

```json
{"Dnn":"Dnn_10","IPType":"IPType_1","State":"State_0",...}
{"Dnn":"Dnn_52","IPType":"IPType_0","State":"State_2",...}
{"Dnn":"Dnn_3","IPType":"IPType_1","State":"State_1",...}
```

Không cần tạo một JSON array khổng lồ.

---

## 8. Data validation đơn giản

Sau khi generate, in ra một số thông tin:

```text
Generated: 100000 records

State:
  State_0: xxxx
  State_1: xxxx
  State_2: xxxx

IPType:
  IPType_0: xxxx
  IPType_1: xxxx

SNSSAI:
  SNSSAI_0: xxxx
  SNSSAI_1: xxxx
  SNSSAI_2: xxxx

Unique SEID: 100000
```

Không cần distribution chính xác tuyệt đối.

Chỉ cần đảm bảo không xảy ra tình trạng:

```text
State_0 = 0
State_1 = 0
State_2 = 100%
```

hoặc tương tự.

---

## 9. Redis / Aerospike

Redis và Aerospike phải sử dụng **cùng một logical dataset**.

Không tạo dataset riêng cho từng database.

Ví dụ:

```text
                    Data Generator
                          │
                          ▼
                    100k records
                     /         \
                    /           \
                   ▼             ▼
                Redis         Aerospike
```

Mục tiêu là benchmark hai database trên cùng dữ liệu.

---

## 10. Không cần làm

Không cần:

* mô phỏng dữ liệu 5G thực tế;
* distribution production;
* hệ thống configuration phức tạp;
* distributed generator;
* AI generate từng record;
* kiểm tra dữ liệu có thực sự hợp lệ trong 5G;
* các validation phức tạp;
* optimization quá mức.

Đây chỉ là benchmark dataset.

Ưu tiên code đơn giản và dễ đọc.

---

## 11. Acceptance Criteria

Implementation được coi là hoàn thành khi:

1. Có thể generate 100,000 records.

2. Các field categorical có nhiều giá trị khác nhau.

3. `State_0`, `State_1`, `State_2` đều xuất hiện.

4. `IPType_0`, `IPType_1` đều xuất hiện.

5. `SNSSAI_0`, `SNSSAI_1`, `SNSSAI_2` đều xuất hiện.

6. `SEID` unique.

7. Redis key unique.

8. Payload size configurable.

9. Cùng seed tạo ra cùng dataset.

10. Dataset có thể load vào cả Redis và Aerospike.

Sau khi hoàn thành, test thử:

```text
FT.SEARCH idx '@State:{State_0}' LIMIT 0 0
FT.SEARCH idx '@State:{State_1}' LIMIT 0 0
FT.SEARCH idx '@State:{State_2}' LIMIT 0 0
```

Cả ba query phải trả về một số lượng record đáng kể, thay vì hai query trả về `0`.

## 12. Important

JSON hiện tại chỉ là **một record mẫu**.

Không được tiếp tục làm:

```text
sample JSON
     ↓
copy 100000 times
```

Mà phải làm:

```text
sample JSON
     ↓
define field format
     ↓
generate different values
     ↓
100000 different records
```

Hãy ưu tiên implementation đơn giản, dễ maintain và phù hợp với code hiện tại của `benchmarkDB`.
