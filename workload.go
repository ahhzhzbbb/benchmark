package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
)

// Generator định nghĩa interface chung cho bộ sinh dữ liệu.
type Generator interface {
	Next() (Request, bool)
}

// payloadPoolSize là số lượng payload đa dạng được sinh trước
// cho WorkloadGenerator trong benchmark mode (SET operations).
const payloadPoolSize = 10000

// WorkloadGenerator sinh ra các Request theo tỉ lệ cấu hình (GET/SET/DELETE/QUERY).
// Sử dụng deterministic seed để đảm bảo reproducibility:
// cùng seed + cùng cấu hình → cùng chuỗi request, bất kể chạy bao nhiêu lần.
//
// KHÔNG thread-safe. Chỉ được gọi từ một goroutine duy nhất
// (trong thiết kế này là RateController).
type WorkloadGenerator struct {
	rng        *rand.Rand
	keyCount   int
	payloads   [][]byte  // pre-generated diverse payloads pool cho SET
	keys       []string  // pre-computed keys: tránh fmt.Sprintf trong hot path
	queries    []string  // pre-defined query patterns: tránh slice alloc mỗi Next()
	getRatio   float64   // tỉ lệ tích luỹ: [0, getRatio) → GET
	setRatio   float64   // tỉ lệ tích luỹ: [getRatio, setRatio) → SET
	delRatio   float64   // tỉ lệ tích luỹ: [setRatio, delRatio) → DELETE
	queryRatio float64   // tỉ lệ tích luỹ: [delRatio, 1.0) → QUERY
}

// Record đại diện cho cấu trúc dữ liệu theo DATASET.md
type Record struct {
	Dnn          string `json:"Dnn"`
	IPType       string `json:"IPType"`
	State        string `json:"State"`
	Timer        int    `json:"Timer"`
	SEID         string `json:"SEID"`
	StartTime    int64  `json:"startTime"`
	AttachTime   int64  `json:"attachTime"`
	FTEIDC       string `json:"FTEIDC"`
	IP           string `json:"IP"`
	SNSSAI       string `json:"SNSSAI"`
	UpfSelection int64  `json:"UpfSelection"`
	Payload      string `json:"payload"`
	TEIDC        string `json:"TEIDC"`
}

// generateRecord tạo một record đa dạng sử dụng seeded RNG.
// Mỗi lần gọi tiêu thụ một số lượng cố định random numbers từ rng,
// đảm bảo tính deterministic. index được dùng để tạo SEID unique.
func generateRecord(rng *rand.Rand, index int, payloadSize int) Record {
	payloadBytes := make([]byte, payloadSize)
	for i := range payloadBytes {
		payloadBytes[i] = 'a'
	}

	startTime := int64(1700000000) + rng.Int63n(10000000)

	return Record{
		Dnn:          "Dnn_" + strconv.Itoa(rng.Intn(100)),
		IPType:       "IPType_" + strconv.Itoa(rng.Intn(2)),
		State:        "State_" + strconv.Itoa(rng.Intn(3)),
		Timer:        rng.Intn(101),
		SEID:         "SEID_" + strconv.Itoa(index),
		StartTime:    startTime,
		AttachTime:   startTime + rng.Int63n(2600000),
		FTEIDC:       "FTEIDC_" + strconv.Itoa(rng.Intn(1000)),
		IP:           "IP_" + strconv.Itoa(rng.Intn(1000)),
		SNSSAI:       "SNSSAI_" + strconv.Itoa(rng.Intn(3)),
		UpfSelection: rng.Int63n(2000000000),
		Payload:      string(payloadBytes),
		TEIDC:        "TEIDC_" + strconv.Itoa(rng.Intn(1000)),
	}
}

// NewWorkloadGenerator khởi tạo generator từ config.
// Pre-generate pool of diverse payloads cho SET operations
// để giảm allocation và GC overhead trong khi benchmark đang chạy.
func NewWorkloadGenerator(cfg Config) *WorkloadGenerator {
	rng := rand.New(rand.NewSource(cfg.Data.Seed))

	total := float64(cfg.Workload.Get + cfg.Workload.Set + cfg.Workload.Delete + cfg.Workload.Query)
	if total == 0 {
		total = 1 // tránh chia cho 0
	}
	getRatio := float64(cfg.Workload.Get) / total
	setRatio := getRatio + float64(cfg.Workload.Set)/total
	delRatio := setRatio + float64(cfg.Workload.Delete)/total
	queryRatio := 1.0

	// Pre-generate pool of diverse payloads cho SET operations.
	// Sử dụng RNG riêng (seed+1) để không ảnh hưởng chuỗi random của main RNG.
	poolRng := rand.New(rand.NewSource(cfg.Data.Seed + 1))
	poolSize := payloadPoolSize
	if cfg.Data.KeyCount < poolSize {
		poolSize = cfg.Data.KeyCount
	}
	payloads := make([][]byte, poolSize)
	for i := 0; i < poolSize; i++ {
		record := generateRecord(poolRng, i, cfg.Data.PayloadSize)
		payloadBytes, _ := json.Marshal(record)
		payloads[i] = payloadBytes
	}

	// Pre-compute tất cả key strings để loại bỏ fmt.Sprintf trong hot path.
	// Mỗi fmt.Sprintf allocate string mới trên heap (~200-500ns).
	// Với pre-computed keys, Next() chỉ cần array index lookup (~1ns, zero-alloc).
	keys := make([]string, cfg.Data.KeyCount)
	for i := 0; i < cfg.Data.KeyCount; i++ {
		keys[i] = "bench:key:" + strconv.Itoa(i)
	}

	// High-Selectivity Query Pool
	//
	// Các query được thiết kế để trả về ít kết quả (high selectivity),
	// mô phỏng thực tế tra cứu session 5G SMF theo điều kiện cụ thể.
	//
	// Phân tích selectivity trên 100K records:
	//   - Single high-card field (FTEIDC=1000): ~100 records (0.1%)
	//   - Compound 2 fields (FTEIDC + State):   ~33 records (0.03%)
	//   - Compound 3 fields:                    ~5-17 records (<0.02%)
	//   - Exact SEID lookup:                    1 record (0.001%)
	//   - Narrow numeric range + TAG:           ~1-10 records (<0.01%)
	//
	// Pool size lớn (200 queries) đảm bảo benchmark không bị bias
	// bởi cache hit trên cùng một tập kết quả nhỏ.
	queryPoolSize := 200
	queryRng := rand.New(rand.NewSource(cfg.Data.Seed + 2))
	queries := make([]string, 0, queryPoolSize)

	for i := 0; i < queryPoolSize; i++ {
		pattern := queryRng.Intn(8)
		switch pattern {
		case 0:
			// FTEIDC exact (1000 values) + State (3 values) → ~33 records / 100K = 0.03%
			queries = append(queries, "@FTEIDC:{FTEIDC_"+strconv.Itoa(queryRng.Intn(1000))+
				"} @State:{State_"+strconv.Itoa(queryRng.Intn(3))+"}")
		case 1:
			// IP exact (1000 values) + SNSSAI (3 values) → ~33 records = 0.03%
			queries = append(queries, "@IP:{IP_"+strconv.Itoa(queryRng.Intn(1000))+
				"} @SNSSAI:{SNSSAI_"+strconv.Itoa(queryRng.Intn(3))+"}")
		case 2:
			// TEIDC exact (1000 values) + IPType (2 values) → ~50 records = 0.05%
			queries = append(queries, "@TEIDC:{TEIDC_"+strconv.Itoa(queryRng.Intn(1000))+
				"} @IPType:{IPType_"+strconv.Itoa(queryRng.Intn(2))+"}")
		case 3:
			// Dnn (100 values) + State + IPType → ~167 records = 0.17%
			queries = append(queries, "@Dnn:{Dnn_"+strconv.Itoa(queryRng.Intn(100))+
				"} @State:{State_"+strconv.Itoa(queryRng.Intn(3))+
				"} @IPType:{IPType_"+strconv.Itoa(queryRng.Intn(2))+"}")
		case 4:
			// FTEIDC + Dnn compound → ~100/1000 * 1/100 = 0.001% ≈ 1 record
			queries = append(queries, "@FTEIDC:{FTEIDC_"+strconv.Itoa(queryRng.Intn(1000))+
				"} @Dnn:{Dnn_"+strconv.Itoa(queryRng.Intn(100))+"}")
		case 5:
			// Narrow Timer range (width=3) + Dnn → ~3/101 * 1/100 ≈ 3 records
			timerStart := queryRng.Intn(98)
			queries = append(queries, "@Timer:["+strconv.Itoa(timerStart)+" "+strconv.Itoa(timerStart+3)+
				"] @Dnn:{Dnn_"+strconv.Itoa(queryRng.Intn(100))+"}")
		case 6:
			// IP + FTEIDC compound → 1/1000 * 1/1000 ≈ 0.1 records (near-miss query)
			queries = append(queries, "@IP:{IP_"+strconv.Itoa(queryRng.Intn(1000))+
				"} @FTEIDC:{FTEIDC_"+strconv.Itoa(queryRng.Intn(1000))+"}")
		case 7:
			// SEID exact match → 1 record (point lookup via index)
			queries = append(queries, "@SEID:{SEID_"+strconv.Itoa(queryRng.Intn(cfg.Data.KeyCount))+"}")
		}
	}

	return &WorkloadGenerator{
		rng:        rng,
		keyCount:   cfg.Data.KeyCount,
		payloads:   payloads,
		keys:       keys,
		queries:    queries,
		getRatio:   getRatio,
		setRatio:   setRatio,
		delRatio:   delRatio,
		queryRatio: queryRatio,
	}
}

// Next sinh ra request tiếp theo dựa trên tỉ lệ workload đã cấu hình.
func (w *WorkloadGenerator) Next() (Request, bool) {
	r := w.rng.Float64()
	key := w.keys[w.rng.Intn(w.keyCount)] // zero-alloc: pre-computed key lookup

	var op OperationType
	var value []byte
	var queryStr string

	switch {
	case r < w.getRatio:
		op = OpGet
	case r < w.setRatio:
		op = OpSet
		value = w.payloads[w.rng.Intn(len(w.payloads))] // diverse payload từ pool
	case r < w.delRatio:
		op = OpDelete
	default:
		op = OpQuery
		queryStr = w.queries[w.rng.Intn(len(w.queries))]
	}

	return Request{
		Operation: op,
		Key:       key,
		Value:     value,
		QueryStr:  queryStr,
	}, true
}

// LoadGenerator được sử dụng cho phase nạp dữ liệu (Load Mode).
// Sinh tuần tự các record đa dạng với SET request từ key 0 đến KeyCount - 1,
// mỗi record có giá trị ngẫu nhiên riêng, sau đó trả về false.
type LoadGenerator struct {
	rng         *rand.Rand
	current     int
	keyCount    int
	payloadSize int
}

func NewLoadGenerator(cfg Config) *LoadGenerator {
	return &LoadGenerator{
		rng:         rand.New(rand.NewSource(cfg.Data.Seed)),
		current:     0,
		keyCount:    cfg.Data.KeyCount,
		payloadSize: cfg.Data.PayloadSize,
	}
}

func (l *LoadGenerator) Next() (Request, bool) {
	if l.current >= l.keyCount {
		return Request{}, false // Đã sinh đủ số lượng key
	}

	key := "bench:key:" + strconv.Itoa(l.current)
	record := generateRecord(l.rng, l.current, l.payloadSize)
	value, _ := json.Marshal(record)

	req := Request{
		Operation: OpSet,
		Key:       key,
		Value:     value,
	}
	l.current++
	return req, true
}

// Current trả về số key đã sinh, dùng cho progress reporting.
func (l *LoadGenerator) Current() int {
	return l.current
}

// PrintDataStats tái tạo dataset với cùng seed và in thống kê phân bổ
// để kiểm tra tính đa dạng của dữ liệu đã sinh.
func PrintDataStats(cfg Config) {
	rng := rand.New(rand.NewSource(cfg.Data.Seed))

	stateCount := make(map[string]int)
	ipTypeCount := make(map[string]int)
	snssaiCount := make(map[string]int)

	for i := 0; i < cfg.Data.KeyCount; i++ {
		rec := generateRecord(rng, i, 0) // payloadSize=0: không ảnh hưởng RNG, tiết kiệm alloc
		stateCount[rec.State]++
		ipTypeCount[rec.IPType]++
		snssaiCount[rec.SNSSAI]++
	}

	fmt.Println("\n--- Data Validation ---")
	fmt.Printf("Generated: %d records\n", cfg.Data.KeyCount)

	fmt.Println("\nState:")
	printSortedMap(stateCount)

	fmt.Println("\nIPType:")
	printSortedMap(ipTypeCount)

	fmt.Println("\nSNSSAI:")
	printSortedMap(snssaiCount)

	fmt.Printf("\nUnique SEID: %d\n", cfg.Data.KeyCount)
	fmt.Println("--- End Validation ---")
}

// printSortedMap in map theo thứ tự key alphabet.
func printSortedMap(m map[string]int) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %s: %d\n", k, m[k])
	}
}
