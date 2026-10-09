package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync/atomic"
)

// Generator định nghĩa interface chung cho bộ sinh dữ liệu.
type Generator interface {
	Next() (Request, bool)
}

// warmupSeedOffset được cộng vào seed khi tạo generator cho warmup phase,
// để warmup và measurement chạy hai chuỗi request KHÁC nhau
// (tránh measurement được lợi từ cache đã được warmup làm nóng đúng các key đó).
const warmupSeedOffset = 1_000_003

// ---------------------------------------------------------------------------
// KeySpace
// ---------------------------------------------------------------------------

// KeySpace theo dõi (theo góc nhìn của generator) key nào đang tồn tại trong DB.
//
// Mục đích: giữ dataset ổn định khi workload có DELETE.
//   - GET / DELETE chỉ chọn key đang tồn tại (live).
//   - DELETE chuyển key sang trạng thái đã xoá (dead).
//   - SET ưu tiên tái tạo (re-insert) một key dead; nếu không có thì update key live.
//
// Nhờ đó, khi tỉ lệ SET >= DELETE, số lượng record trong DB gần như không đổi
// trong suốt benchmark. Thao tác chọn/xoá/tái tạo đều O(1) (swap-remove).
//
// KHÔNG thread-safe: chỉ được dùng bởi một goroutine tại một thời điểm
// (goroutine pre-generate của RateController). KeySpace được chia sẻ giữa
// warmup và measurement để trạng thái live/dead được giữ liên tục.
type KeySpace struct {
	keys  []string // key string đã tính sẵn, index = record id
	live  []int32  // danh sách id đang tồn tại
	dead  []int32  // danh sách id đã bị DELETE
	pos   []int32  // vị trí của id trong live hoặc dead (tuỳ alive[id])
	alive []bool
}

// NewKeySpace tạo KeySpace với n key, giả định tất cả đã được nạp (mode load).
func NewKeySpace(n int) *KeySpace {
	ks := &KeySpace{
		keys:  make([]string, n),
		live:  make([]int32, n),
		dead:  make([]int32, 0, 1024),
		pos:   make([]int32, n),
		alive: make([]bool, n),
	}
	for i := 0; i < n; i++ {
		ks.keys[i] = "bench:key:" + strconv.Itoa(i)
		ks.live[i] = int32(i)
		ks.pos[i] = int32(i)
		ks.alive[i] = true
	}
	return ks
}

// Len trả về tổng số key (live + dead).
func (ks *KeySpace) Len() int { return len(ks.keys) }

// LiveCount trả về số key đang tồn tại.
func (ks *KeySpace) LiveCount() int { return len(ks.live) }

// Key trả về key string của id.
func (ks *KeySpace) Key(id int32) string { return ks.keys[id] }

// swapRemove xoá id khỏi list bằng cách đổi chỗ với phần tử cuối (O(1)).
func swapRemove(list []int32, pos []int32, id int32) []int32 {
	i := pos[id]
	last := list[len(list)-1]
	list[i] = last
	pos[last] = i
	return list[:len(list)-1]
}

// randomLive chọn ngẫu nhiên một key đang tồn tại.
// ok=false nếu không còn key live (khi đó trả về một id bất kỳ).
func (ks *KeySpace) randomLive(rng *rand.Rand) (id int32, ok bool) {
	if len(ks.live) == 0 {
		return int32(rng.Intn(len(ks.keys))), false
	}
	return ks.live[rng.Intn(len(ks.live))], true
}

// markDeleted chuyển id từ live sang dead.
func (ks *KeySpace) markDeleted(id int32) {
	if !ks.alive[id] {
		return
	}
	ks.live = swapRemove(ks.live, ks.pos, id)
	ks.pos[id] = int32(len(ks.dead))
	ks.dead = append(ks.dead, id)
	ks.alive[id] = false
}

// markLive chuyển id từ dead sang live.
func (ks *KeySpace) markLive(id int32) {
	if ks.alive[id] {
		return
	}
	ks.dead = swapRemove(ks.dead, ks.pos, id)
	ks.pos[id] = int32(len(ks.live))
	ks.live = append(ks.live, id)
	ks.alive[id] = true
}

// pickForSet chọn key cho SET: ưu tiên tái tạo một key đã bị xoá,
// nếu không có thì chọn ngẫu nhiên một key đang tồn tại (update).
func (ks *KeySpace) pickForSet(rng *rand.Rand) int32 {
	if len(ks.dead) > 0 {
		id := ks.dead[rng.Intn(len(ks.dead))]
		ks.markLive(id)
		return id
	}
	id, _ := ks.randomLive(rng)
	return id
}

// ---------------------------------------------------------------------------
// Record
// ---------------------------------------------------------------------------

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
// đảm bảo tính deterministic. index là record id: SEID = "SEID_<index>",
// tương ứng với key "bench:key:<index>".
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

// recordToBins chuyển Record thành bins cho Aerospike.
func recordToBins(rec Record) map[string]interface{} {
	return map[string]interface{}{
		"Dnn":          rec.Dnn,
		"IPType":       rec.IPType,
		"State":        rec.State,
		"Timer":        rec.Timer,
		"SEID":         rec.SEID,
		"startTime":    rec.StartTime,
		"attachTime":   rec.AttachTime,
		"FTEIDC":       rec.FTEIDC,
		"IP":           rec.IP,
		"SNSSAI":       rec.SNSSAI,
		"UpfSelection": rec.UpfSelection,
		"payload":      rec.Payload,
		"TEIDC":        rec.TEIDC,
	}
}

// recordToHash chuyển Record thành danh sách field/value cho Redis HSET.
// Thứ tự field cố định để đảm bảo deterministic. Giá trị số được convert
// sẵn sang string tại đây (ngoài vùng đo latency).
func recordToHash(rec Record) []interface{} {
	return []interface{}{
		"Dnn", rec.Dnn,
		"IPType", rec.IPType,
		"State", rec.State,
		"Timer", strconv.Itoa(rec.Timer),
		"SEID", rec.SEID,
		"startTime", strconv.FormatInt(rec.StartTime, 10),
		"attachTime", strconv.FormatInt(rec.AttachTime, 10),
		"FTEIDC", rec.FTEIDC,
		"IP", rec.IP,
		"SNSSAI", rec.SNSSAI,
		"UpfSelection", strconv.FormatInt(rec.UpfSelection, 10),
		"payload", rec.Payload,
		"TEIDC", rec.TEIDC,
	}
}

// makePayload dựng payload ở định dạng mà database đích cần.
// Được gọi trong generator (ngoài vùng đo latency), nên chi phí serialize
// không bị tính vào latency của database.
func makePayload(rec Record, dbType string) *Payload {
	if dbType == "aerospike" {
		return &Payload{Bins: recordToBins(rec)}
	}
	return &Payload{Hash: recordToHash(rec)}
}

// ---------------------------------------------------------------------------
// WorkloadGenerator (mode run / open-loop)
// ---------------------------------------------------------------------------

// WorkloadGenerator sinh ra các Request theo tỉ lệ cấu hình (GET/SET/DELETE/QUERY).
// Sử dụng deterministic seed để đảm bảo reproducibility:
// cùng seed + cùng cấu hình → cùng chuỗi request.
//
// KHÔNG thread-safe. Chỉ được gọi từ một goroutine duy nhất
// (trong thiết kế này là goroutine pre-generate của RateController).
type WorkloadGenerator struct {
	rng              *rand.Rand // chọn loại thao tác, key, query
	updateRng        *rand.Rand // sinh nội dung record cho SET
	keys             *KeySpace
	payloadSize      int
	dbType           string
	queriesTag       []string // pre-defined query tag patterns
	queriesNumeric   []string // pre-defined query numeric patterns
	queriesOr        []string // pre-defined query OR patterns
	queriesPrefix    []string // pre-defined query prefix patterns
	getRatio         float64  // tỉ lệ tích luỹ: [0, getRatio) → GET
	setRatio         float64  // tỉ lệ tích luỹ: [getRatio, setRatio) → SET
	delRatio         float64  // tỉ lệ tích luỹ: [setRatio, delRatio) → DELETE
	queryTagRatio    float64  // tỉ lệ tích luỹ: [delRatio, queryTagRatio) → QUERY_TAG
	queryNumRatio    float64  // tỉ lệ tích luỹ: [queryTagRatio, queryNumRatio) → QUERY_NUMERIC
	queryOrRatio     float64  // tỉ lệ tích luỹ: [queryNumRatio, queryOrRatio) → QUERY_OR
	queryPrefixRatio float64  // tỉ lệ tích luỹ: [queryOrRatio, 1.0) → QUERY_PREFIX
}

// NewWorkloadGenerator khởi tạo generator từ config.
//
//   - streamSeed: seed của chuỗi request. Warmup và measurement dùng seed khác
//     nhau để không lặp lại cùng một chuỗi request.
//   - keys: trạng thái key dùng chung giữa các phase.
func NewWorkloadGenerator(cfg Config, streamSeed int64, keys *KeySpace) *WorkloadGenerator {
	total := float64(cfg.Workload.Get + cfg.Workload.Set + cfg.Workload.Delete + cfg.Workload.QueryTag + cfg.Workload.QueryNumeric + cfg.Workload.QueryOr + cfg.Workload.QueryPrefix)
	if total == 0 {
		total = 1 // tránh chia cho 0
	}
	getRatio := float64(cfg.Workload.Get) / total
	setRatio := getRatio + float64(cfg.Workload.Set)/total
	delRatio := setRatio + float64(cfg.Workload.Delete)/total
	queryTagRatio := delRatio + float64(cfg.Workload.QueryTag)/total
	queryNumRatio := queryTagRatio + float64(cfg.Workload.QueryNumeric)/total
	queryOrRatio := queryNumRatio + float64(cfg.Workload.QueryOr)/total
	queryPrefixRatio := 1.0

	queryPoolSize := 200
	queryRng := rand.New(rand.NewSource(streamSeed + 2))
	queriesTag := make([]string, 0, queryPoolSize)

	for i := 0; i < queryPoolSize; i++ {
		pattern := queryRng.Intn(5) // 5 normalized cardinality patterns for tag index lookup
		switch pattern {
		case 0:
			// Point lookup: SEID (100k distinct values) -> exact 1 record match
			queriesTag = append(queriesTag, "@SEID:{SEID_"+strconv.Itoa(queryRng.Intn(cfg.Data.KeyCount))+"}")
		case 1:
			// High cardinality: FTEIDC (1k distinct values) -> ~100 records match (0.1% dataset)
			queriesTag = append(queriesTag, "@FTEIDC:{FTEIDC_"+strconv.Itoa(queryRng.Intn(1000))+"}")
		case 2:
			// High cardinality: TEIDC (1k distinct values) -> ~100 records match (0.1% dataset)
			queriesTag = append(queriesTag, "@TEIDC:{TEIDC_"+strconv.Itoa(queryRng.Intn(1000))+"}")
		case 3:
			// Medium cardinality: Dnn (100 distinct values) -> ~1000 records match (1.0% dataset)
			queriesTag = append(queriesTag, "@Dnn:{Dnn_"+strconv.Itoa(queryRng.Intn(100))+"}")
		case 4:
			// Compound high/medium cardinality: FTEIDC (1k) + Dnn (100) -> ~1 record match
			queriesTag = append(queriesTag, "@FTEIDC:{FTEIDC_"+strconv.Itoa(queryRng.Intn(1000))+
				"} @Dnn:{Dnn_"+strconv.Itoa(queryRng.Intn(100))+"}")
		}
	}

	queriesNumeric := make([]string, 0, queryPoolSize)
	for i := 0; i < queryPoolSize; i++ {
		pattern := queryRng.Intn(3)
		switch pattern {
		case 0:
			// Timer range (width 10)
			tStart := queryRng.Intn(90)
			queriesNumeric = append(queriesNumeric, "@Timer:["+strconv.Itoa(tStart)+" "+strconv.Itoa(tStart+10)+"]")
		case 1:
			// startTime range (width 5000)
			sStart := 1700000000 + queryRng.Intn(9995000)
			queriesNumeric = append(queriesNumeric, "@startTime:["+strconv.Itoa(sStart)+" "+strconv.Itoa(sStart+5000)+"]")
		case 2:
			// UpfSelection range (width 10000)
			uStart := queryRng.Intn(1999900000)
			queriesNumeric = append(queriesNumeric, "@UpfSelection:["+strconv.Itoa(uStart)+" "+strconv.Itoa(uStart+10000)+"]")
		}
	}

	queriesOr := make([]string, 0, queryPoolSize)
	for i := 0; i < queryPoolSize; i++ {
		pattern := queryRng.Intn(3)
		switch pattern {
		case 0:
			// OR between two State & Timer ranges
			queriesOr = append(queriesOr, "(@State:{State_0} @Timer:[0 30]) | (@State:{State_2} @Timer:[70 100])")
		case 1:
			// OR between two FTEID values
			f1 := queryRng.Intn(1000)
			f2 := queryRng.Intn(1000)
			queriesOr = append(queriesOr, "(@FTEIDC:{FTEIDC_"+strconv.Itoa(f1)+"}) | (@FTEIDC:{FTEIDC_"+strconv.Itoa(f2)+"})")
		case 2:
			// OR between two Dnn values
			d1 := queryRng.Intn(100)
			d2 := queryRng.Intn(100)
			queriesOr = append(queriesOr, "(@Dnn:{Dnn_"+strconv.Itoa(d1)+"}) | (@Dnn:{Dnn_"+strconv.Itoa(d2)+"})")
		}
	}

	queriesPrefix := make([]string, 0, queryPoolSize)
	for i := 0; i < queryPoolSize; i++ {
		pattern := queryRng.Intn(4)
		switch pattern {
		// Prefix trên TAG field phải dùng cú pháp @field:{prefix*}
		case 0:
			queriesPrefix = append(queriesPrefix, "@Dnn:{Dnn_"+strconv.Itoa(queryRng.Intn(10))+"*}")
		case 1:
			queriesPrefix = append(queriesPrefix, "@FTEIDC:{FTEIDC_"+strconv.Itoa(queryRng.Intn(10))+"*}")
		case 2:
			queriesPrefix = append(queriesPrefix, "@TEIDC:{TEIDC_"+strconv.Itoa(queryRng.Intn(10))+"*}")
		case 3:
			queriesPrefix = append(queriesPrefix, "@IP:{IP_"+strconv.Itoa(queryRng.Intn(10))+"*}")
		}
	}

	return &WorkloadGenerator{
		rng:              rand.New(rand.NewSource(streamSeed)),
		updateRng:        rand.New(rand.NewSource(streamSeed + 1)),
		keys:             keys,
		payloadSize:      cfg.Data.PayloadSize,
		dbType:           cfg.Database.Type,
		queriesTag:       queriesTag,
		queriesNumeric:   queriesNumeric,
		queriesOr:        queriesOr,
		queriesPrefix:    queriesPrefix,
		getRatio:         getRatio,
		setRatio:         setRatio,
		delRatio:         delRatio,
		queryTagRatio:    queryTagRatio,
		queryNumRatio:    queryNumRatio,
		queryOrRatio:     queryOrRatio,
		queryPrefixRatio: queryPrefixRatio,
	}
}

// Next sinh ra request tiếp theo dựa trên tỉ lệ workload đã cấu hình.
//
// SET luôn ghi một record có SEID khớp với key (bench:key:X ↔ SEID_X),
// nên tính unique của SEID được giữ nguyên trong suốt benchmark.
// Record được sinh + serialize tại đây (goroutine pre-generate, ngoài vùng đo latency).
func (w *WorkloadGenerator) Next() (Request, bool) {
	r := w.rng.Float64()

	switch {
	case r < w.getRatio:
		id, _ := w.keys.randomLive(w.rng)
		return Request{Operation: OpGet, Key: w.keys.Key(id)}, true

	case r < w.setRatio:
		id := w.keys.pickForSet(w.rng)
		rec := generateRecord(w.updateRng, int(id), w.payloadSize)
		return Request{
			Operation: OpSet,
			Key:       w.keys.Key(id),
			Payload:   makePayload(rec, w.dbType),
		}, true

	case r < w.delRatio:
		id, ok := w.keys.randomLive(w.rng)
		if ok {
			w.keys.markDeleted(id)
		}
		return Request{Operation: OpDelete, Key: w.keys.Key(id)}, true

	case r < w.queryTagRatio:
		return Request{
			Operation: OpQueryTag,
			QueryStr:  w.queriesTag[w.rng.Intn(len(w.queriesTag))],
		}, true

	case r < w.queryNumRatio:
		return Request{
			Operation: OpQueryNumeric,
			QueryStr:  w.queriesNumeric[w.rng.Intn(len(w.queriesNumeric))],
		}, true

	case r < w.queryOrRatio:
		return Request{
			Operation: OpQueryOr,
			QueryStr:  w.queriesOr[w.rng.Intn(len(w.queriesOr))],
		}, true

	default:
		return Request{
			Operation: OpQueryPrefix,
			QueryStr:  w.queriesPrefix[w.rng.Intn(len(w.queriesPrefix))],
		}, true
	}
}

// ---------------------------------------------------------------------------
// LoadGenerator (mode load)
// ---------------------------------------------------------------------------

// LoadGenerator được sử dụng cho phase nạp dữ liệu (Load Mode).
// Sinh tuần tự các record đa dạng với SET request từ key 0 đến KeyCount - 1,
// mỗi record có giá trị ngẫu nhiên riêng, sau đó trả về false.
type LoadGenerator struct {
	rng         *rand.Rand
	current     atomic.Int64 // đọc đồng thời bởi progress reporter
	keyCount    int
	payloadSize int
	dbType      string
}

func NewLoadGenerator(cfg Config) *LoadGenerator {
	return &LoadGenerator{
		rng:         rand.New(rand.NewSource(cfg.Data.Seed)),
		keyCount:    cfg.Data.KeyCount,
		payloadSize: cfg.Data.PayloadSize,
		dbType:      cfg.Database.Type,
	}
}

func (l *LoadGenerator) Next() (Request, bool) {
	cur := int(l.current.Load())
	if cur >= l.keyCount {
		return Request{}, false // Đã sinh đủ số lượng key
	}

	rec := generateRecord(l.rng, cur, l.payloadSize)
	req := Request{
		Operation: OpSet,
		Key:       "bench:key:" + strconv.Itoa(cur),
		Payload:   makePayload(rec, l.dbType),
	}
	l.current.Add(1)
	return req, true
}

// Current trả về số key đã sinh, dùng cho progress reporting.
func (l *LoadGenerator) Current() int {
	return int(l.current.Load())
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
