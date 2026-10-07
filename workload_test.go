package main

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

// hashToMap chuyển payload HSET (f1, v1, f2, v2, ...) thành map để kiểm tra.
func hashToMap(t *testing.T, p *Payload) map[string]string {
	t.Helper()
	if p == nil || len(p.Hash) == 0 || len(p.Hash)%2 != 0 {
		t.Fatalf("invalid hash payload: %v", p)
	}
	m := make(map[string]string, len(p.Hash)/2)
	for i := 0; i < len(p.Hash); i += 2 {
		k, ok1 := p.Hash[i].(string)
		v, ok2 := p.Hash[i+1].(string)
		if !ok1 || !ok2 {
			t.Fatalf("hash field/value must be strings: %v=%v", p.Hash[i], p.Hash[i+1])
		}
		m[k] = v
	}
	return m
}

// TestGenerateRecordDiversity kiểm tra dữ liệu sinh ra có đa dạng
func TestGenerateRecordDiversity(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))
	count := 100000

	stateCount := make(map[string]int)
	ipTypeCount := make(map[string]int)
	snssaiCount := make(map[string]int)
	seidSet := make(map[string]bool)

	for i := 0; i < count; i++ {
		rec := generateRecord(rng, i, 0)
		stateCount[rec.State]++
		ipTypeCount[rec.IPType]++
		snssaiCount[rec.SNSSAI]++
		seidSet[rec.SEID] = true
	}

	// State phải có đủ 3 giá trị
	for _, s := range []string{"State_0", "State_1", "State_2"} {
		if stateCount[s] == 0 {
			t.Errorf("State %s has 0 records", s)
		} else {
			t.Logf("%s: %d records", s, stateCount[s])
		}
	}

	// IPType phải có đủ 2 giá trị
	for _, s := range []string{"IPType_0", "IPType_1"} {
		if ipTypeCount[s] == 0 {
			t.Errorf("IPType %s has 0 records", s)
		} else {
			t.Logf("%s: %d records", s, ipTypeCount[s])
		}
	}

	// SNSSAI phải có đủ 3 giá trị
	for _, s := range []string{"SNSSAI_0", "SNSSAI_1", "SNSSAI_2"} {
		if snssaiCount[s] == 0 {
			t.Errorf("SNSSAI %s has 0 records", s)
		} else {
			t.Logf("%s: %d records", s, snssaiCount[s])
		}
	}

	// SEID phải unique
	if len(seidSet) != count {
		t.Errorf("Expected %d unique SEIDs, got %d", count, len(seidSet))
	} else {
		t.Logf("Unique SEID: %d", len(seidSet))
	}
}

// TestGenerateRecordDeterministic kiểm tra cùng seed cho cùng kết quả
func TestGenerateRecordDeterministic(t *testing.T) {
	seed := int64(12345)
	count := 1000

	// Run 1
	rng1 := rand.New(rand.NewSource(seed))
	records1 := make([]Record, count)
	for i := 0; i < count; i++ {
		records1[i] = generateRecord(rng1, i, 64)
	}

	// Run 2 — cùng seed
	rng2 := rand.New(rand.NewSource(seed))
	records2 := make([]Record, count)
	for i := 0; i < count; i++ {
		records2[i] = generateRecord(rng2, i, 64)
	}

	for i := 0; i < count; i++ {
		j1, _ := json.Marshal(records1[i])
		j2, _ := json.Marshal(records2[i])
		if string(j1) != string(j2) {
			t.Errorf("Record %d differs between runs", i)
			return
		}
	}
	t.Logf("All %d records are identical across 2 runs with seed=%d", count, seed)
}

// TestLoadGeneratorDiversity kiểm tra LoadGenerator sinh record đa dạng
func TestLoadGeneratorDiversity(t *testing.T) {
	cfg := defaultConfig()
	cfg.Data.KeyCount = 1000
	cfg.Data.Seed = 12345

	gen := NewLoadGenerator(cfg)

	stateCount := make(map[string]int)
	var records []map[string]string

	for {
		req, hasMore := gen.Next()
		if !hasMore {
			break
		}
		rec := hashToMap(t, req.Payload)
		if len(rec) != 13 {
			t.Fatalf("expected 13 hash fields, got %d", len(rec))
		}
		records = append(records, rec)
		stateCount[rec["State"]]++
	}

	if len(records) != cfg.Data.KeyCount {
		t.Errorf("Expected %d records, got %d", cfg.Data.KeyCount, len(records))
	}

	// Kiểm tra không phải tất cả giống nhau
	if len(stateCount) < 3 {
		t.Errorf("Expected 3 State values, got %d", len(stateCount))
	}

	// Kiểm tra record đầu và cuối khác nhau
	if reflect.DeepEqual(records[0], records[len(records)-1]) {
		t.Error("First and last records are identical — data is not diverse")
	}

	for k, v := range stateCount {
		t.Logf("%s: %d", k, v)
	}
}

func runWorkloadCfg(get, set, del, qt, qn int) Config {
	cfg := defaultConfig()
	cfg.Data.KeyCount = 1000
	cfg.Workload = WorkloadCfg{Get: get, Set: set, Delete: del, QueryTag: qt, QueryNumeric: qn}
	return cfg
}

// TestSetKeepsSEIDMatchingKey: SET ở mode run phải ghi record có SEID khớp với key.
func TestSetKeepsSEIDMatchingKey(t *testing.T) {
	cfg := runWorkloadCfg(0, 100, 0, 0, 0)
	gen := NewWorkloadGenerator(cfg, cfg.Data.Seed, NewKeySpace(cfg.Data.KeyCount))

	for i := 0; i < 5000; i++ {
		req, _ := gen.Next()
		rec := hashToMap(t, req.Payload)
		id := strings.TrimPrefix(req.Key, "bench:key:")
		if rec["SEID"] != "SEID_"+id {
			t.Fatalf("key %s has SEID %s", req.Key, rec["SEID"])
		}
	}
}

// TestDeleteDoesNotShrinkDataset: với set >= delete, số key live gần như không đổi,
// và GET không bao giờ nhắm vào key đã bị xoá.
func TestDeleteDoesNotShrinkDataset(t *testing.T) {
	cfg := runWorkloadCfg(40, 35, 25, 0, 0)
	ks := NewKeySpace(cfg.Data.KeyCount)
	gen := NewWorkloadGenerator(cfg, cfg.Data.Seed, ks)

	deleted := make(map[string]bool)
	minLive := ks.LiveCount()
	for i := 0; i < 200000; i++ {
		req, _ := gen.Next()
		switch req.Operation {
		case OpGet:
			if deleted[req.Key] {
				t.Fatalf("GET on deleted key %s", req.Key)
			}
		case OpDelete:
			if deleted[req.Key] {
				t.Fatalf("DELETE on already-deleted key %s", req.Key)
			}
			deleted[req.Key] = true
		case OpSet:
			delete(deleted, req.Key)
		}
		if ks.LiveCount() < minLive {
			minLive = ks.LiveCount()
		}
		if ks.LiveCount()+len(deleted) != cfg.Data.KeyCount {
			t.Fatalf("keyspace out of sync: live=%d deleted=%d", ks.LiveCount(), len(deleted))
		}
	}
	// set > delete → key bị xoá được tái tạo ngay, dataset không teo dần.
	if minLive < cfg.Data.KeyCount*95/100 {
		t.Errorf("dataset shrank too much: min live = %d / %d", minLive, cfg.Data.KeyCount)
	}
	t.Logf("min live = %d / %d, final live = %d", minLive, cfg.Data.KeyCount, ks.LiveCount())
}

// TestKeySpaceInvariants kiểm tra pos/live/dead luôn nhất quán.
func TestKeySpaceInvariants(t *testing.T) {
	ks := NewKeySpace(100)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		if rng.Intn(2) == 0 {
			if id, ok := ks.randomLive(rng); ok {
				ks.markDeleted(id)
			}
		} else {
			ks.pickForSet(rng)
		}
		for idx, id := range ks.live {
			if !ks.alive[id] || ks.pos[id] != int32(idx) {
				t.Fatalf("live list corrupted at %d", idx)
			}
		}
		for idx, id := range ks.dead {
			if ks.alive[id] || ks.pos[id] != int32(idx) {
				t.Fatalf("dead list corrupted at %d", idx)
			}
		}
		if len(ks.live)+len(ks.dead) != ks.Len() {
			t.Fatalf("size mismatch")
		}
	}
}

// TestWarmupStreamDiffersFromMeasurement: warmup và measurement không được
// sinh cùng chuỗi request; cùng seed thì chuỗi phải giống hệt (reproducible).
func TestWarmupStreamDiffersFromMeasurement(t *testing.T) {
	cfg := runWorkloadCfg(50, 0, 0, 25, 25)
	seq := func(seed int64) []string {
		gen := NewWorkloadGenerator(cfg, seed, NewKeySpace(cfg.Data.KeyCount))
		out := make([]string, 200)
		for i := range out {
			req, _ := gen.Next()
			out[i] = req.Operation.String() + req.Key + req.QueryStr
		}
		return out
	}
	m1, m2 := seq(cfg.Data.Seed), seq(cfg.Data.Seed)
	w := seq(cfg.Data.Seed + warmupSeedOffset)

	same := 0
	for i := range m1 {
		if m1[i] != m2[i] {
			t.Fatalf("same seed produced different request at %d", i)
		}
		if m1[i] == w[i] {
			same++
		}
	}
	if same > len(m1)/10 {
		t.Errorf("warmup and measurement streams too similar: %d/%d identical", same, len(m1))
	}
}

// TestAerospikePayloadIsPrebuilt: với Aerospike, payload phải là bins dựng sẵn.
func TestAerospikePayloadIsPrebuilt(t *testing.T) {
	cfg := runWorkloadCfg(0, 100, 0, 0, 0)
	cfg.Database.Type = "aerospike"
	gen := NewWorkloadGenerator(cfg, cfg.Data.Seed, NewKeySpace(cfg.Data.KeyCount))
	req, _ := gen.Next()
	if req.Payload == nil || req.Payload.Bins == nil {
		t.Fatal("expected pre-built bins")
	}
	if len(req.Payload.Bins) != 13 {
		t.Errorf("expected 13 bins, got %d", len(req.Payload.Bins))
	}
	id := strings.TrimPrefix(req.Key, "bench:key:")
	if req.Payload.Bins["SEID"] != "SEID_"+id {
		t.Errorf("SEID bin %v does not match key %s", req.Payload.Bins["SEID"], req.Key)
	}
}

func TestParseAerospikeHost(t *testing.T) {
	cases := map[string]int{"10.0.0.1": 3000, "db-0.svc:3100": 3100}
	for in, port := range cases {
		h, err := parseAerospikeHost(in)
		if err != nil || h.Port != port {
			t.Errorf("%s: got %v, %v", in, h, err)
		}
	}
	if _, err := parseAerospikeHost("host:abc"); err == nil {
		t.Error("expected error for bad port")
	}
}

// TestParseQueryExpr kiểm tra việc dịch cú pháp RediSearch sang Aerospike.
func TestParseQueryExpr(t *testing.T) {
	// AND thường: điều kiện đầu dùng SI, điều kiện sau là expression.
	if f, e := parseQueryExpr("@FTEIDC:{FTEIDC_1} @State:{State_2}"); f == nil || e == nil {
		t.Errorf("AND query: want filter+expr, got %v, %v", f, e)
	}
	// OR mà mỗi nhánh chỉ có 1 TAG: cả hai nhánh phải thành expression, không dùng SI.
	if f, e := parseQueryExpr("(@FTEIDC:{FTEIDC_1}) | (@FTEIDC:{FTEIDC_2})"); f != nil || e == nil {
		t.Errorf("OR query: want no filter + expr, got %v, %v", f, e)
	}
	// Prefix trên TAG: không được bị nhận nhầm thành equality filter.
	if tagRegex.MatchString("@Dnn:{Dnn_9*}") {
		t.Error("tagRegex must not match prefix query")
	}
	if f, e := parseQueryExpr("@Dnn:{Dnn_9*}"); f != nil || e == nil {
		t.Errorf("prefix query: want no filter + expr, got %v, %v", f, e)
	}
}
