package main

import (
	"encoding/json"
	"math/rand"
	"testing"
)

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
	var records []Record

	for {
		req, hasMore := gen.Next()
		if !hasMore {
			break
		}
		var rec Record
		json.Unmarshal(req.Value, &rec)
		records = append(records, rec)
		stateCount[rec.State]++
	}

	if len(records) != cfg.Data.KeyCount {
		t.Errorf("Expected %d records, got %d", cfg.Data.KeyCount, len(records))
	}

	// Kiểm tra không phải tất cả giống nhau
	if len(stateCount) < 3 {
		t.Errorf("Expected 3 State values, got %d", len(stateCount))
	}

	// Kiểm tra record đầu và cuối khác nhau
	j1, _ := json.Marshal(records[0])
	j2, _ := json.Marshal(records[len(records)-1])
	if string(j1) == string(j2) {
		t.Error("First and last records are identical — data is not diverse")
	}

	for k, v := range stateCount {
		t.Logf("%s: %d", k, v)
	}
}
