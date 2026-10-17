package main

import (
	"context"
	"errors"
	"strconv"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v7"
)

type mockDB struct {
	getFunc func(ctx context.Context, key string) error
}

func (m *mockDB) Get(ctx context.Context, key string) error {
	if m.getFunc != nil {
		return m.getFunc(ctx, key)
	}
	return nil
}
func (m *mockDB) Set(ctx context.Context, key string, payload *Payload) error { return nil }
func (m *mockDB) Delete(ctx context.Context, key string) error                 { return nil }
func (m *mockDB) Close() error                                                 { return nil }
func (m *mockDB) Ping(ctx context.Context) error                              { return nil }
func (m *mockDB) Init(ctx context.Context) error                              { return nil }
func (m *mockDB) Query(ctx context.Context, query *QuerySpec) error           { return nil }
func (m *mockDB) Name() string                                                { return "mock" }

func TestExecuteRequestLatencyExclusion(t *testing.T) {
	// 1. Success case: Latency MUST be recorded
	resSuccess := &WorkerResult{}
	dbSuccess := &mockDB{getFunc: func(ctx context.Context, key string) error { return nil }}
	executeRequest(dbSuccess, Request{Operation: OpGet, Key: "bench:key:1"}, resSuccess, true, true)

	if resSuccess.Ops[OpGet].Success != 1 {
		t.Errorf("Expected Success=1, got %d", resSuccess.Ops[OpGet].Success)
	}
	if len(resSuccess.Ops[OpGet].Latencies) != 1 {
		t.Errorf("Expected 1 latency recorded for Success, got %d", len(resSuccess.Ops[OpGet].Latencies))
	}

	// 2. NotFound case: Latency MUST NOT be recorded
	resNotFound := &WorkerResult{}
	dbNotFound := &mockDB{getFunc: func(ctx context.Context, key string) error { return ErrNotFound }}
	executeRequest(dbNotFound, Request{Operation: OpGet, Key: "bench:key:2"}, resNotFound, true, true)

	if resNotFound.Ops[OpGet].NotFound != 1 {
		t.Errorf("Expected NotFound=1, got %d", resNotFound.Ops[OpGet].NotFound)
	}
	if resNotFound.Ops[OpGet].Success != 0 {
		t.Errorf("Expected Success=0, got %d", resNotFound.Ops[OpGet].Success)
	}
	if len(resNotFound.Ops[OpGet].Latencies) != 0 {
		t.Errorf("Expected 0 latency recorded for NotFound, got %d", len(resNotFound.Ops[OpGet].Latencies))
	}
	if len(resNotFound.windowLatencies) != 0 {
		t.Errorf("Expected 0 window latency for NotFound, got %d", len(resNotFound.windowLatencies))
	}

	// 3. Error case: Latency MUST NOT be recorded
	resError := &WorkerResult{}
	dbError := &mockDB{getFunc: func(ctx context.Context, key string) error { return errors.New("connection reset") }}
	executeRequest(dbError, Request{Operation: OpGet, Key: "bench:key:3"}, resError, true, true)

	if resError.Ops[OpGet].Errors != 1 {
		t.Errorf("Expected Errors=1, got %d", resError.Ops[OpGet].Errors)
	}
	if resError.Ops[OpGet].Success != 0 {
		t.Errorf("Expected Success=0, got %d", resError.Ops[OpGet].Success)
	}
	if len(resError.Ops[OpGet].Latencies) != 0 {
		t.Errorf("Expected 0 latency recorded for Error, got %d", len(resError.Ops[OpGet].Latencies))
	}
	if len(resError.windowLatencies) != 0 {
		t.Errorf("Expected 0 window latency for Error, got %d", len(resError.windowLatencies))
	}
}

func TestAerospikeGetKeyPrebuilt(t *testing.T) {
	keyCount := 100
	keys := make([]*as.Key, keyCount)
	for i := 0; i < keyCount; i++ {
		k, err := as.NewKey("smf", "session", "bench:key:"+strconv.Itoa(i))
		if err != nil {
			t.Fatalf("as.NewKey failed: %v", err)
		}
		keys[i] = k
	}

	db := &AerospikeDB{
		namespace: "smf",
		setName:   "session",
		keys:      keys,
	}

	// 1. In-range pre-built lookup
	k42, err := db.getKey("bench:key:42")
	if err != nil {
		t.Fatalf("getKey error: %v", err)
	}
	if k42 != keys[42] {
		t.Errorf("Expected exact pointer match for pre-built key, got %v vs %v", k42, keys[42])
	}

	// 2. Out-of-range fallback lookup
	kOut, err := db.getKey("bench:key:999")
	if err != nil {
		t.Fatalf("getKey out-of-range error: %v", err)
	}
	if kOut == nil {
		t.Fatal("Expected valid fallback key, got nil")
	}

	// 3. Arbitrary string fallback
	kCustom, err := db.getKey("custom:key:abc")
	if err != nil {
		t.Fatalf("getKey custom error: %v", err)
	}
	if kCustom == nil {
		t.Fatal("Expected valid custom key, got nil")
	}
}
