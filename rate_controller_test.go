package main

import (
	"context"
	"testing"
	"time"
)

type dummyGen struct{}

func (d *dummyGen) Next() (Request, bool) {
	return Request{Operation: OpGet, Key: "bench:key:1"}, true
}

func TestRateControllerStandaloneFast(t *testing.T) {
	targetTPS := 50000
	rc := NewRateController(targetTPS)
	requests := make(chan Request, 10000)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Consumer that drains requests as fast as possible
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-requests:
			}
		}
	}()

	start := time.Now()
	rc.Run(ctx, &dummyGen{}, requests)
	elapsed := time.Since(start)

	sched := rc.Scheduled()
	issued := rc.Issued()
	dropped := rc.Dropped()

	t.Logf("Elapsed: %v, Sched: %d (%.0f/s), Issued (Sent): %d (%.0f/s), Dropped: %d (%.0f/s)",
		elapsed, sched, float64(sched)/elapsed.Seconds(),
		issued, float64(issued)/elapsed.Seconds(),
		dropped, float64(dropped)/elapsed.Seconds(),
	)

	// Sched và Issued phải đạt xấp xỉ targetTPS * 2 giây (±5%) và Dropped = 0
	expected := int64(targetTPS) * 2
	if sched < expected*95/100 {
		t.Errorf("sched %d quá thấp so với expected %d", sched, expected)
	}
	if issued < expected*95/100 {
		t.Errorf("issued %d quá thấp so với expected %d", issued, expected)
	}
	if dropped != 0 {
		t.Errorf("dropped = %d, expected 0", dropped)
	}
}

func TestRateControllerGroup(t *testing.T) {
	targetTPS := 105
	concurrency := 10
	group := NewRateControllerGroup(targetTPS, concurrency)

	if len(group) != concurrency {
		t.Fatalf("expected %d controllers, got %d", concurrency, len(group))
	}

	totalConfiguredTPS := 0
	for _, rc := range group {
		totalConfiguredTPS += rc.targetTPS
	}
	if totalConfiguredTPS != targetTPS {
		t.Errorf("expected total target TPS %d, got %d", targetTPS, totalConfiguredTPS)
	}

	// Chạy thử group trong 1 giây với consumer song song
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	channels := make([]chan Request, concurrency)
	for i := 0; i < concurrency; i++ {
		channels[i] = make(chan Request, 100)
		go func(ch chan Request) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ch:
				}
			}
		}(channels[i])
	}

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			group[idx].Run(ctx, &dummyGen{}, channels[idx])
		}(i)
	}

	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)

	sched := group.Scheduled()
	issued := group.Issued()
	dropped := group.Dropped()

	t.Logf("Group Sched: %d, Issued: %d, Dropped: %d", sched, issued, dropped)
	if issued < int64(targetTPS)*90/100 {
		t.Errorf("issued %d quá thấp so với expected ~%d", issued, targetTPS)
	}
	if dropped != 0 {
		t.Errorf("dropped = %d, expected 0", dropped)
	}
}
