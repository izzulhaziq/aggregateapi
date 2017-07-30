package main

import (
	"testing"
	"time"
)

type mockSrc struct {
	count int
}

func (src mockSrc) Read(q string, f fields, out chan map[string]interface{}) error {
	for i := 0; i < src.count; i++ {
		rec := map[string]interface{}{
			"LicenseId":       "license1",
			"BilledProductId": "product1",
			"Date":            time.Now().AddDate(0, 0, -i),
			"Value":           2,
		}
		out <- rec
	}
	return nil
}

func TestAggregateByLicense(t *testing.T) {
	target := &aggregator{&mockSrc{5}, 1, 1, time.RFC3339, "Date"}
	param := aggrParam{
		GroupBy:         []string{"LicenseId"},
		Interval:        "daily",
		AggregatedField: "Value",
	}

	var results []map[string]interface{}
	for r := range target.Aggregate(param) {
		results = append(results, r)
	}

	if len(results) != 5 {
		t.Errorf("Expected results to be 5; got %d", len(results))
	}

	for _, r := range results {
		if v, ok := r["license1"]; ok {
			if v != 2 {
				t.Errorf("aggregated value not as expected, expect %d get %d", 1, v)
			}
		} else {
			t.Errorf("aggregate group is not as expected, expect license1 get keys %v", r)
		}
	}
}

func TestAggregateByProduct(t *testing.T) {
	target := &aggregator{&mockSrc{5}, 1, 1, time.RFC3339, "Date"}
	param := aggrParam{
		GroupBy:         []string{"LicenseId", "BilledProductId"},
		Interval:        "monthly",
		AggregatedField: "Value",
	}

	var results []map[string]interface{}
	for r := range target.Aggregate(param) {
		results = append(results, r)
	}

	if len(results) != 1 {
		t.Errorf("Expected results to be 1; got %d", len(results))
	}

	if v, ok := results[0]["license1,product1"]; ok {
		if v != 10 {
			t.Errorf("aggregated value not as expected, expect %d get %d", 5, v)
		}
	} else {
		t.Errorf("aggregate group is not as expected, expect license1 get keys %v", results[0])
	}
}

func TestAggregateByProductRowCount(t *testing.T) {
	target := &aggregator{&mockSrc{5}, 1, 1, time.RFC3339, "Date"}
	param := aggrParam{
		GroupBy:         []string{"LicenseId", "BilledProductId"},
		Interval:        "yearly",
		AggregatedField: "",
	}

	var results []map[string]interface{}
	for r := range target.Aggregate(param) {
		results = append(results, r)
	}

	if len(results) != 1 {
		t.Errorf("Expected results to be 1; got %d", len(results))
	}

	if v, ok := results[0]["license1,product1"]; ok {
		if v != 5 {
			t.Errorf("aggregated value not as expected, expect %d get %d", 5, v)
		}
	} else {
		t.Errorf("aggregate group is not as expected, expect license1 get keys %v", results[0])
	}
}

var benchRes []map[string]interface{}

func benchmarkAggregate(i int, b *testing.B) {
	b.ReportAllocs()
	target := &aggregator{&mockSrc{i}, 1, 1, time.RFC3339, "Date"}
	param := aggrParam{
		GroupBy:         []string{"LicenseId"},
		Interval:        "daily",
		AggregatedField: "Value",
	}
	var results []map[string]interface{}
	for n := 0; n < b.N; n++ {
		for r := range target.Aggregate(param) {
			results = append(results, r)
		}
	}
	benchRes = results
}

func BenchmarkAggregate1000(b *testing.B)  { benchmarkAggregate(1000, b) }
func BenchmarkAggregate10000(b *testing.B) { benchmarkAggregate(10000, b) }
