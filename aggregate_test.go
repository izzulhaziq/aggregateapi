package main

import (
	"testing"
	"time"
)

type mockSrc struct{}

func (src mockSrc) read(out chan map[string]interface{}) error {
	for i := 0; i < 5; i++ {
		rec := map[string]interface{}{
			"LicenseId":       "license1",
			"BilledProductId": "product1",
			"Date":            time.Now().AddDate(0, 0, -i),
			"Value":           1,
		}
		out <- rec
	}
	return nil
}

func TestAggregateByLicense(t *testing.T) {
	target := &aggregator{&mockSrc{}, 1, 1, time.RFC3339, "Date"}
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
		t.Error("Expected results to be 5")
	}

	for _, r := range results {
		if v, ok := r["license1"]; ok {
			if v != 1 {
				t.Errorf("aggregated value not as expected, expect %d get %d", 1, v)
			}
		} else {
			t.Errorf("aggregate group is not as expected, expect license1 get keys %v", r)
		}
	}
}

func TestAggregateByProduct(t *testing.T) {
	target := &aggregator{&mockSrc{}, 1, 1, time.RFC3339, "Date"}
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
		t.Error("Expected results to be 1")
	}

	if v, ok := results[0]["license1,product1"]; ok {
		if v != 5 {
			t.Errorf("aggregated value not as expected, expect %d get %d", 5, v)
		}
	} else {
		t.Errorf("aggregate group is not as expected, expect license1 get keys %v", results[0])
	}
}
