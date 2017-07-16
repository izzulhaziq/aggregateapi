package main

import (
	"encoding/json"
	"log"
	"net/http"

	"fmt"
	"time"

	"github.com/chrislusf/glow/flow"
	"github.com/go-zoo/bone"
)

type aggrParam struct {
	GroupBy  []string `json:"groupBy"`
	Interval string   `json:"interval"`
}

func main() {
	mux := bone.New()
	mux.Post("/aggregate", http.HandlerFunc(aggregateHandler))

	log.Fatal(http.ListenAndServe(":8080", mux))
}

func aggregateHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var param aggrParam
	err := decoder.Decode(&param)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	results := aggregate(param.GroupBy, param.Interval)

	result := map[string]interface{}{}
	groupByKey := fmt.Sprintf("groupby_%s", param.GroupBy)
	result[groupByKey] = results

	w.Header().Set("content-type", "application/json")
	encoder := json.NewEncoder(w)

	if err := encoder.Encode(result); err != nil {
		log.Println("unable to encode result")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func aggregate(groupBy []string, interval string) map[string]int {
	result := map[string]int{}
	f := flow.New().Source(func(out chan map[string]interface{}) {
		for _, d := range mockData() {
			out <- d
		}
	}, 5).Map(func(data map[string]interface{}) flow.KeyValue {
		key := groupKey(groupBy, interval, data)
		return flow.KeyValue{Key: key, Value: data["Value"].(int)}
	}).ReduceByKey(func(x int, y int) int {
		return x + y
	}).Map(func(group string, count int) {
		result[group] = count
	})

	flow.Ready()
	f.Run()
	return result
}

func groupKey(groupBy []string, interval string, data map[string]interface{}) (key string) {
	var timeKey string
	time := data["StartDateTime"].(time.Time)
	switch interval {
	case "daily":
		timeKey = fmt.Sprintf("%d-%d-%d", time.Year(), time.Month(), time.Day())
	case "monthly":
		timeKey = fmt.Sprintf("%d-%d", time.Year(), time.Month())
	}

	for _, g := range groupBy {
		key = fmt.Sprintf("%s_%s_", key, data[g])
	}

	key = fmt.Sprintf("%s_%s_", key, timeKey)
	return
}

func mockData() (data []map[string]interface{}) {
	for i := 0; i < 100000; i++ {
		data = append(data, map[string]interface{}{
			"LicenseId":       "license1",
			"BilledProductId": "product1",
			"StartDateTime":   time.Now().AddDate(0, 0, i%365),
			"Value":           1,
		})

		data = append(data, map[string]interface{}{
			"LicenseId":       "license1",
			"BilledProductId": "product2",
			"StartDateTime":   time.Now().AddDate(0, 0, i%365),
			"Value":           1,
		})

		data = append(data, map[string]interface{}{
			"LicenseId":       "license1",
			"BilledProductId": "product3",
			"StartDateTime":   time.Now().AddDate(0, 0, i%365),
			"Value":           1,
		})
	}

	return
}
