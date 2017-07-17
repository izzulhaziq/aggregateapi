package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"

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

func aggregate(groupBy []string, interval string) []map[string]interface{} {
	aggrOut := make(chan map[string]interface{})
	f := flow.New().Source(func(out chan map[string]interface{}) {
		for _, d := range mockData() {
			out <- d
		}
	}, 5).Map(func(data map[string]interface{}) flow.KeyValue {
		key := groupKey(groupBy, interval, data)
		return flow.KeyValue{Key: key, Value: data["Value"].(int)}
	}).ReduceByKey(func(x int, y int) int {
		return x + y
	}).Map(func(group string, count int) flow.KeyValue {
		k := strings.Split(group, ",")
		v := map[string]int{
			strings.Join(k[:len(k)-1], "-"): count,
		}
		// key = date, value = [group]
		return flow.KeyValue{Key: k[len(k)-1], Value: v}
	}).GroupByKey().Map(func(group string, values []map[string]int) map[string]interface{} {
		flatten := map[string]interface{}{
			"date": group,
		}
		for _, item := range values {
			for k, v := range item {
				flatten[k] = v
			}
		}
		// { date, group1, group2, ... }
		return flatten
	}).AddOutput(aggrOut)

	flow.Ready()
	go f.Run()

	result := []map[string]interface{}{}
	for item := range aggrOut {
		result = append(result, item)
	}

	return result
}

func groupKey(groupBy []string, interval string, data map[string]interface{}) (key string) {
	var keys []string

	for _, g := range groupBy {
		keys = append(keys, data[g].(string))
	}
	time := data["StartDateTime"].(time.Time)
	keys = append(keys, fromInterval(time, interval))
	key = strings.Join(keys, ",")
	return
}

func fromInterval(t time.Time, interval string) string {
	var timeKey string
	switch interval {
	case "daily":
		timeKey = fmt.Sprintf("%d-%d-%d", t.Year(), t.Month(), t.Day())
	case "monthly":
		timeKey = fmt.Sprintf("%d-%d", t.Year(), t.Month())
	}
	return timeKey
}

func mockData() (data []map[string]interface{}) {
	for i := 0; i < 365; i++ {
		for j := 0; j < 2; j++ {
			data = append(data, map[string]interface{}{
				"LicenseId":       "license1",
				"BilledProductId": "product" + strconv.Itoa(j),
				"StartDateTime":   time.Now().AddDate(0, 0, -i),
				"Value":           rand.Intn(100),
			})
		}
	}
	return
}
