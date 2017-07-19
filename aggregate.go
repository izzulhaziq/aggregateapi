package main

import (
	"fmt"
	"strings"
	"time"

	"strconv"

	"github.com/izzulhaziq/glow/flow"
)

func aggregate(param aggrParam) <-chan map[string]interface{} {
	aggrOut := make(chan map[string]interface{})
	f := flow.New().Source(func(out chan map[string]interface{}) {
		if err := cfg.src.read(out); err != nil {
			panic(err)
		}
	}, 5).Map(func(data map[string]interface{}) flow.KeyValue {
		key := groupKey(param.GroupBy, param.Interval, data)
		val, ok := data[param.AggregatedField].(int)
		if !ok {
			val, _ = strconv.Atoi(data[param.AggregatedField].(string))
		}
		return flow.KeyValue{Key: key, Value: val}
	}).ReduceByKey(func(x int, y int) int {
		return x + y
	}).Map(func(group string, count int) flow.KeyValue {
		k := strings.Split(group, ",")
		v := map[string]int{
			strings.Join(k[:len(k)-1], ","): count,
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

	go f.Run()
	return aggrOut
}

func closeFlow() {
	copy(flow.Contexts[0:], flow.Contexts[1:])
	flow.Contexts[len(flow.Contexts)-1] = nil
	flow.Contexts = flow.Contexts[:len(flow.Contexts)-1]
}

func groupKey(groupBy []string, interval string, data map[string]interface{}) (key string) {
	var keys []string
	for _, g := range groupBy {
		keys = append(keys, data[g].(string))
	}

	t, ok := data[cfg.dateKey].(time.Time)
	if !ok {
		parsed, err := time.Parse(cfg.dateFormat, data[cfg.dateKey].(string))
		if err != nil {
			panic(err)
		}
		t = parsed
	}

	keys = append(keys, fromInterval(t, interval))
	key = strings.Join(keys, ",")
	return
}

func fromInterval(t time.Time, interval string) string {
	var timeKey string
	switch interval {
	case "daily":
		timeKey = fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
	case "monthly":
		timeKey = fmt.Sprintf("%04d-%02d", t.Year(), t.Month())
	}
	return timeKey
}
