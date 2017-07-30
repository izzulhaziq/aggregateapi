package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/izzulhaziq/glow/flow"
)

type aggregator struct {
	src       Source
	shard     int
	partition int
	dateFmt   string
	dateKey   string
}

// Aggregate aggregates the data from the src based on the param aggrParam values
func (aggr *aggregator) Aggregate(param aggrParam) <-chan map[string]interface{} {
	aggrOut := make(chan map[string]interface{})
	f := flow.New().
		Source(func(out chan map[string]interface{}) {
			err := aggr.src.Read(param.Query, aggr.getSelectedFields(param), out)
			if err != nil {
				panic(err)
			}
		}, aggr.shard).
		Map(func(data map[string]interface{}) flow.KeyValue {
			key, val := aggr.assignGroup(param, data)
			return flow.KeyValue{Key: key, Value: val}
		}).
		Partition(
			aggr.partition,
		).
		ReduceByKey(func(x int, y int) int {
			return x + y
		}).
		Map(func(group string, count int) flow.KeyValue {
			k := strings.Split(group, ",")
			v := map[string]int{
				strings.Join(k[:len(k)-1], ","): count,
			}
			// key = date, value = [group]
			return flow.KeyValue{Key: k[len(k)-1], Value: v}
		}).
		GroupByKey().
		Map(func(group string, values []map[string]int) map[string]interface{} {
			// { date, group1, group2, ... }
			return aggr.flattenResult(group, values)
		}).AddOutput(aggrOut)

	go f.Run()
	return aggrOut
}

func (aggr *aggregator) getSelectedFields(param aggrParam) (selFields fields) {
	selFields = append(selFields, param.GroupBy...)
	if param.Interval != "" {
		selFields = append(selFields, aggr.dateKey)
	}
	if param.AggregatedField != "" {
		selFields = append(selFields, param.AggregatedField)
	}
	return selFields
}

func (aggr *aggregator) assignGroup(param aggrParam, data map[string]interface{}) (string, int) {
	key := aggr.getKey(param.GroupBy, param.Interval, data)
	if param.AggregatedField == "" {
		return key, 1
	}

	val, ok := data[param.AggregatedField].(int)
	if !ok {
		val, _ = strconv.Atoi(data[param.AggregatedField].(string))
	}
	return key, val
}

func (aggr *aggregator) getKey(groupBy []string, interval string, data map[string]interface{}) (key string) {
	var keys []string
	for _, g := range groupBy {
		k, ok := data[g].(string)
		if !ok {
			k = "null"
		}
		keys = append(keys, k)
	}
	if interval == "" {
		key = strings.Join(keys, ",")
		return
	}

	t, ok := data[aggr.dateKey].(time.Time)
	if !ok {
		parsed, err := time.Parse(aggr.dateFmt, data[aggr.dateKey].(string))
		if err != nil {
			panic(err)
		}
		t = parsed
	}

	keys = append(keys, timeKey{t}.from(interval))
	key = strings.Join(keys, ",")
	return
}

func (aggr *aggregator) flattenResult(dataKey string, values []map[string]int) map[string]interface{} {
	flatten := map[string]interface{}{
		"dataKey": dataKey,
	}
	for _, item := range values {
		for k, v := range item {
			if k == "" {
				k = "total"
			}
			flatten[k] = v
		}
	}
	return flatten
}

type timeKey struct {
	time.Time
}

func (t timeKey) from(interval string) string {
	switch interval {
	case "daily":
		return t.Format("2006-01-02")
	case "monthly":
		return t.Format("2006-01")
	case "yearly":
		return t.Format("2006")
	default:
		return ""
	}
}

func closeFlow() {
	copy(flow.Contexts[0:], flow.Contexts[1:])
	flow.Contexts[len(flow.Contexts)-1] = nil
	flow.Contexts = flow.Contexts[:len(flow.Contexts)-1]
}

func filterFunc(data map[string]interface{}) bool {
	return true
}
