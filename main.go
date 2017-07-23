package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/go-zoo/bone"
	"github.com/izzulhaziq/glow/flow"
	"github.com/rs/cors"
)

type config struct {
	shard      int
	partition  int
	src        source
	dateKey    string
	dateFormat string
}

type aggrParam struct {
	GroupBy         []string `json:"groupBy"`
	Interval        string   `json:"interval"`
	AggregatedField string   `json:"aggregatedField"`
}

var cfg config

func main() {
	shard := flag.Int("shard", 1, "specify the number of data source shards")
	partition := flag.Int("partition", 2, "specify the number of partitions before reducing")
	csv := flag.String("csv", "", "specify the csv file as the datasource")
	port := flag.Int("port", 8080, "specify port to listen to")
	dateKey := flag.String("datekey", "Date", "specify the date field/key if using external sources")
	dateFormat := flag.String("datefmt", "2006-01-02", "specify the date format to parse")
	flag.Parse()
	cfg.parse(*shard, *partition, *csv, *dateFormat, *dateKey)

	fmt.Printf("Starting HTTP server on port :%d\n", *port)
	flow.Ready()

	server := startHTTPServer(*port)
	waitForStop()
	if err := server.Shutdown(nil); err != nil {
		panic(err)
	}

	fmt.Println("HTTP server has shutdown gracefully")
}

func (cfg *config) parse(shard, partition int, csv, dateFmt, dateKey string) {
	cfg.shard = shard
	cfg.partition = partition
	cfg.dateFormat = dateFmt
	cfg.dateKey = dateKey
	if csv == "" {
		cfg.src = &mockSource{}
		return
	}

	if _, err := os.Stat(csv); os.IsNotExist(err) {
		panic(err)
	}
	cfg.src = &csvSource{path: csv}
}

func waitForStop() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop
}

func startHTTPServer(port int) *http.Server {
	mux := bone.New()
	mux.Post("/aggregate", http.HandlerFunc(errorHandler(aggregateHandler)))
	handler := cors.Default().Handler(mux)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: handler,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			log.Printf("Httpserver: ListenAndServe() error: %s", err)
		}
	}()
	return srv
}

func aggregateHandler(w http.ResponseWriter, r *http.Request) (int, error) {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var param aggrParam
	err := decoder.Decode(&param)
	if err != nil {
		return http.StatusNoContent, err
	}

	aggregator := &aggregator{cfg.src, cfg.shard, cfg.partition}
	aggrOut := aggregator.aggregate(param)
	defer closeFlow()

	results := []map[string]interface{}{}
	for item := range aggrOut {
		results = append(results, item)
	}

	groupByKey := fmt.Sprintf("groupby_%s", param.GroupBy)
	result := map[string]interface{}{
		groupByKey: results,
	}

	encoder := json.NewEncoder(w)
	if err := encoder.Encode(result); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func errorHandler(f func(http.ResponseWriter, *http.Request) (int, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		code, err := f(w, r)
		if err != nil {
			http.Error(w, err.Error(), code)
			log.Printf("handling %q: %v", r.RequestURI, err)
		}
	}
}
