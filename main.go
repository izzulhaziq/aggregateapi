package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"

	"fmt"
	"time"

	"flag"

	"github.com/go-zoo/bone"
	"github.com/izzulhaziq/glow/flow"
	"github.com/rs/cors"
)

type config struct {
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
	cfg = config{}
	csv := flag.String("csv", "", "specify the csv file as the datasource")
	dateKey := flag.String("datekey", "Date", "specify the date field/key if using external sources")
	dateFormat := flag.String("datefmt", "2006-01-02", "specify the date format to parse")
	flag.Parse()
	parseConfig(*csv, *dateFormat, *dateKey)

	fmt.Println("Starting HTTP server")
	flow.Ready()

	server := startHTTPServer()
	waitForStopSignal()
	if err := server.Shutdown(nil); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	fmt.Println("HTTP server has shutdown gracefully")
}

func parseConfig(csv, dateFmt, dateKey string) {
	cfg.dateFormat = dateFmt
	cfg.dateKey = dateKey
	if csv != "" {
		if _, err := os.Stat(csv); os.IsNotExist(err) {
			panic(err)
		}
		cfg.src = &csvSource{path: csv}
	} else {
		cfg.src = &mockSource{}
	}
}

func waitForStopSignal() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop
}

func startHTTPServer() *http.Server {
	mux := bone.New()
	mux.Post("/aggregate", http.HandlerFunc(aggregateHandler))
	handler := cors.Default().Handler(mux)

	srv := &http.Server{
		Addr:    ":8080",
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

func aggregateHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var param aggrParam
	err := decoder.Decode(&param)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	aggrOut := aggregate(param)
	defer closeFlow()

	results := []map[string]interface{}{}
	for item := range aggrOut {
		results = append(results, item)
	}

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
