package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-zoo/bone"
)

type aggrParam struct {
	GroupBy  []string `json:"groupBy"`
	Interval string   `json:"interval"`
}

type aggrRes struct {
	TimeSlice []string `json:"timeSlice"`
	Values    []int64  `json:"values"`
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

	result := &aggrRes{
		TimeSlice: []string{param.Interval},
		Values:    []int64{20},
	}

	w.Header().Set("content-type", "application/json")
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(result); err != nil {
		log.Println("unable to encode result")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
