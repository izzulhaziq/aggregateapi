package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/gwenn/yacr"
)

type source interface {
	read(chan map[string]interface{}) error
}

type csvSource struct {
	path string
}

func (c csvSource) read(out chan map[string]interface{}) error {
	file, err := os.Open(c.path)
	if err != nil {
		err = fmt.Errorf("unable to open csv file %s, %v", c.path, err)
		return err
	}
	defer file.Close()

	var headers []string
	lineCount := 0
	reader := yacr.DefaultReader(file)
	record := map[string]interface{}{}
	i := 0
	for reader.Scan() {
		if lineCount == 0 {
			headers = append(headers, reader.Text())
			if reader.EndOfRecord() {
				lineCount++
			}
		} else {
			record[headers[i]] = reader.Text()
			i++

			if reader.EndOfRecord() {
				out <- record
				i = 0
				lineCount++
				record = map[string]interface{}{}
			}
		}
	}
	return nil
}

type mockSource struct{}

func (s mockSource) read(out chan map[string]interface{}) error {
	for i := 0; i < 365; i++ {
		for j := 0; j < 2; j++ {
			out <- map[string]interface{}{
				"LicenseId":       "license1",
				"BilledProductId": "product" + strconv.Itoa(j),
				"Date":            time.Now().AddDate(0, 0, -i),
				"Value":           rand.Intn(100),
			}
		}
	}
	return nil
}
