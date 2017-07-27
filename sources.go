package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/gwenn/yacr"
)

// Source is the interface that wraps the source of aggregation that will Read
// data based on the specified fields []string into the channel map[string]interface{}.
type Source interface {
	Read(fields, chan map[string]interface{}) error
}

type fields []string

type csvSource struct {
	path string
}

func (c *csvSource) Read(flds fields, out chan map[string]interface{}) error {
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
			if flds.contain(headers[i]) {
				record[headers[i]] = reader.Text()
			}
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

func (f fields) contain(header string) bool {
	for i := 0; i < len(f); i++ {
		if f[i] == header {
			return true
		}
	}
	return false
}

type mockSource struct{}

func (s *mockSource) Read(f fields, out chan map[string]interface{}) error {
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
