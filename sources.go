package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"
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

	lineCount, headers := 0, []string{}
	reader := csv.NewReader(file)
	for {
		records, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			err = fmt.Errorf("error reading record at line %d, %v", lineCount, err)
			return err
		}

		if lineCount == 0 {
			headers = append(headers, records...)
		} else {
			record := map[string]interface{}{}
			for i, v := range records {
				record[headers[i]] = v
			}
			out <- record
		}
		lineCount++
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
