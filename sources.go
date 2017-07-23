package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
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

	lineFeeds := make(chan []string)
	var wg sync.WaitGroup
	headers := []string{}
	go func() {
		lineCount := 0
		reader := csv.NewReader(file)
		for {
			line, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Printf("error reading record at line %d, %v", lineCount, err)
				continue
			}

			if lineCount == 0 {
				headers = append(headers, line...)
			} else {
				lineFeeds <- line

			}
			lineCount++
		}
		close(lineFeeds)
	}()

	wg.Add(cfg.concurRead)
	for i := 0; i < cfg.concurRead; i++ {
		go func() {
			defer wg.Done()
			for line := range lineFeeds {
				record := map[string]interface{}{}
				for i, v := range line {
					record[headers[i]] = v
				}
				out <- record
			}
		}()
	}

	wg.Wait()
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
