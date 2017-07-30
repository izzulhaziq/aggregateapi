package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gwenn/yacr"
)

// Source is the interface that wraps the source of aggregation that will Read
// data based on the specified fields []string into the channel map[string]interface{}.
// If the source support query, it should handle the query given by 1st parameter.
type Source interface {
	Read(string, fields, chan map[string]interface{}) error
}

type csvSource struct {
	path string
}

func (c *csvSource) Read(q string, flds fields, out chan map[string]interface{}) error {
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

type fields []string

func (f fields) contain(header string) bool {
	for i := 0; i < len(f); i++ {
		if f[i] == header {
			return true
		}
	}
	return false
}

type mockSource struct{}

func (s *mockSource) Read(q string, f fields, out chan map[string]interface{}) error {
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

type sqlSource struct {
	username string
	password string
	hostname string
	port     int
}

func (s *sqlSource) connString() string {
	query := url.Values{}
	query.Add("connection timeout", fmt.Sprintf("%d", 30))
	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(s.username, s.password),
		Host:     fmt.Sprintf("%s:%d", s.hostname, s.port),
		RawQuery: query.Encode(),
	}
	return u.String()
}

func (s *sqlSource) Read(q string, f fields, out chan map[string]interface{}) error {
	connString := s.connString()
	conn, err := sql.Open("mssql", connString)
	if err != nil {
		return err
	}
	defer conn.Close()

	rows, err := conn.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return err
		}
		var value string
		var record map[string]interface{}
		for i, col := range values {
			if f.contain(columns[i]) {
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}
				record[columns[i]] = value
			}
		}
		out <- record
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return nil
}
