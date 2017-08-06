package main

import (
	"io/ioutil"
	"os"

	yaml "gopkg.in/yaml.v2"
)

type yamlSQL struct {
	Host     string `yaml:"host,omitempty"`
	Port     int    `yaml:"port,omitempty"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}

type yamlCsv struct {
	Path string `yaml:"path,omitempty"`
}

type yamlCfg struct {
	Shard      int     `yaml:"shard,omitempty"`
	Partition  int     `yaml:"partition,omitempty"`
	DateField  string  `yaml:"dateField,omitempty"`
	DateFormat string  `yaml:"dateFormat,omitempty"`
	Source     string  `yaml:"source,omitempty"`
	SQLCfg     yamlSQL `yaml:"sql,omitempty"`
	CsvCfg     yamlCsv `yaml:"csv,omitempty"`
}

type config struct {
	shard      int
	partition  int
	src        Source
	dateKey    string
	dateFormat string
}

func (cfg *config) parseYaml(yamlPath string) {
	data, err := ioutil.ReadFile(yamlPath)
	if err != nil {
		panic(err)
	}

	var cfgFile yamlCfg
	err = yaml.Unmarshal([]byte(data), &cfgFile)
	if err != nil {
		panic(err)
	}

	cfg.shard = cfgFile.Shard
	cfg.partition = cfgFile.Partition
	cfg.dateFormat = cfgFile.DateFormat
	cfg.dateKey = cfgFile.DateField
	cfg.configureSrc(cfgFile.Source, cfgFile.CsvCfg.Path, cfgFile.SQLCfg.Host,
		cfgFile.SQLCfg.Username, cfgFile.SQLCfg.Password, cfgFile.SQLCfg.Port)
}

func (cfg *config) parseFlag() {
	cfg.shard = *shard
	cfg.partition = *partition
	cfg.dateFormat = *dateFormat
	cfg.dateKey = *dateKey
	cfg.configureSrc(*srcType, *csv, *sqlhost, *sqlusername, *sqlpassword, *sqlport)
}

func (cfg *config) parse(shard, partition int, dateFmt, dateKey string) {
	cfg.shard = shard
	cfg.partition = partition
	cfg.dateFormat = dateFmt
	cfg.dateKey = dateKey
}

func (cfg *config) configureSrc(srcType, csv, sqlhost, sqlusername, sqlpassword string, sqlport int) {
	switch srcType {
	case "mock":
		cfg.src = &mockSource{}
	case "csv":
		if _, err := os.Stat(csv); os.IsNotExist(err) {
			panic(err)
		}
		cfg.src = &csvSource{csv}
	case "sql":
		cfg.src = &sqlSource{
			sqlusername,
			sqlpassword,
			sqlhost,
			sqlport,
		}
	}
}
