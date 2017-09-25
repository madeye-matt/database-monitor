package main

import (
	"io/ioutil"
	"encoding/json"
	"os"
	"log"
)

type DatabaseConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	SSLMode  string
}

type Query struct {
	SQL    string
	RollUp bool
	TimeFilter bool
	TimeStampColumn string
	TimeStampFormat string
}

type Config struct {
	DatabaseConfig DatabaseConfig
	Monitoring     []Query
	SpaceReplacement     string
}

func loadConfig(filename string) (Config, error) {
	var config Config
	configData, err := ioutil.ReadFile(filename)

	if err != nil {
		return config, err
	}

	if err = json.Unmarshal(configData, &config); err != nil {
		return config, err
	}

	return config, nil
}

func initLogging() *os.File {
	f, err := os.OpenFile("database-monitor.log", os.O_CREATE|os.O_RDWR, 0666)
	checkError(err)

	// assign it to the standard logger
	log.SetOutput(f)

	return f
}

