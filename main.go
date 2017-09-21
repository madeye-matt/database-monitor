package main

import (
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"time"
	"strings"
)

const (
    splunkDateFormat string = "01/02/2006 15:04:05 -0700"
	commandLineDateFormat string = "2006-01-02 15:04:05.000"
)

var (
	configFile = flag.String("c", "", "location of configuration json file")
	timestamp = flag.String("t", "", "earliest time to retrieve records for (where appropriate)")
)

func checkError(e error) {
	if e != nil {
		log.Fatalf("Error: %s\n", e)
	}
}

func printMap(timestamp time.Time, m map[string]interface{}, spaceReplacement string) {
	fmt.Printf("[%s] ", timestamp.Format(splunkDateFormat))
	for key, value := range m {
		key := strings.Replace(key, " ", spaceReplacement, -1)
		fmt.Printf("%s=\"%v\" ", key, value)
	}
	fmt.Printf("\n")
}

func parseTimestamp(timeStr string) time.Time {
	time, err := time.Parse(commandLineDateFormat, timeStr)
	checkError(err)

	return time
}

func main() {
	flag.Parse()
	logFile := initLogging()
	defer logFile.Close()
	config, err := loadConfig(*configFile)
	checkError(err)

	filterTime := time.Time{}

	if len(*timestamp) > 0 {
		filterTime = parseTimestamp(*timestamp)
	}

	dbConfig := config.DatabaseConfig
	db := initDb(dbConfig)
	defer db.Close()

	for _, query := range config.Monitoring {
		currentTime := time.Now()

		log.Printf("Running monitoring query: %s", query)
		var r ResultHandler

		if !query.RollUp {
			r = new(DefaultResultHandler)
		} else {
			r = NewRolledUpResultHandler()
		}

		if query.TimeFilter {
			executeQueryWithTimeFilter(db, query, &r, filterTime)
		} else {
			executeQuery(db, query, &r)
		}

		result := r.GetMap()
		log.Printf("result: %v\n", result)

		for _, m := range result {
			printMap(currentTime, m, config.SpaceReplacement)
		}
	}
}
