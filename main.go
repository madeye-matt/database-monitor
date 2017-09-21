package main

import (
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"time"
	"strings"
)

const splunkDateFormat string = "01/02/2006 15:04:05 -0700"

var configFile = flag.String("c", "", "location of configuration json file")

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

func main() {
	flag.Parse()
	logFile := initLogging()
	defer logFile.Close()
	config, err := loadConfig(*configFile)
	checkError(err)

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

		executeQuery(db, query, &r)

		result := r.GetMap()
		log.Printf("result: %v\n", result)

		for _, m := range result {
			printMap(currentTime, m, config.SpaceReplacement)
		}
	}
}
