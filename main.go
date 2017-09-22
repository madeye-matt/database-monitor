package main

import (
	"flag"
	"fmt"
	"regexp"
	_ "github.com/lib/pq"
	"log"
	"time"
	"strings"
	"strconv"
)

const (
    splunkDateFormat string = "01/02/2006 15:04:05 -0700"
	commandLineDateFormat string = "2006-01-02 15:04:05.000"
)

var (
	configFile = flag.String("c", "", "location of configuration json file")
	timestamp = flag.String("t", "", "earliest time to retrieve records for (where appropriate)")
	periodRegexp = regexp.MustCompile("^([0-9]+)([smhdMy])$")
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

func getTimeFactor(units string) int {
	switch units {
	case "s":
		return 1
	case "m":
		return 60
	case "h":
		return 60*60
	case "d":
		return 60*60*24
	case "y":
		return 60*60*24*365
	default:
		log.Fatal("Unknown duration units %s", units)
	}

	return 0
}

/*
	Time can either be represented according to the commandLineDateFormat defined as a constant or as a time period
	defined as follows:
	[0-9]+[smhdMy] representing a number of seconds, minutes, hours, days, months and years
 */
func parseTimestamp(timeStr string) time.Time {
	timeVal, err := time.Parse(commandLineDateFormat, timeStr)

	if err != nil {
		matches := periodRegexp.FindAllStringSubmatch(timeStr, -1)

		if matches != nil && len(matches) > 0 {
			amountStr := matches[0][1]
			timeAmount, err := strconv.Atoi(amountStr)
			checkError(err)
			timeUnits := matches[0][2]
			timeFactor := getTimeFactor(timeUnits)
			durationSecs := timeFactor * timeAmount

			log.Printf("duration: %d", durationSecs)

			timeVal = time.Now().Add(time.Duration(-1 * durationSecs) * time.Second)

			log.Printf("startTime: %s", timeVal)
		}
	}

	return timeVal
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
		//log.Printf("result: %v\n", result)

		for _, m := range result {
			printMap(currentTime, m, config.SpaceReplacement)
		}
	}
}
