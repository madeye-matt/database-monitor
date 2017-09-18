package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"os"
	"time"
	"strings"
)

const splunkDateFormat string = "01/02/2006 15:04:05 -0700"

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
}

type Config struct {
	DatabaseConfig DatabaseConfig
	Monitoring     []Query
	SpaceReplacement     string
}

type ResultHandler interface {
	HandleResult(cols []string, columnPointers []interface{})
	GetMap() []map[string]interface{}
}

type DefaultResultHandler struct {
	result []map[string]interface{}
}

func (r *DefaultResultHandler) HandleResult(cols []string, columnPointers []interface{}) {
	log.Printf("r.result(before): %v\n", r.result)
	log.Printf("cols: %v\n", cols)
	log.Printf("columnPointers: %v\n", columnPointers)
	m := make(map[string]interface{})

	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		log.Printf("%s=%v\n", colName, *val)
		m[colName] = *val
	}

	r.result = append(r.result, m)
	log.Printf("r.result: %v\n", r.result)
}

func (r *DefaultResultHandler) GetMap() []map[string]interface{} {
	return r.result
}

type RolledUpResultHandler struct {
	result map[string]interface{}
}

func NewRolledUpResultHandler() *RolledUpResultHandler {
	r := new(RolledUpResultHandler)

	r.result = make(map[string]interface{})

	return r
}

func (r *RolledUpResultHandler) HandleResult(cols []string, columnPointers []interface{}) {
	if len(cols) != 2 {
		log.Fatal("Must have only 2 result columns to use RolledUpResultHandler (", len(cols), " found)")
	}
	key := columnPointers[0].(*interface{})
	val := columnPointers[1].(*interface{})

	keyStr := fmt.Sprintf("%v", *key)

	r.result[keyStr] = *val
}

func (r *RolledUpResultHandler) GetMap() []map[string]interface{} {
	var newResult []map[string]interface{}

	return append(newResult, r.result)
}

var configFile = flag.String("c", "", "location of configuration json file")

func checkError(e error) {
	if e != nil {
		log.Fatalf("Error: %s\n", e)
	}
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
	f, err := os.OpenFile("database-monitor.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	checkError(err)

	// assign it to the standard logger
	log.SetOutput(f)

	return f
}

func getConnectionParameters(dbConfig DatabaseConfig) string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s", dbConfig.Host, dbConfig.Port, dbConfig.Database, dbConfig.Username, dbConfig.Password, dbConfig.SSLMode)
}

func initDb(dbConfig DatabaseConfig) *sql.DB {
	connectionParams := getConnectionParameters(dbConfig)

	db, err := sql.Open("postgres", connectionParams)
	checkError(err)

	return db
}

func executeQuery(db *sql.DB, query Query, resultHandler *ResultHandler) {
	var rows *sql.Rows
	rows, err := db.Query(query.SQL)
	checkError(err)

	var cols []string
	cols, err = rows.Columns()
	checkError(err)

	numColumns := len(cols)
	//rollUp := query.RollUp && numColumns == 2

	for rows.Next() {
		log.Printf("resultHandler(before): %v\n", *resultHandler)
		columns := make([]interface{}, numColumns)
		columnPointers := make([]interface{}, numColumns)

		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		err = rows.Scan(columnPointers...)
		checkError(err)

		(*resultHandler).HandleResult(cols, columnPointers)
		log.Printf("resultHandler: %v\n", *resultHandler)
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
