package main

import (
	//"log"
	"fmt"
	"database/sql"
	"time"
	"sync"
	"log"
)

func getConnectionParameters(dbConfig DatabaseConfig) string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s", dbConfig.Host, dbConfig.Port, dbConfig.Database, dbConfig.Username, dbConfig.Password, dbConfig.SSLMode)
}

func initDb(dbConfig DatabaseConfig) *sql.DB {
	connectionParams := getConnectionParameters(dbConfig)

	db, err := sql.Open("postgres", connectionParams)
	checkError(err)

	return db
}

func executeQuery(wg *sync.WaitGroup, db *sql.DB, query Query, resultHandler *ResultHandler) {
	log.Printf("start: executeQuery")
	defer wg.Done()
	executeQueryCore(db, query, resultHandler, func(string) (*sql.Rows, error) { return db.Query(query.SQL) })
	log.Printf("end: executeQuery")
}

func executeQueryWithTimeFilter(wg *sync.WaitGroup, db *sql.DB, query Query, resultHandler *ResultHandler, time time.Time) {
	log.Printf("start: executeQueryWithTimeFilter")
	defer wg.Done()
	executeQueryCore(db, query, resultHandler, func(string) (*sql.Rows, error) { return db.Query(query.SQL, time) })
	log.Printf("end: executeQueryWithTimeFilter")
}

func executeQueryCore(db *sql.DB, query Query, resultHandler *ResultHandler, queryFunc func (string) (*sql.Rows, error)){
	var rows *sql.Rows
	var err error

	rows, err = queryFunc(query.SQL)

	checkError(err)

	var cols []string
	cols, err = rows.Columns()
	checkError(err)

	numColumns := len(cols)
	//rollUp := query.RollUp && numColumns == 2

	for rows.Next() {
		//log.Printf("resultHandler(before): %v\n", *resultHandler)
		columns := make([]interface{}, numColumns)
		columnPointers := make([]interface{}, numColumns)

		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		err = rows.Scan(columnPointers...)
		checkError(err)

		(*resultHandler).HandleResult(cols, columnPointers)
		//log.Printf("resultHandler: %v\n", *resultHandler)
	}

	(*resultHandler).Finalise()
}

