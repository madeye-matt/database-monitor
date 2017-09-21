package main

import (
	"log"
	"fmt"
	"database/sql"
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

