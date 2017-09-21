package main

import (
	"fmt"
	"log"
)

type ResultHandler interface {
	HandleResult(cols []string, columnPointers []interface{})
	GetMap() []map[string]interface{}
}

type DefaultResultHandler struct {
	result []map[string]interface{}
}

func (r *DefaultResultHandler) HandleResult(cols []string, columnPointers []interface{}) {
	//log.Printf("r.result(before): %v\n", r.result)
	//log.Printf("cols: %v\n", cols)
	//log.Printf("columnPointers: %v\n", columnPointers)
	m := make(map[string]interface{})

	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		//log.Printf("%s=%v\n", colName, *val)
		m[colName] = *val
	}

	r.result = append(r.result, m)
	//log.Printf("r.result: %v\n", r.result)
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

