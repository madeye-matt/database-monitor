package main

import (
	"fmt"
	"log"
)

type ResultChannel chan map[string]interface{}

type ResultHandler interface {
	HandleResult(cols []string, columnPointers []interface{})
	Finalise()
}

type DefaultResultHandler struct {
	ResultChannel ResultChannel
}

func (r *DefaultResultHandler) HandleResult(cols []string, columnPointers []interface{}) {
	//log.Printf("DefaultResultHandler.HandleResult")
	//log.Printf("r.result(before): %v\n", r.result)
	//log.Printf("cols: %v\n", cols)
	//log.Printf("columnPointers: %v\n", columnPointers)
	m := make(map[string]interface{})

	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		//log.Printf("%s=%v\n", colName, *val)
		m[colName] = *val
	}

	r.ResultChannel <- m
}

func (r *DefaultResultHandler) Finalise(){ }

type RolledUpResultHandler struct {
	ResultChannel ResultChannel
	result map[string]interface{}
}

func NewRolledUpResultHandler(rc ResultChannel) *RolledUpResultHandler {
	r := &RolledUpResultHandler{ rc, make(map[string]interface{}) }

	return r
}

func (r *RolledUpResultHandler) HandleResult(cols []string, columnPointers []interface{}) {
	//log.Printf("RolledUpResultHandler.HandleResult")
	if len(cols) != 2 {
		log.Fatal("Must have only 2 result columns to use RolledUpResultHandler (", len(cols), " found)")
	}
	key := columnPointers[0].(*interface{})
	val := columnPointers[1].(*interface{})

	keyStr := fmt.Sprintf("%v", *key)

	r.result[keyStr] = *val
}

func (r *RolledUpResultHandler) Finalise() {
	log.Printf("r.result: %v", r.result)
	r.ResultChannel <- r.result
}

