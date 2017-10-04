package main

import (
	"fmt"
	"log"
	"time"
	"regexp"
)

type ResultChannel chan map[string]interface{}

type ResultHandler interface {
	HandleResult(query Query, cols []string, columnPointers []interface{})
	Finalise(query Query)
}

type DefaultResultHandler struct {
	ResultChannel ResultChannel
}

func addTimestampToMap(query Query, m map[string]interface{}) map[string]interface{} {
	var timestamp time.Time
	var err error

	if query.TimeStampColumn != "" {
		var ok bool
		timestamp, ok = m[query.TimeStampColumn].(time.Time)

		if !ok {
			timeStr := m[query.TimeStampColumn].(string)
			timestamp, err = time.Parse(query.TimeStampFormat, timeStr)

			if err != nil {
				log.Printf("Failed to parse timestmap %s", timeStr)
			}
		}

		if err == nil {
			m[timestampColumn] = timestamp
		}
	}

	return m
}

func transformField(fieldValue string, reStr string, output string) string {
	re, err := regexp.Compile(reStr)
	checkError(err)

	return re.ReplaceAllLiteralString(fieldValue, output)
}

func applyTransform(transformer Transformer, result *map[string]interface{}) {
	if transformer.FieldName == "" {
		for k, v := range *result {
			fieldStr, err := v.(string)

			if !err {
				(*result)[k] = transformField(fieldStr, transformer.Regexp, transformer.Output)
			}
			// If there's an error we'll just leave the field alone
		}
	} else {
		(*result)[transformer.FieldName] = transformField((*result)[transformer.FieldName].(string), transformer.Regexp, transformer.Output)
	}
}

func transformResult(transformers []Transformer, result *map[string]interface{}) {
	for _, transformer := range transformers {
		applyTransform(transformer, result)
	}
}

func (r *DefaultResultHandler) HandleResult(query Query, cols []string, columnPointers []interface{}) {
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

	m = addTimestampToMap(query, m)

	transformResult(query.Transforms, &m)

	r.ResultChannel <- m
}

func (r *DefaultResultHandler) Finalise(query Query){ }

type RolledUpResultHandler struct {
	ResultChannel ResultChannel
	result map[string]interface{}
}

func NewRolledUpResultHandler(rc ResultChannel) *RolledUpResultHandler {
	r := &RolledUpResultHandler{ rc, make(map[string]interface{}) }

	return r
}

func (r *RolledUpResultHandler) HandleResult(query Query, cols []string, columnPointers []interface{}) {
	//log.Printf("RolledUpResultHandler.HandleResult")
	if len(cols) != 2 {
		log.Fatal("Must have only 2 result columns to use RolledUpResultHandler (", len(cols), " found)")
	}
	key := columnPointers[0].(*interface{})
	val := columnPointers[1].(*interface{})

	keyStr := fmt.Sprintf("%v", *key)

	r.result[keyStr] = *val
}

func (r *RolledUpResultHandler) Finalise(query Query) {
	r.result = addTimestampToMap(query, r.result)
	log.Printf("r.result: %v", r.result)

	transformResult(query.Transforms, &r.result)

	r.ResultChannel <- r.result
}

