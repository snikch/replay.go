package main

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
)

type Query struct {
	Binds         []interface{} `json:"binds"`
	ConnectionId  int64         `json:"connection_id"`
	Score         float64       `json:"score"`
	Sql           string        `json:"sql"`
	TransactionId string        `json:"transaction_id"`
}

// TransactionBegin returns true if the query marks the start of a transaction
func (query Query) TransactionBegin() bool {
	exp := regexp.MustCompile("^\\s*BEGIN")
	return exp.MatchString(query.Sql)
}

// TransactionCommit marks the commit of a transaction.
// This does not handle anything beyond a basic transaction.
func (query Query) TransactionCommit() bool {
	exp := regexp.MustCompile("^\\s*COMMIT")
	return exp.MatchString(query.Sql)
}

// TransactionRollback marks the commit or rollback of a transaction.
// This does not handle anything beyond a basic transaction.
func (query Query) TransactionRollback() bool {
	exp := regexp.MustCompile("^\\s*ROLLBACK")
	return exp.MatchString(query.Sql)
}

func (q Query) FilteredBinds() ([]interface{}, error) {
	out := []interface{}{}
	for _, bind := range q.Binds {
		kind := reflect.ValueOf(bind).Kind()
		if kind == reflect.Slice {
			parts := bind.([]interface{})
			stringParts := []string{}
			for _, part := range parts {
				switch v := part.(type) {
				case string:
					stringParts = append(stringParts, fmt.Sprintf("\"%s\"", v))
				case int:
					stringParts = append(stringParts, fmt.Sprintf("%d", v))
				case float64:
					stringParts = append(stringParts, fmt.Sprintf("%f", v))
				default:
					return nil, fmt.Errorf("Didn't know what %s is", v)
				}
			}
			bind = fmt.Sprintf("{%s}", strings.Join(stringParts, ","))
		} else if kind == reflect.Float64 {
			// Change whole numbers into Int type
			val := bind.(float64)
			rem := math.Remainder(val, 1.0)
			if rem == 0.0 {
				newBind := int(val)
				bind = newBind
			}
		}
		out = append(out, bind)
	}
	return out, nil
}
