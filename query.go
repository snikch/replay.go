package main

import "regexp"

type Query struct {
	Binds         []interface{} `json:"binds"`
	ConnectionId  int64         `json:"connection_id"`
	Score         float64       `json:"score"`
	Sql           string        `json:"sql"`
	TransactionId string        `json:"transaction_id"`
}

// TransactionStart returns true if the query marks the start of a transaction
func (query Query) TransactionStart() bool {
	exp := regexp.MustCompile("/^\\sBEGIN/i")
	return exp.Match([]byte(query.Sql))
}

// TransactionEnd marks the commit or rollback of a transaction.
// This does not handle anything beyond a basic transaction.
func (query Query) TransactionEnd() bool {
	exp := regexp.MustCompile("/^\\s[COMMIT|ROLLBACK]/i")
	return exp.Match([]byte(query.Sql))
}

func (query Query) Queue() error {
	return nil

}
