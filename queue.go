package main

import (
	"database/sql/driver"
	"fmt"
	"log"
	"sync"
)

var primaryQueue *Queue

func init() {
	primaryQueue = newQueue()
}

type Queue struct {
	Processor         *Processor
	Transactions      []*Transaction
	Queries           []Query
	TransactionsMutex sync.Mutex
	QueryMutex        sync.Mutex
}

func newQueue() *Queue {
	return &Queue{
		Transactions:      []*Transaction{},
		TransactionsMutex: sync.Mutex{},
		Queries:           []Query{},
		QueryMutex:        sync.Mutex{},
	}
}

func (q *Queue) AddTransaction(transaction *Transaction) {
	q.TransactionsMutex.Lock()
	q.Transactions = append(q.Transactions, transaction)
	log.Printf("Adding transaction id %s at %f to queue, length is now %d", transaction.Id, transaction.Queries[0].Score, len(q.Transactions))
	q.TransactionsMutex.Unlock()
}

func (q *Queue) RemoveUnstartedTransactions() {
	q.TransactionsMutex.Lock()
	out := []*Transaction{}
	for _, transaction := range q.Transactions {
		if transaction.Started {
			out = append(out, transaction)
		}
	}
	q.Transactions = out
	q.TransactionsMutex.Unlock()
}

func (q *Queue) FlushTransactions() {
	q.TransactionsMutex.Lock()
	if len(q.Transactions) > 0 {
		log.Printf("Draining transaction queue (%d)", len(q.Transactions))
		if q.Transactions[0].Complete {
			transaction := q.Transactions[0]
			q.Transactions = q.Transactions[1:]
			q.QueryMutex.Lock()
			q.Queries = append(q.Queries, transaction.Queries...)
			q.QueryMutex.Unlock()
			// Does this work?
			defer q.FlushTransactions()
		} else {
			log.Printf("Incomplete current transaction %s", q.Transactions[0])
		}
	}
	q.TransactionsMutex.Unlock()
}

func (q *Queue) FlushQueries() error {
	q.QueryMutex.Lock()
	if len(q.Queries) > 0 {

		log.Printf("Draining query queue (%d)", len(q.Queries))

		query := q.Queries[0]

		binds, err := query.FilteredBinds()
		if err != nil {
			log.Fatal(err)
		}

		connection := q.Processor.GetConnection(query.ConnectionId)
		log.Printf(
			"Running query %f on connection %d against postgres %s, with binds %s",
			query.Score,
			connection.Id,
			query.Sql,
			binds,
		)

		rows, err := connection.Conn.Query(query.Sql, binds...)
		if err != nil {
			if err == driver.ErrBadConn {
				log.Printf("Postgres connection error (will reconnect): %s", err)
				connection = newConnection(connection.Id)
				log.Printf("Created new connection")
				q.Processor.SetConnection(connection.Id, connection)
				log.Printf("Flushing queries again")
				q.QueryMutex.Unlock()
				q.FlushQueries()
				return nil
			} else {
				if len(q.Queries) > 1 {
					nextQuery := q.Queries[1]
					log.Printf(
						"Error info: Next Query (%s): %s",
						nextQuery.Score,
						nextQuery.Sql,
					)
				}
				log.Fatal(fmt.Errorf("Postgres error: %s", err))
			}
		}
		rows.Close()

		q.Queries = q.Queries[1:]
		defer q.FlushQueries()
	}
	q.QueryMutex.Unlock()
	return nil
}
