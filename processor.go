package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis"
)

const MAX_QUERIES_PER_GET = 20

type Processor struct {
	Connections map[int64]*Connection
	start       float64
	lastScore   float64
	stopChan    chan bool
}

func newProcessor(start float64) *Processor {
	return &Processor{
		Connections: map[int64]*Connection{},
		start:       start,
		lastScore:   start,
		stopChan:    make(chan bool),
	}
}

func (p *Processor) Run() error {
	t1 := time.NewTimer(0 * time.Second)
	t2 := time.NewTimer(0 * time.Second)
	t3 := time.NewTimer(0 * time.Second)

LOOP:
	for {
		select {
		case <-p.stopChan:
			break LOOP
		case <-t1.C:
			queries, err := p.GetQueries()
			if err != nil {
				return err
			}
			log.Printf("Found %d queries", len(queries))
			err = p.ProcessQueries(queries)
			if err != nil {
				return err
			}
			// Immediately continue if we had queries
			if len(queries) > 0 {
				t1.Reset(50 * time.Millisecond)
			} else {
				t1.Reset(1 * time.Second)
			}
			break
		case <-t2.C:
			primaryQueue.FlushTransactions()
			t2.Reset(50 * time.Millisecond)
		case <-t3.C:
			primaryQueue.FlushQueries()
			t3.Reset(50 * time.Millisecond)
		}
	}
	return nil
}

func (p *Processor) Stop() {
	p.stopChan <- true
}

func (p *Processor) ProcessQueries(queries []Query) error {
	for _, query := range queries {
		err := p.GetConnection(query.ConnectionId).ProcessQuery(query)
		if err != nil {
			return err
		}
		log.Printf("Added %s at %f for processing\n", query.TransactionId, query.Score)
		// Avoid any truncation issues on the score by ensuring it is rounded up.
		p.lastScore = query.Score + 0.0000006
	}
	return nil
}

func (p *Processor) GetQueries() ([]Query, error) {
	score := redis.ZRangeByScore{
		Min:   fmt.Sprintf("%f", p.lastScore),
		Max:   "+inf",
		Count: MAX_QUERIES_PER_GET,
	}
	results, err := Redis.ZRangeByScore("sql-log", score).Result()
	if err != nil {
		return nil, err
	}

	queries := []Query{}
	for _, result := range results {
		query := Query{}
		err := json.Unmarshal([]byte(result), &query)
		if err != nil {
			return nil, err
		}
		queries = append(queries, query)
	}
	return queries, nil
}

func (p *Processor) GetConnection(connection_id int64) *Connection {
	connection, ok := p.Connections[connection_id]
	if !ok {
		connection = newConnection(connection_id)
		p.Connections[connection_id] = connection
	}
	return connection
}

func (p *Processor) SetConnection(connection_id int64, connection *Connection) {
	p.Connections[connection_id] = connection
}
