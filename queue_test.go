package main

import "testing"

func TestQueueDeleteTransaction(t *testing.T) {
	t1 := newTransaction("one")
	t2 := newTransaction("two")
	t3 := newTransaction("three")

	q := newQueue()
	q.AddTransaction(t1)
	q.AddTransaction(t2)
	q.AddTransaction(t3)
	q.RemoveUnstartedTransactions()

	if len(q.Transactions) != 2 {
		t.Errorf("Expected transactions length to be 2, got %d", len(q.Transactions))
	}

	if q.Transactions[0].Id != t1.Id {
		t.Errorf("Expected first transaction to be id %s, got %s", t1.Id, q.Transactions[0].Id)
	}

	if q.Transactions[1].Id != t3.Id {
		t.Errorf("Expected first transaction to be id %s, got %s", t3.Id, q.Transactions[1].Id)
	}

}
