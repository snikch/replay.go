package main

import "testing"

func TestQueryTransactionStartTrue(t *testing.T) {
	q := Query{
		Sql: "BEGIN",
	}
	if !q.TransactionStart() {
		t.Errorf("Expected TransactionStart for %s to be true", q.Sql)
	}
}

func TestQueryTransactionStartFalse(t *testing.T) {
	q := Query{
		Sql: "NOT BEGIN",
	}
	if q.TransactionStart() {
		t.Errorf("Expected TransactionStart for %s to be false", q.Sql)
	}
}

func TestQueryTransactionEndTrue(t *testing.T) {
	q := Query{
		Sql: "COMMIT",
	}
	if !q.TransactionEnd() {
		t.Errorf("Expected TransactionEnd for %s to be true", q.Sql)
	}
	q.Sql = "ROLLBACK"
	if !q.TransactionEnd() {
		t.Errorf("Expected TransactionEnd for %s to be true", q.Sql)
	}
	q.Sql = "  ROLLBACK"
	if !q.TransactionEnd() {
		t.Errorf("Expected TransactionEnd for %s to be true", q.Sql)
	}
}

func TestQueryTransactionEndFalse(t *testing.T) {
	q := Query{
		Sql: "NOT ROLLBACK OR COMMIT",
	}
	if q.TransactionEnd() {
		t.Errorf("Expected TransactionEnd for %s to be false", q.Sql)
	}
}
