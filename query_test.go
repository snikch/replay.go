package main

import "testing"

func TestQueryTransactionBeginTrue(t *testing.T) {
	q := Query{
		Sql: "BEGIN",
	}
	if !q.TransactionBegin() {
		t.Errorf("Expected TransactionBegin for %s to be true", q.Sql)
	}
}

func TestQueryTransactionBeginFalse(t *testing.T) {
	q := Query{
		Sql: "NOT BEGIN",
	}
	if q.TransactionBegin() {
		t.Errorf("Expected TransactionBegin for %s to be false", q.Sql)
	}
}

func TestQueryTransactionCommitTrue(t *testing.T) {
	q := Query{
		Sql: "COMMIT",
	}
	if !q.TransactionCommit() {
		t.Errorf("Expected TransactionCommit for %s to be true", q.Sql)
	}
}

func TestQueryTransactionCommitFalse(t *testing.T) {
	q := Query{
		Sql: "NOT COMMIT ",
	}
	if q.TransactionCommit() {
		t.Errorf("Expected TransactionCommit for %s to be false", q.Sql)
	}
}

func TestQueryTransactionRollbackTrue(t *testing.T) {
	q := Query{
		Sql: "ROLLBACK",
	}
	if !q.TransactionRollback() {
		t.Errorf("Expected TransactionRollback for %s to be true", q.Sql)
	}
	q.Sql = "  ROLLBACK"
	if !q.TransactionRollback() {
		t.Errorf("Expected TransactionRollback for %s to be true", q.Sql)
	}
}
