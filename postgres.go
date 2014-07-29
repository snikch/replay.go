package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

var Postgres *sql.DB

func init() {
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		url = "postgres://"
	}

	fmt.Println("Postgres will connect to", url)

	db, err := sql.Open("postgres", url)
	if err != nil {
		log.Fatal(err)
	}

	Postgres = db
}
