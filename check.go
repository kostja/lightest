package main

import (
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

func check(session *gocql.Session, prev *inf.Dec) *inf.Dec {

	iter := session.Query(CHECK_BALANCE).Iter()
	var sum *inf.Dec

	iter.Scan(&sum)
	if err := iter.Close(); err != nil {
		llog.Fatalf("Failed to fetch the sum: %v", err)
	}

	if prev != nil && prev.Cmp(sum) != 0 {
		llog.Fatalf("Check balance mismatch:\nbefore: %v\nafter:  %v", prev, sum)
	}
	return sum
}
