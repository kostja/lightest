package main

import (
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"gopkg.in/inf.v0"
	"time"
)

func check(prev *inf.Dec) *inf.Dec {

	cluster := gocql.NewCluster("localhost")
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	cluster.Timeout, _ = time.ParseDuration("3000s")
	cluster.Keyspace = "lightest"
	cluster.Consistency = gocql.One

	session, err := cluster.CreateSession()
	defer session.Close()
	if err != nil {
		llog.Fatalf("Failed to run the check: %v")
	}

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
