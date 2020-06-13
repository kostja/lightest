package main

import (
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"gopkg.in/inf.v0"
	"time"
)

func check(settings *Settings, prev *inf.Dec) *inf.Dec {

	cluster := gocql.NewCluster(settings.host)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: settings.user,
		Password: settings.password,
	}
	cluster.Timeout, _ = time.ParseDuration("3000s")
	cluster.Keyspace = "lightest"
	cluster.Consistency = gocql.One

	session, err := cluster.CreateSession()
	defer session.Close()
	if err != nil {
		llog.Fatalf("Failed to run the check: %v", err)
	}
	// Only persist the balance if it is not persisted yet
	// Only calculate the balance if it's necessary to persist
	// it, or it is necessary for a check (prev != nil)
	var sum *inf.Dec
	var iter *gocql.Iter
	persistBalance := false

	if prev == nil {
		iter = session.Query(FETCH_TOTAL).Iter()
		if iter.NumRows() == 0 {
			if err := iter.Close(); err != nil {
				llog.Fatalf("Failed to fetch the stored total: %v", err)
			}
			iter = nil
			persistBalance = true
		}
	}
	if iter == nil {
		llog.Infof("Calculating the total balance...")
		iter = session.Query(CHECK_BALANCE).Iter()
	}
	iter.Scan(&sum)

	if err := iter.Close(); err != nil {
		llog.Fatalf("Failed to calculate the total: %v", err)
	}

	if prev != nil {
		if prev.Cmp(sum) != 0 {
			llog.Fatalf("Check balance mismatch:\nbefore: %v\nafter:  %v", prev, sum)
		}
	}
	if persistBalance {
		// Do not overwrite the total balance if it is already persisted.
		llog.Infof("Persisting the total balance...")
		if err := session.Query(PERSIST_TOTAL).Bind(sum).Exec(); err != nil {
			llog.Fatalf("Failed to persist total balance: %v", sum)
		}
	}

	return sum
}
