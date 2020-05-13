package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
	"gopkg.in/inf.v0"
	"math/rand"
	"sync"
	"time"
)

func recover1() error {
	return nil
}

func pay1(stmt *gocql.Query, rand *rand.Rand) error {
	bic := gocql.TimeUUID().String()
	ban := gocql.TimeUUID().String()
	balance := inf.NewDec(rand.Int63n(100000), 0)
	stmt.Bind(bic, ban, balance)
	if err := stmt.Exec(); err != nil {
		llog.Fatalf("%v", err)
	}
	return nil
}

func pay(cmd *cobra.Command, args []string) error {

	fmt.Printf("Inside pay with args: %v\n", args)
	cluster := gocql.NewCluster("localhost")
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	cluster.Timeout, _ = time.ParseDuration("30s")
	cluster.Keyspace = "lightest"
	cluster.Consistency = gocql.One

	llog.Printf("Establishing connection to the cluster")
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	worker := func(id int, wg *sync.WaitGroup) {

		defer wg.Done()

		rand := rand.New(rand.NewSource(time.Now().UnixNano()))
		stmt := session.Query(INSERT_ACCOUNT)
		stmt.Consistency(gocql.One)
		for i := 0; i < 10000; i++ {
			pay1(stmt, rand)
		}

	}
	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go worker(i, &wg)
	}

	wg.Wait()

	llog.Printf("Done")

	return nil
}
