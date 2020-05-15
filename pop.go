package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
	"gopkg.in/inf.v0"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

func populate(cmd *cobra.Command, n int) error {

	var cores = runtime.NumCPU()
	var workers = 4 * cores
	if workers > n {
		workers = n
	}
	fmt.Printf("Creating %d accounts using %d workers on %d cores \n", n, workers, cores)

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
	llog.Printf("Creating the keyspace and tables")
	if err = session.Query(DROP_KS).Exec(); err != nil {
		return err
	}
	if err = session.Query(CREATE_KS).Exec(); err != nil {
		return err
	}
	if err = session.Query(CREATE_ACCOUNTS_TAB).Exec(); err != nil {
		return err
	}
	if err = session.Query(CREATE_TRANSFERS_TAB).Exec(); err != nil {
		return err
	}
	llog.Printf("Creating a worker pool")

	worker := func(id int, wg *sync.WaitGroup) {

		defer wg.Done()

		llog.Printf("Worker %d starting\n", id)

		rand := rand.New(rand.NewSource(time.Now().UnixNano()))
		stmt := session.Query(INSERT_ACCOUNT)
		stmt.Consistency(gocql.One)
		llog.Printf("Inserting accounts")
		for i := 0; i < n/workers; i++ {
			bic := gocql.TimeUUID().String()
			ban := gocql.TimeUUID().String()
			balance := inf.NewDec(rand.Int63n(100000), 0)
			stmt.Bind(bic, ban, balance)
			if err = stmt.Exec(); err != nil {
				llog.Fatalf("%v", err)
			}
		}

		llog.Printf("Worker %d done\n", id)
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
