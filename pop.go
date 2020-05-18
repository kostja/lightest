package main

import (
	"fmt"
	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
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
	cluster.Consistency = gocql.One

	llog.Printf("Establishing connection to the cluster")
	session, err := cluster.CreateSession()
	if err != nil {
		return merry.Wrap(err)
	}
	llog.Printf("Creating the keyspace and tables")
	if err = session.Query(DROP_KS).Exec(); err != nil {
		return merry.Wrap(err)
	}
	if err = session.Query(CREATE_KS).Exec(); err != nil {
		return merry.Wrap(err)
	}
	cluster.Keyspace = "lightest"
	session, err = cluster.CreateSession()
	if err != nil {
		return merry.Wrap(err)
	}
	if err = session.Query(CREATE_ACCOUNTS_TAB).Exec(); err != nil {
		return merry.Wrap(err)
	}
	if err = session.Query(CREATE_TRANSFERS_TAB).Exec(); err != nil {
		return merry.Wrap(err)
	}
	llog.Printf("Creating a worker pool")

	worker := func(id int, n_accounts int, wg *sync.WaitGroup) {

		defer wg.Done()

		llog.Printf("Worker %d starting\n", id)

		var rand FixedRandomSource
		rand.Init()

		stmt := session.Query(INSERT_ACCOUNT)
		stmt.Consistency(gocql.One)
		llog.Printf("Inserting %d accounts", n_accounts)
		for i := 0; i < n_accounts; i++ {
			bic, ban := rand.NewBicAndBan()
			balance := rand.NewStartBalance()
			stmt.Bind(bic, ban, balance)
			if err = stmt.Exec(); err != nil {
				llog.Fatalf("%+v", err)
			}
		}

		llog.Printf("Worker %d done\n", id)
	}
	var wg sync.WaitGroup

	accounts_per_worker := n / workers
	remainder := n - accounts_per_worker*workers

	for i := 0; i < workers; i++ {
		n_accounts := accounts_per_worker
		if i < remainder {
			n_accounts++
		}
		wg.Add(1)
		go worker(i+1, n_accounts, &wg)
	}

	wg.Wait()

	llog.Printf("Done")

	return nil
}
