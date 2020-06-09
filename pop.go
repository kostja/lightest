package main

import (
	"fmt"
	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"runtime"
	"sync"
	"time"
)

func populate(settings *Settings) error {

	fmt.Printf("Creating %d accounts using %d workers on %d cores \n",
		settings.count, settings.workers,
		runtime.NumCPU())

	cluster := gocql.NewCluster(settings.host)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: settings.user,
		Password: settings.password,
	}
	cluster.Timeout, _ = time.ParseDuration("30s")
	cluster.Consistency = gocql.One

	llog.Infof("Establishing connection to the cluster")
	session, err := cluster.CreateSession()
	if err != nil {
		return merry.Wrap(err)
	}
	defer session.Close()
	llog.Infof("Creating the keyspace and tables...")
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
	defer session.Close()
	if err = session.Query(CREATE_ACCOUNTS_TAB).Exec(); err != nil {
		return merry.Wrap(err)
	}
	if err = session.Query(CREATE_TRANSFERS_TAB).Exec(); err != nil {
		return merry.Wrap(err)
	}

	worker := func(id int, n_accounts int, wg *sync.WaitGroup) {

		defer wg.Done()

		var rand FixedRandomSource
		rand.Init(nil)

		stmt := session.Query(INSERT_ACCOUNT)
		stmt.Consistency(gocql.One)
		llog.Tracef("Worker %d inserting %d accounts", id, n_accounts)
		for i := 0; i < n_accounts; i++ {
			cookie := StatsRequestStart()
			bic, ban := rand.NewBicAndBan()
			balance := rand.NewStartBalance()
			stmt.Bind(bic, ban, balance)
			for err = stmt.Exec(); err != nil; {
				reqErr, isRequestErr := err.(gocql.RequestError)
				if isRequestErr && reqErr != nil {
					llog.Tracef("Error: !!!")
					//					llog.Tracef("Error: %v Code: %v Message: %v",
					//	reqErr.Error(), reqErr.Code(), reqErr.Message())
					time.Sleep(time.Millisecond)
				} else {
					llog.Fatalf("Got fatal error: %+v", err)
				}
			}
			StatsRequestEnd(cookie)
		}
		llog.Tracef("Worker %d done %d accounts", id, n_accounts)
	}
	var wg sync.WaitGroup

	accounts_per_worker := settings.count / settings.workers
	remainder := settings.count - accounts_per_worker*settings.workers

	llog.Infof("Creating %v accounts using %v workers",
		settings.count, settings.workers)
	for i := 0; i < settings.workers; i++ {
		n_accounts := accounts_per_worker
		if i < remainder {
			n_accounts++
		}
		wg.Add(1)
		go worker(i+1, n_accounts, &wg)
	}

	wg.Wait()

	return nil
}
