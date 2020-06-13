package main

import (
	"encoding/json"
	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func bootstrapDatabase(cluster *gocql.ClusterConfig, settings *Settings) error {

	llog.Infof("Establishing connection to %v", settings.host)

	session, err := cluster.CreateSession()
	if err != nil {
		return merry.Wrap(err)
	}
	defer session.Close()

	llog.Infof("Creating the keyspace and tables...")

	if err := session.Query(DROP_KS).Exec(); err != nil {
		return merry.Wrap(err)
	}
	if err := session.Query(CREATE_KS).Exec(); err != nil {
		return merry.Wrap(err)
	}
	defer session.Close()
	if err = session.Query(CREATE_SETTINGS_TAB).Exec(); err != nil {
		return merry.Wrap(err)
	}
	if err = session.Query(CREATE_ACCOUNTS_TAB).Exec(); err != nil {
		return merry.Wrap(err)
	}
	if err = session.Query(CREATE_TRANSFERS_TAB).Exec(); err != nil {
		return merry.Wrap(err)
	}
	str := func(val interface{}) string {
		b, _ := json.Marshal(val)
		return string(b)
	}
	if err = session.Query(INSERT_SETTING).Bind("accounts", str(settings.count)).Exec(); err != nil {
		return merry.Wrap(err)
	}
	if err = session.Query(INSERT_SETTING).Bind("seed", str(settings.seed)).Exec(); err != nil {
		return merry.Wrap(err)
	}
	return nil
}

type PopStats struct {
	errors     uint64
	duplicates uint64
}

// type L struct{}

// func (l L) ObserveQuery(ctx context.Context, q gocql.ObservedQuery) {
// 	fmt.Println(q.Statement)
// }
//
//	cluster.QueryObserver = L{}

func populate(settings *Settings) error {

	cluster := gocql.NewCluster(settings.host)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: settings.user,
		Password: settings.password,
	}
	cluster.Timeout, _ = time.ParseDuration("30s")
	cluster.Consistency = gocql.One

	if err := bootstrapDatabase(cluster, settings); err != nil {
		return merry.Wrap(err)
	}
	stats := PopStats{}

	cluster.Keyspace = "lightest"
	session, err := cluster.CreateSession()
	if err != nil {
		return merry.Wrap(err)
	}
	defer session.Close()

	worker := func(id int, n_accounts int, wg *sync.WaitGroup) {

		defer wg.Done()

		var rand FixedRandomSource
		rand.Init(session)

		stmt := session.Query(INSERT_ACCOUNT)
		stmt.Consistency(gocql.One)
		llog.Tracef("Worker %d inserting %d accounts", id, n_accounts)
		for i := 0; i < n_accounts; {
			cookie := StatsRequestStart()
			bic, ban := rand.NewBicAndBan()
			balance := rand.NewStartBalance()
			llog.Tracef("Inserting account %v:%v - %v", bic, ban, balance)
			stmt.Bind(bic, ban, balance)
			row := Row{}
			applied := false
			for applied, err = stmt.MapScanCAS(row); err != nil; {
				atomic.AddUint64(&stats.errors, 1)
				reqErr, isRequestErr := err.(gocql.RequestError)
				if isRequestErr && reqErr != nil {

					llog.Errorf("Retrying after request error: %v", reqErr)
					time.Sleep(time.Millisecond)
				} else if err == gocql.ErrTimeoutNoResponse {
					llog.Errorf("Retrying after timeout: %v", err)
					time.Sleep(time.Millisecond)
				} else {
					llog.Fatalf("Fatal error: %+v", err)
				}
			}
			if applied {
				i++
				StatsRequestEnd(cookie)
			} else {
				atomic.AddUint64(&stats.duplicates, 1)
			}
		}
		llog.Tracef("Worker %d done %d accounts", id, n_accounts)
	}

	llog.Infof("Creating %d accounts using %d workers on %d cores \n",
		settings.count, settings.workers,
		runtime.NumCPU())

	var wg sync.WaitGroup

	accounts_per_worker := settings.count / settings.workers
	remainder := settings.count - accounts_per_worker*settings.workers

	for i := 0; i < settings.workers; i++ {
		n_accounts := accounts_per_worker
		if i < remainder {
			n_accounts++
		}
		wg.Add(1)
		go worker(i+1, n_accounts, &wg)
	}

	wg.Wait()
	llog.Infof("Done %v accounts, %v errors, %v duplicates",
		settings.count, stats.errors, stats.duplicates)

	return nil
}
