package main

import (
	"fmt"
	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/inf.v0"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type PayStats struct {
	errors          uint64
	no_such_account uint64
	retries         uint64
	recoveries      uint64
}

type Transfer struct {
	insert         *gocql.Query
	update         *gocql.Query
	delete_        *gocql.Query
	lock_account   *gocql.Query
	fetch_balance  *gocql.Query
	update_balance *gocql.Query
}

type Account struct {
	bic       string
	ban       string
	balance   *inf.Dec
	lockOrder int
}

func (t *Transfer) New(session *gocql.Session) {
	t.insert = session.Query(INSERT_TRANSFER)
	t.update = session.Query(UPDATE_TRANSFER)
	t.delete_ = session.Query(DELETE_TRANSFER)
	t.lock_account = session.Query(LOCK_ACCOUNT)
	t.fetch_balance = session.Query(FETCH_BALANCE)
	t.update_balance = session.Query(UPDATE_BALANCE)
}

func (t *Transfer) Make(
	client_id gocql.UUID, accounts []Account,
	amount *inf.Dec, stats *PayStats) error {

	type Row = map[string]interface{}
	var row Row

	// Register a new transfer
	transfer_id := gocql.TimeUUID()
	row = Row{}
	t.insert.Bind(transfer_id,
		accounts[0].bic, accounts[0].ban,
		accounts[1].bic, accounts[1].ban, amount)
	if applied, err := t.insert.MapScanCAS(row); err != nil {
		return merry.Wrap(err)
	} else if !applied {
		// Should never happen, transfer id is globally unique
		return merry.New(fmt.Sprintf("Failed to create a transfer %v: a duplicate transfer exists",
			transfer_id))
	}
	// Change transfer state to pending and set client id
	t.update.Bind(client_id, "pending", transfer_id, nil)
	row = Row{}
	if applied, err := t.update.MapScanCAS(row); err != nil {
		return merry.Wrap(err)
	} else if !applied {
		// Should never happen, noone can pick up our transfer but us
		return merry.New(fmt.Sprintf("Failed to update transfer %v",
			transfer_id))
	}

	// Always lock accounts in lexicographical order to avoid livelocks
	if accounts[1].bic > accounts[0].bic ||
		accounts[1].bic == accounts[0].bic &&
			accounts[1].ban > accounts[0].ban {
		accounts[1].lockOrder = 1
	} else {
		accounts[0].lockOrder = 1
	}
	sleepDuration := time.Millisecond*time.Duration(rand.Intn(10)) + time.Millisecond
	maxSleepDuration, _ := time.ParseDuration("10s")
	var i = 0
	for i < 2 {
		account := &accounts[accounts[i].lockOrder]
		t.lock_account.Bind(transfer_id, account.bic, account.ban, nil)
		row = Row{}
		if applied, err := t.lock_account.MapScanCAS(row); !applied || err != nil {
			if i == 1 {
				// Remove the pending transfer from the previously
				// locked account, do not wait with locks.
				account = &accounts[accounts[0].lockOrder]
				t.lock_account.Bind(nil, account.bic, account.ban, transfer_id)
				if err1 := t.lock_account.Exec(); err1 != nil {
					return merry.WithCause(err1, err)
				}
			}
			// Check for transient errors, such as query timeout, and retry.
			// Not doing it in scope of a demo
			if err != nil {
				return merry.Wrap(err)
			}
			if len(row) == 0 {
				atomic.AddUint64(&stats.no_such_account, 1)
				llog.Tracef("Account %v not found, ending transfer %v", account.ban, transfer_id)
				return t.SetTransferState(client_id, nil, transfer_id, "complete")
			}
			// pending_transfer != null
			// @todo: check if the transfer is orphaned and repair
			llog.Tracef("Restarting after sleeping %v, pending transfer %v",
				sleepDuration, row["pending_transfer"])
			atomic.AddUint64(&stats.retries, 1)
			time.Sleep(sleepDuration)
			sleepDuration = sleepDuration * 2
			if sleepDuration > maxSleepDuration {
				sleepDuration = maxSleepDuration
			}
			// Restart locking
			i = 0
		} else {
			// In Scylla, the previous row returned even if LWT is applied.
			// In Cassandra, make a separate query.
			if val, exists := row["balance"]; exists {
				account.balance = val.(*inf.Dec)
			} else {
				// Support Cassandra which doens't provide balance
				t.fetch_balance.Bind(account.bic, account.ban)
				row = Row{}
				if _, err := t.fetch_balance.MapScanCAS(row); err != nil {
					return merry.Wrap(err)
				}
				account.balance = row["balance"].(*inf.Dec)
			}
			i++
		}
	}
	if amount.Cmp(accounts[0].balance) < 0 {
		llog.Tracef("Moving %v from %v to %v", amount, accounts[0].balance, accounts[1].balance)
		accounts[0].balance.Sub(accounts[0].balance, amount)
		accounts[1].balance.Add(accounts[1].balance, amount)
	} else {
		llog.Tracef("Insufficient balance %v to withdraw %v", accounts[0].balance, amount)
	}
	return t.CompleteLockedTransfer(client_id, transfer_id, accounts)
}

func (t *Transfer) CompleteLockedTransfer(
	client_id gocql.UUID, transfer_id gocql.UUID, accounts []Account) error {

	// From now on we can ignore 'applied' - the record may
	// not be applied only if someone completed our transfer or
	// 300 seconds have elapsed.
	// Move transfer to 'in progress', to not attempt to transfer the money
	// twice
	t.update.Bind(client_id, "in progress", transfer_id, client_id)
	if err := t.update.Exec(); err != nil {
		return merry.Wrap(err)
	}

	t.update_balance.Bind(accounts[0].balance, accounts[0].bic, accounts[0].ban, transfer_id)
	if err := t.update_balance.Exec(); err != nil {
		return merry.Wrap(err)
	}
	t.update_balance.Bind(accounts[1].balance, accounts[1].bic, accounts[1].ban, transfer_id)
	if err := t.update_balance.Exec(); err != nil {
		return merry.Wrap(err)
	}
	// Move transfer to "complete". Typically a transfer is kept
	// for a few years, we just delete it for simplicity.
	t.delete_.Bind(transfer_id, client_id, "in progress")
	if err := t.delete_.Exec(); err != nil {
		return merry.Wrap(err)
	}
	return nil
}

func (t *Transfer) SetTransferState(
	old_client_id interface{}, new_client_id interface{},
	transfer_id gocql.UUID, state string) error {

	// Change transfer state to pending and set client id
	t.update.Bind(new_client_id, state, transfer_id, new_client_id)
	if err := t.update.Exec(); err != nil {
		return merry.Wrap(err)
	}
	return nil
}

func pay(cmd *cobra.Command, n int) error {

	var cores = runtime.NumCPU()
	var workers = 4 * cores
	if workers > n {
		workers = n
	}
	llog.Infof("Making %d transfers using %d workers on %d cores \n",
		n, workers, cores)

	cluster := gocql.NewCluster("localhost")
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}
	cluster.Timeout, _ = time.ParseDuration("30s")
	cluster.Keyspace = "lightest"
	cluster.Consistency = gocql.One

	llog.Infof("Establishing connection to the cluster")
	session, err := cluster.CreateSession()
	if err != nil {
		return merry.Wrap(err)
	}
	var stats PayStats
	var rand FixedRandomSource
	rand.Init(session)
	sum := check(session, nil)

	llog.Infof("Initial balance: %v", sum)

	worker := func(client_id gocql.UUID, n_transfers int, wg *sync.WaitGroup) {

		defer wg.Done()

		var transfer Transfer
		transfer.New(session)

		for i := 0; i < n_transfers; i++ {

			amount := rand.NewTransferAmount()
			accounts := make([]Account, 2, 2)
			accounts[0].bic, accounts[0].ban = rand.BicAndBan()
			accounts[1].bic, accounts[1].ban = rand.BicAndBan(accounts[0].bic, accounts[0].ban)

			err := transfer.Make(client_id, accounts, amount, &stats)
			if err != nil {
				llog.Errorf("%+v", err)
				atomic.AddUint64(&stats.errors, 1)
				return
			}
		}

	}
	var wg sync.WaitGroup
	transfers_per_worker := n / workers
	remainder := n - transfers_per_worker*workers

	for i := 0; i < workers; i++ {
		wg.Add(1)
		n_transfers := transfers_per_worker
		if i < remainder {
			n_transfers++
		}
		go worker(gocql.TimeUUID(), n_transfers, &wg)
	}

	wg.Wait()

	llog.Infof("Final balance: %v", check(session, sum))
	llog.Infof("Errors: %v, Retries: %v, Recoveries: %v, Not found: %v\n",
		stats.errors,
		stats.retries,
		stats.recoveries,
		stats.no_such_account)

	return nil
}
