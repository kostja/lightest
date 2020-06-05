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

type Row = map[string]interface{}

type PayStats struct {
	errors          uint64
	no_such_account uint64
	retries         uint64
	recoveries      uint64
}

type Client struct {
	client_id            gocql.UUID
	session              *gocql.Session
	payStats             *PayStats
	insert               *gocql.Query
	fetch                *gocql.Query
	update_client        *gocql.Query
	update_client_if_nil *gocql.Query
	update_state         *gocql.Query
	delete_              *gocql.Query
	lock_account         *gocql.Query
	fetch_balance        *gocql.Query
	update_balance       *gocql.Query
}

type Account struct {
	bic       string
	ban       string
	balance   *inf.Dec
	lockOrder int
}

func (c *Client) New(session *gocql.Session, payStats *PayStats) {
	c.client_id = gocql.TimeUUID()
	c.session = session
	c.payStats = payStats
	c.insert = session.Query(INSERT_TRANSFER)
	c.fetch = session.Query(FETCH_TRANSFER)
	c.update_client = session.Query(UPDATE_TRANSFER_CLIENT)
	c.update_client_if_nil = session.Query(UPDATE_TRANSFER_CLIENT_IF_NIL)
	c.update_state = session.Query(UPDATE_TRANSFER_STATE)
	c.delete_ = session.Query(DELETE_TRANSFER)
	c.lock_account = session.Query(LOCK_ACCOUNT)
	c.fetch_balance = session.Query(FETCH_BALANCE)
	c.update_balance = session.Query(UPDATE_BALANCE)
}

func (c *Client) RegisterTransfer(accounts []Account, amount *inf.Dec) (gocql.UUID, error) {

	// Register a new transfer
	transfer_id := gocql.TimeUUID()
	c.insert.Bind(transfer_id, accounts[0].bic, accounts[0].ban,
		accounts[1].bic, accounts[1].ban, amount)

	row := Row{}
	if applied, err := c.insert.MapScanCAS(row); err != nil || !applied {
		if err == nil && !applied {
			// Should never happen, transfer id is globally unique
			err = merry.New(fmt.Sprintf("Failed to create a transfer %v: a duplicate transfer exists",
				transfer_id))
		}
		return transfer_id, merry.Wrap(err)
	}
	return transfer_id, nil
}

// Accept interfaces to allow nil client id
func (c *Client) SetTransferState(
	old_client_id interface{}, new_client_id interface{},
	transfer_id gocql.UUID, state string, must_apply bool) error {

	var cql *gocql.Query

	if old_client_id == nil {
		cql = c.update_client_if_nil
		cql.Bind(new_client_id, transfer_id)
	} else {
		cql = c.update_client
		cql.Bind(new_client_id, transfer_id, old_client_id)
	}
	// Change transfer - set client id
	row := Row{}
	if applied, err := cql.MapScanCAS(row); err != nil || (!applied && must_apply) {
		if err == nil && !applied && must_apply {
			// Should never happen, noone can pick up our transfer but us
			err = merry.New(fmt.Sprintf("Failed to cilent_id update: %v != %v",
				old_client_id, row["client_id"]))
		}
		return merry.Wrap(err)
	}
	if new_client_id == nil {
		return nil
	}
	// Change transfer - set state. The state is reset without TTL
	row = Row{}
	c.update_state.Bind(state, transfer_id, new_client_id)
	if applied, err := c.update_state.MapScanCAS(row); err != nil || (!applied && must_apply) {
		if err == nil && !applied && must_apply {
			// Should never happen, noone can pick up our transfer but us
			err = merry.New(fmt.Sprintf("Failed to apply state change: %v != %v",
				old_client_id, row["client_id"]))
		}
		return merry.Wrap(err)
	}
	return nil
}

func (c *Client) LockAccounts(transfer_id gocql.UUID, accounts []Account, wait bool) error {

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
		c.lock_account.Bind(transfer_id, account.bic, account.ban, nil)
		row := Row{}
		// If the update is not applied because we've already locked the
		// transfer, it's a success. This is possible during recovery.
		lockFailed := func(applied bool) bool {
			return !applied && row["pending_transfer"] != transfer_id
		}
		if applied, err := c.lock_account.MapScanCAS(row); err != nil || lockFailed(applied) {
			if i == 1 {
				// Remove the pending transfer from the previously
				// locked account, do not wait with locks.
				account = &accounts[accounts[0].lockOrder]
				c.lock_account.Bind(nil, account.bic, account.ban, transfer_id)
				if err1 := c.lock_account.Exec(); err1 != nil {
					return merry.WithCause(err1, err)
				}
			}
			// Check for transient errors, such as query timeout, and retry.
			// Not doing it in scope of a demo
			if err != nil {
				return merry.Wrap(err)
			}
			if len(row) == 0 {
				atomic.AddUint64(&c.payStats.no_such_account, 1)
				llog.Tracef("Account %v not found, ending transfer %v", account.ban, transfer_id)
				return c.SetTransferState(c.client_id, nil, transfer_id, "complete", false)
			}
			pending_transfer := row["pending_transfer"].(gocql.UUID)

			// Check if the transfer is orphaned and recover it
			row = Row{}
			iter := c.fetch.Bind(pending_transfer).Iter()
			nil_uuid := gocql.UUID{}
			// Ignore possible error, we will retry
			defer iter.Close()
			if iter.MapScan(row) {
				if client_id, exists := row["client_id"]; !exists || client_id == nil_uuid {
					// The transfer has no client working on it,
					// recover it.
					// @todo: if we already in recovery, only push
					// into the queue if it is not full, to avoid a
					// deadlock.
					atomic.AddUint64(&c.payStats.recoveries, 1)
					Recover(pending_transfer)
				} else {
					llog.Tracef("Not recovering transfer %v worked on by client %v",
						pending_transfer, client_id)
				}
			}
			atomic.AddUint64(&c.payStats.retries, 1)

			if !wait {
				return merry.New("Wait aborted")
			}

			llog.Tracef("Restarting after sleeping %v, pending transfer %v",
				sleepDuration, pending_transfer)

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
				c.fetch_balance.Bind(account.bic, account.ban)
				row = Row{}
				if _, err := c.fetch_balance.MapScanCAS(row); err != nil {
					return merry.Wrap(err)
				}
				account.balance = row["balance"].(*inf.Dec)
			}
			i++
		}
	}
	return nil
}

func (c *Client) CompleteLockedTransfer(
	transfer_id gocql.UUID, accounts []Account, amount *inf.Dec) error {

	if amount.Cmp(accounts[0].balance) < 0 {
		llog.Tracef("Moving %v from %v to %v", amount, accounts[0].balance, accounts[1].balance)
		accounts[0].balance.Sub(accounts[0].balance, amount)
		accounts[1].balance.Add(accounts[1].balance, amount)
	} else {
		llog.Tracef("Insufficient balance %v to withdraw %v", accounts[0].balance, amount)
	}

	// From now on we can ignore 'applied' - the record may
	// not be applied only if someone completed our transfer or
	// 30 seconds have elapsed.
	// Move transfer to 'in progress', to not attempt to transfer the money
	// twice
	c.update_state.Bind("in progress", transfer_id, c.client_id)
	if err := c.update_state.Exec(); err != nil {
		return merry.Wrap(err)
	}

	c.update_balance.Bind(accounts[0].balance, accounts[0].bic, accounts[0].ban, transfer_id)
	if err := c.update_balance.Exec(); err != nil {
		return merry.Wrap(err)
	}
	c.update_balance.Bind(accounts[1].balance, accounts[1].bic, accounts[1].ban, transfer_id)
	if err := c.update_balance.Exec(); err != nil {
		return merry.Wrap(err)
	}
	// Move transfer to "complete". Typically a transfer is kept
	// for a few years, we just delete it for simplicity.
	c.delete_.Bind(transfer_id, c.client_id)
	if err := c.delete_.Exec(); err != nil {
		return merry.Wrap(err)
	}
	return nil
}

func (c *Client) MakeTransfer(accounts []Account, amount *inf.Dec) error {

	var transfer_id gocql.UUID
	var err error
	if transfer_id, err = c.RegisterTransfer(accounts, amount); err != nil {
		return merry.Wrap(err)
	}
	// Change transfer state to pending and set client id
	if err = c.SetTransferState(nil, c.client_id, transfer_id, "pending", true); err != nil {
		return merry.Wrap(err)
	}
	if err = c.LockAccounts(transfer_id, accounts, true); err != nil {
		return merry.Wrap(err)
	}
	return c.CompleteLockedTransfer(transfer_id, accounts, amount)
}

func (c *Client) RecoverTransfer(transfer_id gocql.UUID) {
	llog.Infof("Recover: %v", transfer_id)
	if err := c.SetTransferState(nil, c.client_id, transfer_id, "pending", true); err != nil {
		llog.Infof("Failed to recover transfer %v: %v", transfer_id, err)
		return
	}
	row := Row{}
	iter := c.fetch.Bind(transfer_id).Iter()
	// Ignore possible error, we will retry
	defer iter.Close()
	accounts := make([]Account, 2, 2)
	var amount *inf.Dec
	var state string
	if iter.MapScan(row) {
		accounts[0].bic = row["src_bic"].(string)
		accounts[0].ban = row["src_ban"].(string)
		accounts[1].bic = row["dst_bic"].(string)
		accounts[1].ban = row["dst_ban"].(string)
		amount = row["amount"].(*inf.Dec)
		state = row["state"].(string)
	} else {
		llog.Infof("Failed to recover transfer: transfer %v not found", transfer_id)
		return
	}
	if state != "in progress" {
		// We should avoid locking the transfer if state = "in
		// progresss" since that may lead to double withdrawal
		// on the same transfer
		if err := c.LockAccounts(transfer_id, accounts, false); err != nil {
			llog.Infof("Recovery failed to lock accounts: %v", err)
			return
		}
	}
	llog.Infof("Recovering transfer %v amount %v", transfer_id, amount)
	if amount == nil {
		// This can happen because of a timestamp tie:
		// http://datanerds.io/post/cassandra-no-row-consistency/
		c.delete_.Bind(transfer_id, c.client_id)
		c.delete_.Exec()
	} else {
		c.CompleteLockedTransfer(transfer_id, accounts, amount)
	}
}

func payWorker(
	n_transfers int, session *gocql.Session, payStats *PayStats,
	randSource *FixedRandomSource, wg *sync.WaitGroup) {

	defer wg.Done()

	var client Client
	client.New(session, payStats)

	for i := 0; i < n_transfers; i++ {

		amount := randSource.NewTransferAmount()
		accounts := make([]Account, 2, 2)
		accounts[0].bic, accounts[0].ban = randSource.BicAndBan()
		accounts[1].bic, accounts[1].ban = randSource.BicAndBan(accounts[0].bic, accounts[0].ban)

		cookie := StatsRequestStart()
		err := client.MakeTransfer(accounts, amount)
		StatsRequestEnd(cookie)

		if err != nil {
			llog.Errorf("%+v", err)
			atomic.AddUint64(&payStats.errors, 1)
			return
		}
	}
}

func pay(cmd *cobra.Command, n int, workers int) error {

	if workers > n {
		workers = n
	}
	llog.Infof("Making %d transfers using %d workers on %d cores \n",
		n, workers, runtime.NumCPU())

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
	defer session.Close()

	var wg sync.WaitGroup
	var payStats PayStats
	var randSource FixedRandomSource
	randSource.Init(session)

	transfers_per_worker := n / workers
	remainder := n - transfers_per_worker*workers

	for i := 0; i < workers; i++ {
		wg.Add(1)
		n_transfers := transfers_per_worker
		if i < remainder {
			n_transfers++
		}
		go payWorker(n_transfers, session, &payStats, &randSource, &wg)
	}
	RecoveryStart(session, &payStats)

	wg.Wait()
	RecoveryStop()

	llog.Infof("Errors: %v, Retries: %v, Recoveries: %v, Not found: %v\n",
		payStats.errors,
		payStats.retries,
		payStats.recoveries,
		payStats.no_such_account)

	return nil
}
