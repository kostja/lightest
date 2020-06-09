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
	client_id             gocql.UUID
	session               *gocql.Session
	payStats              *PayStats
	insert                *gocql.Query
	fetch                 *gocql.Query
	update                *gocql.Query
	update_if_client_null *gocql.Query
	update_state          *gocql.Query
	delete_               *gocql.Query
	lock_account          *gocql.Query
	fetch_balance         *gocql.Query
	update_balance        *gocql.Query
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
	c.update = session.Query(UPDATE_TRANSFER)
	c.update_if_client_null = session.Query(UPDATE_TRANSFER_IF_CLIENT_NULL)
	c.update_state = session.Query(UPDATE_TRANSFER_STATE)
	c.delete_ = session.Query(DELETE_TRANSFER)
	c.lock_account = session.Query(LOCK_ACCOUNT)
	c.fetch_balance = session.Query(FETCH_BALANCE)
	c.update_balance = session.Query(UPDATE_BALANCE)
}

func (c *Client) RegisterTransfer(acs []Account, amount *inf.Dec) (gocql.UUID, error) {

	// Register a new transfer
	transfer_id := gocql.TimeUUID()
	c.insert.Bind(transfer_id, acs[0].bic, acs[0].ban,
		acs[1].bic, acs[1].ban, amount)

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
func (c *Client) SetTransferClient(
	old_client_id interface{}, new_client_id interface{},
	transfer_id gocql.UUID) error {

	var cql *gocql.Query

	if old_client_id == nil {
		cql = c.update_if_client_null
		cql.Bind(new_client_id, transfer_id)
	} else {
		cql = c.update
		cql.Bind(new_client_id, transfer_id, old_client_id)
	}
	// Change transfer - set client id
	row := Row{}
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err == nil && !applied {
			// Should never happen, noone can pick up our transfer but us
			err = merry.New(fmt.Sprintf("Failed client_id update: %v != %v",
				old_client_id, row["client_id"]))
		}
		return merry.Wrap(err)
	}
	return nil
}

func (c *Client) FetchBalance(bic string, ban string) (*inf.Dec, error) {
	c.fetch_balance.Bind(bic, ban)
	row := Row{}
	if _, err := c.fetch_balance.MapScanCAS(row); err != nil {
		return nil, merry.Wrap(err)
	}
	return row["balance"].(*inf.Dec), nil
}

func (c *Client) LockAccounts(transfer_id gocql.UUID, acs []Account, wait bool) error {

	// Always lock accounts in lexicographical order to avoid livelocks
	if acs[1].bic > acs[0].bic ||
		acs[1].bic == acs[0].bic &&
			acs[1].ban > acs[0].ban {
		acs[1].lockOrder = 1
	} else {
		acs[0].lockOrder = 1
	}
	sleepDuration := time.Millisecond*time.Duration(rand.Intn(10)) + time.Millisecond
	maxSleepDuration, _ := time.ParseDuration("10s")

	var i = 0
	for i < 2 {
		account := &acs[acs[i].lockOrder]
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
				account = &acs[acs[0].lockOrder]
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
				return c.DeleteTransfer(transfer_id)
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
				var err error
				// Support Cassandra which doens't provide balance
				if account.balance, err = c.FetchBalance(account.bic, account.ban); err != nil {
					return merry.Wrap(err)
				}
			}
			i++
		}
	}
	// Move transfer to 'in progress', to not attempt to transfer
	// 	the money twice during recovery
	row := Row{}
	c.update_state.Bind("in progress", transfer_id, c.client_id)
	if applied, err := c.update_state.MapScanCAS(row); err != nil || !applied {
		if err != nil {
			return merry.Wrap(err)
		}
		return merry.New(fmt.Sprintf("Failed to change transfer %v to 'in progress', %v",
			transfer_id,
			fmt.Sprintf("client id mismatch: %v != %v",
				c.client_id, row["client_id"])))
		// No need to unlock: the transfer will be repaired
	}
	return nil
}

func (c *Client) CompleteLockedTransfer(
	transfer_id gocql.UUID, acs []Account, amount *inf.Dec) error {

	if amount.Cmp(acs[0].balance) < 0 {
		llog.Tracef("Moving %v from %v to %v", amount, acs[0].balance, acs[1].balance)
		acs[0].balance.Sub(acs[0].balance, amount)
		acs[1].balance.Add(acs[1].balance, amount)
	} else {
		llog.Tracef("Insufficient balance %v to withdraw %v", acs[0].balance, amount)
	}
	// From now on we can ignore 'applied' - the record may
	// not be applied only if someone completed our transfer or
	// 30 seconds have elapsed.

	c.update_balance.Bind(acs[0].balance, acs[0].bic, acs[0].ban, transfer_id)
	if err := c.update_balance.Exec(); err != nil {
		return merry.Wrap(err)
	}
	c.update_balance.Bind(acs[1].balance, acs[1].bic, acs[1].ban, transfer_id)
	if err := c.update_balance.Exec(); err != nil {
		return merry.Wrap(err)
	}
	return c.DeleteTransfer(transfer_id)
}

func (c *Client) DeleteTransfer(transfer_id gocql.UUID) error {
	// Move transfer to "complete". Typically a transfer is kept
	// for a few years, we just delete it for simplicity.
	row := Row{}
	c.delete_.Bind(transfer_id, c.client_id)
	if applied, err := c.delete_.MapScanCAS(row); err != nil || !applied {
		if err != nil {
			return merry.Wrap(err)
		} else {
			return merry.New(fmt.Sprintf("Failed to delete transfer %v: %v != %v",
				transfer_id, c.client_id, row["client_id"]))
		}
	}
	return nil
}

func (c *Client) MakeTransfer(acs []Account, amount *inf.Dec) error {

	var transfer_id gocql.UUID
	var err error
	if transfer_id, err = c.RegisterTransfer(acs, amount); err != nil {
		return merry.Wrap(err)
	}
	// Change transfer state to pending and set client id
	if err = c.SetTransferClient(nil, c.client_id, transfer_id); err != nil {
		return merry.Wrap(err)
	}
	if err = c.LockAccounts(transfer_id, acs, true); err != nil {
		return merry.Wrap(err)
	}
	return c.CompleteLockedTransfer(transfer_id, acs, amount)
}

func (c *Client) RecoverTransfer(transfer_id gocql.UUID) {
	atomic.AddUint64(&c.payStats.recoveries, 1)
	if err := c.SetTransferClient(nil, c.client_id, transfer_id); err != nil {
		llog.Errorf("Recovery failed to set transfer %v state: %v", transfer_id, err)
		return
	}
	row := Row{}
	iter := c.fetch.Bind(transfer_id).Iter()
	// Ignore possible error, we will retry
	defer iter.Close()
	acs := make([]Account, 2, 2)
	var amount *inf.Dec
	var state string
	if iter.MapScan(row) {
		acs[0].bic = row["src_bic"].(string)
		acs[0].ban = row["src_ban"].(string)
		acs[1].bic = row["dst_bic"].(string)
		acs[1].ban = row["dst_ban"].(string)
		amount = row["amount"].(*inf.Dec)
		state = row["state"].(string)

		if amount == nil {
			llog.Errorf("Deleting transfer %v with nil amount", transfer_id)
			cql := c.delete_.Bind(transfer_id, c.client_id)
			// This can happen because of a timestamp tie:
			// http://datanerds.io/post/cassandra-no-row-consistency/
			row := Row{}
			if applied, err := cql.MapScanCAS(row); err != nil || !applied {
				llog.Errorf("Recovery failed to delete a dead transfer %v: %v", transfer_id, err)
			}
			return
		}
		if state == "new" {
			// We should avoid locking the transfer if state = "in
			// progresss" since that may lead to double withdrawal
			// on the same transfer
			if err := c.LockAccounts(transfer_id, acs, false); err != nil {
				llog.Errorf("Recovery of %v failed to lock acs: %v", transfer_id, err)
				return
			}
		} else if state == "in progress" {
			var err error
			// Support Cassandra which doens't provide balance
			if acs[0].balance, err = c.FetchBalance(acs[0].bic, acs[0].ban); err != nil {
				return
			}
			if acs[1].balance, err = c.FetchBalance(acs[1].bic, acs[1].ban); err != nil {
				return
			}

		}
		llog.Infof("Recovering transfer %v amount %v", transfer_id, amount)
		c.CompleteLockedTransfer(transfer_id, acs, amount)
	} else {
		llog.Errorf("Recovery failed, transfer %v not found", transfer_id)
		return
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
		acs := make([]Account, 2, 2)
		acs[0].bic, acs[0].ban = randSource.BicAndBan()
		acs[1].bic, acs[1].ban = randSource.BicAndBan(acs[0].bic, acs[0].ban)

		cookie := StatsRequestStart()
		err := client.MakeTransfer(acs, amount)
		StatsRequestEnd(cookie)

		if err != nil {
			llog.Errorf("%+v", err)
			atomic.AddUint64(&payStats.errors, 1)
			return
		}
	}
}

func pay(cmd *cobra.Command, n int, workers int) error {

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

	RecoveryStart(session, &payStats)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		n_transfers := transfers_per_worker
		if i < remainder {
			n_transfers++
		}
		go payWorker(n_transfers, session, &payStats, &randSource, &wg)
	}

	wg.Wait()
	RecoveryStop()

	llog.Infof("Errors: %v, Retries: %v, Recoveries: %v, Not found: %v\n",
		payStats.errors,
		payStats.retries,
		payStats.recoveries,
		payStats.no_such_account)

	return nil
}
