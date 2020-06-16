package main

import (
	"fmt"
	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"gopkg.in/inf.v0"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Row = map[string]interface{}

var nilUuid gocql.UUID

type PayStats struct {
	errors             uint64
	no_such_account    uint64
	insufficient_funds uint64
	retries            uint64
	recoveries         uint64
}

var nClients uint64

type Client struct {
	shortId             uint64     // For logging
	clientId            gocql.UUID // For locking
	session             *gocql.Session
	oracle              *Oracle
	payStats            *PayStats
	insert              *gocql.Query
	fetch               *gocql.Query
	fetchClient         *gocql.Query
	update              *gocql.Query
	setTransferClient   *gocql.Query
	clearTransferClient *gocql.Query
	delete_             *gocql.Query
	lockAccount         *gocql.Query
	unlockAccount       *gocql.Query
	fetchBalance        *gocql.Query
	updateBalance       *gocql.Query
}

type Account struct {
	bic       string
	ban       string
	balance   *inf.Dec
	amount    *inf.Dec
	lockOrder int
	found     bool
}

func InitAccounts(acs []Account, amount *inf.Dec) {
	// Always lock accounts in lexicographical order to avoid livelocks
	if acs[1].bic > acs[0].bic ||
		acs[1].bic == acs[0].bic &&
			acs[1].ban > acs[0].ban {
		acs[1].lockOrder = 1
	} else {
		acs[0].lockOrder = 1
	}
	acs[0].amount = new(inf.Dec).Neg(amount)
	acs[1].amount = amount
}

func (c *Client) Init(session *gocql.Session, oracle *Oracle, payStats *PayStats) {
	c.clientId = gocql.TimeUUID()
	c.shortId = atomic.AddUint64(&nClients, 1)
	llog.Tracef("[%v] Assigning client id %v", c.shortId, c.clientId)
	c.session = session
	c.oracle = oracle
	c.payStats = payStats
	c.insert = session.Query(INSERT_TRANSFER)
	c.fetch = session.Query(FETCH_TRANSFER)
	c.fetch.SerialConsistency(gocql.Serial)
	c.fetchClient = session.Query(FETCH_TRANSFER_CLIENT)
	c.fetchClient.SerialConsistency(gocql.Serial)
	c.fetchBalance = session.Query(FETCH_BALANCE)
	c.fetchBalance.SerialConsistency(gocql.Serial)
	c.setTransferClient = session.Query(SET_TRANSFER_CLIENT)
	c.clearTransferClient = session.Query(CLEAR_TRANSFER_CLIENT)
	c.delete_ = session.Query(DELETE_TRANSFER)
	c.lockAccount = session.Query(LOCK_ACCOUNT)
	c.unlockAccount = session.Query(UNLOCK_ACCOUNT)
	c.updateBalance = session.Query(UPDATE_BALANCE)
}

func (c *Client) RegisterTransfer(acs []Account, amount *inf.Dec) (gocql.UUID, error) {

	transferId := gocql.TimeUUID()
	llog.Tracef("[%v] Registering transfer %v from %v:%v to %v:%v - %v",
		c.shortId, transferId, acs[0].bic, acs[0].ban, acs[1].bic, acs[1].ban, amount)
	// Register a new transfer
	cql := c.insert
	cql.Bind(transferId, acs[0].bic, acs[0].ban, acs[1].bic, acs[1].ban, amount)
	row := Row{}
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err == nil && !applied {
			// Should never happen, transfer id is globally unique
			llog.Fatalf("Failed to create a transfer %v: a duplicate transfer exists", transferId)
		}
		return transferId, merry.Wrap(err)
	}
	return transferId, c.SetTransferClient(transferId)
}

// Accept interfaces to allow nil client id
func (c *Client) SetTransferClient(transferId gocql.UUID) error {

	llog.Tracef("[%v] Setting client on %v", c.shortId, transferId)

	cql := c.setTransferClient
	cql.Bind(c.clientId, transferId)
	// Change transfer - set client id
	row := Row{}
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err != nil {
			return merry.Wrap(err)
		}
		rowClientId, exists := row["client_id"]
		if !exists || rowClientId == nilUuid {
			return merry.Wrap(gocql.ErrNotFound)
		}
		if c.clientId != rowClientId {
			return merry.New(fmt.Sprintf("our id %v, previous id %v",
				c.clientId, rowClientId))
		} // c.clientId == rowClientId
	}
	return nil
}

// In case we failed for whatever reason try to clean up
// the transfer client, to allow speedy recovery
func (c *Client) ClearTransferClient(transferId gocql.UUID) {
	llog.Tracef("[%v] Clearing client on %v", c.shortId, transferId)

	cql := c.clearTransferClient
	cql.Bind(transferId, c.clientId)
	row := Row{}
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err != nil {
			llog.Errorf("[%v] Failed to clear transfer client: %v",
				c.shortId, err)
		} else if !applied {
			rowClientId, exists := row["client_id"]
			if !exists || rowClientId == nilUuid {
				// The transfer is gone, do not complain
			} else {
				err = merry.New(fmt.Sprintf("Client id mismatch: %v != %v",
					c.clientId, row["client_id"]))
				llog.Errorf("[%v] Failed to clear transfer client: %v",
					c.shortId, err)
			}
		}
	}
}

func (c *Client) FetchBalance(acc *Account) error {
	cql := c.fetchBalance
	cql.Bind(acc.bic, acc.ban)
	if err := cql.Scan(&acc.balance); err != nil {
		return err
	}
	acc.found = true
	return nil
}

func (c *Client) UnlockAccount(transferId gocql.UUID, account *Account) error {
	return c.unlockAccount.Bind(account.bic, account.ban, transferId).Exec()
}

func (c *Client) LockAccounts(transferId gocql.UUID, acs []Account, amount *inf.Dec, wait bool) error {

	llog.Tracef("[%v] Locking %v:%v and %v:%v", c.shortId, acs[0].bic, acs[0].ban, acs[1].bic, acs[1].ban)
	sleepDuration := time.Millisecond*time.Duration(rand.Intn(10)) + time.Millisecond
	maxSleepDuration, _ := time.ParseDuration("10s")

	var i = 0
	for i < 2 {
		account := &acs[acs[i].lockOrder]
		cql := c.lockAccount
		cql.Bind(transferId, account.amount, account.bic, account.ban)
		row := Row{}
		// If the update is not applied because we've already locked the
		// transfer, it's a success. This is possible during recovery.
		lockFailed := func(applied bool) bool {
			if applied {
				return false
			}
			// pendingTransfer may be missing from returns (Cassandra)
			pendingTransfer, exists := row["pending_transfer"].(gocql.UUID)
			if exists && pendingTransfer == transferId {
				return false
			}
			return true
		}
		if applied, err := cql.MapScanCAS(row); err != nil || lockFailed(applied) {
			// Remove the pending transfer from the previously
			// locked account, do not wait with locks.
			if i == 1 {
				if err1 := c.UnlockAccount(transferId, &acs[acs[0].lockOrder]); err1 != nil {
					return merry.WithCause(err1, err)
				}
			}
			// Check for transient errors, such as query timeout, and retry.
			// In case of a non-transient error, return it to the client.
			// No money changed its hands and the transfer can be recovered
			// later
			if err != nil {
				reqErr, isRequestErr := err.(gocql.RequestError)
				if isRequestErr && reqErr != nil {
					llog.Errorf("Retrying after request error: %v", reqErr)
				} else if err == gocql.ErrTimeoutNoResponse {
					llog.Errorf("Retrying after timeout: %v", err)
				} else {
					return merry.Wrap(err)
				}
			} else {
				// Lock failed because of a conflict or account is missing.
				pendingTransfer, exists := row["pending_transfer"].(gocql.UUID)
				if !exists || pendingTransfer == nilUuid {
					// No such account. We're not holding locks. CompleteTransfer() will delete
					// the transfer.
					return nil
				}
				// There is a non-empty pending transfer. Check if the
				// transfer we've conflicted with is orphaned and recover
				// it, before waiting
				var clientId gocql.UUID
				c.fetchClient.Bind(pendingTransfer)
				if err := c.fetchClient.Scan(&clientId); err != nil {
					if err != gocql.ErrNotFound {
						return err
					}
					// Transfer not found, even though it's just aborted
					// our lock. It is OK, it might just got completed.
					llog.Tracef("[%v] Conflicting client for transfer %v not found",
						c.shortId, pendingTransfer)
				} else if clientId == nilUuid {
					// The transfer has no client working on it, recover it.
					llog.Tracef("[%v] Adding %v to the recovery queue",
						c.shortId, pendingTransfer)
					RecoverTransfer(pendingTransfer)
				}
				atomic.AddUint64(&c.payStats.retries, 1)

				if !wait {
					return merry.New("Wait aborted")
				}
			}
			// Restart locking
			i = 0

			time.Sleep(sleepDuration)

			llog.Tracef("[%v] Restarting %v after sleeping %v",
				c.shortId, transferId, sleepDuration)

			sleepDuration = sleepDuration * 2
			if sleepDuration > maxSleepDuration {
				sleepDuration = maxSleepDuration
			}
			acs[0].found = false
			acs[1].found = false
			// Reset client id in case it expired while we were sleeping
			if err := c.SetTransferClient(transferId); err != nil {
				if !merry.Is(err, gocql.ErrNotFound) {
					return err
				}
				return nil
			}
		} else {
			// In Scylla, the previous row returned even if LWT is applied.
			// In Cassandra, make a separate query.
			if account.balance, account.found = row["balance"].(*inf.Dec); !account.found {
				// Support Cassandra which doens't provide balance
				if err = c.FetchBalance(account); err != nil {
					return merry.Wrap(err)
				}
			}
			i++
		}
	}
	// Move transfer to 'in progress', to not attempt to transfer
	// 	the money twice during recovery
	return nil
}

func (c *Client) CompleteTransfer(
	transferId gocql.UUID, acs []Account, amount *inf.Dec) error {

	llog.Tracef("[%v] Completing transfer %v amount %v", c.shortId,
		transferId, amount)

	if c.oracle != nil {
		c.oracle.MakeTransfer(transferId, acs, amount)
	}

	if acs[0].found && acs[1].found &&
		amount.Cmp(acs[0].balance) <= 0 {

		llog.Tracef("[%v] Moving %v from %v:%v (%v) to %v:%v (%v)",
			c.shortId, amount,
			acs[0].bic, acs[0].ban, acs[0].balance,
			acs[1].bic, acs[1].ban, acs[1].balance)
		acs[0].balance.Sub(acs[0].balance, amount)
		acs[1].balance.Add(acs[1].balance, amount)
		// From now on we can ignore 'applied' - the record may
		// not be applied only if someone completed our transfer or
		// 30 seconds have elapsed.
		cql := c.updateBalance
		cql.Bind(acs[0].balance, acs[0].bic, acs[0].ban, acs[0].amount, transferId)
		if err := cql.Exec(); err != nil {
			llog.Tracef("[%v] Failed to set %v:%v to %v",
				c.shortId, acs[0].bic, acs[0].ban, acs[0].balance)
			return merry.Wrap(err)
		}
		cql.Bind(acs[1].balance, acs[1].bic, acs[1].ban, acs[1].amount, transferId)
		if err := cql.Exec(); err != nil {
			llog.Tracef("[%v] Failed to set %v:%v to %v",
				c.shortId, acs[1].bic, acs[1].ban, acs[1].balance)
			return merry.Wrap(err)
		}
		if err := c.UnlockAccount(transferId, &acs[0]); err != nil {
			return err
		}
		if err := c.UnlockAccount(transferId, &acs[1]); err != nil {
			return err
		}
	} else if !acs[0].found || !acs[1].found {
		llog.Tracef("[%v] Not found when moving %v from %v:%v (%v) to %v:%v (%v)",
			c.shortId, amount,
			acs[0].bic, acs[0].ban, acs[0].balance,
			acs[1].bic, acs[1].ban, acs[1].balance)

		atomic.AddUint64(&c.payStats.no_such_account, 1)
	} else {
		llog.Tracef("[%v] Insufficient funds when moving %v from %v:%v (%v) to %v:%v (%v)",
			c.shortId, amount,
			acs[0].bic, acs[0].ban, acs[0].balance,
			acs[1].bic, acs[1].ban, acs[1].balance)
		if err := c.UnlockAccount(transferId, &acs[0]); err != nil {
			return err
		}
		if err := c.UnlockAccount(transferId, &acs[1]); err != nil {
			return err
		}
		atomic.AddUint64(&c.payStats.insufficient_funds, 1)
	}

	return c.DeleteTransfer(transferId)
}

func (c *Client) DeleteTransfer(transferId gocql.UUID) error {
	// Move transfer to "complete". Typically a transfer is kept
	// for a few years, we just delete it for simplicity.
	row := Row{}
	cql := c.delete_
	cql.Bind(transferId, c.clientId)
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err != nil {
			return merry.Wrap(err)
		}
		rowClientId, exists := row["client_id"]
		if exists && rowClientId != nilUuid {
			return merry.New(fmt.Sprintf("Delete failed, client id %v does not match row client id %v",
				c.clientId, rowClientId))
		}
		llog.Tracef("[%v] Transfer %v is already deleted", c.shortId, transferId)
		return nil
	}
	llog.Tracef("[%v] Deleted transfer %v", c.shortId, transferId)
	return nil
}

func (c *Client) MakeTransfer(acs []Account, amount *inf.Dec) error {

	var transferId gocql.UUID
	var err error
	if transferId, err = c.RegisterTransfer(acs, amount); err != nil {
		return merry.Wrap(err)
	}
	if err = c.LockAccounts(transferId, acs, amount, true); err != nil {
		return merry.Wrap(err)
	}
	return c.CompleteTransfer(transferId, acs, amount)
}

func (c *Client) RecoverTransfer(transferId gocql.UUID) {
	llog.Tracef("[%v] Recovering transfer %v", c.shortId, transferId)
	atomic.AddUint64(&c.payStats.recoveries, 1)
	if err := c.SetTransferClient(transferId); err != nil {
		llog.Errorf("[%v] Failed to set client on transfer %v: %v",
			c.shortId, transferId, err)
		return
	}
	cql := c.fetch
	cql.Bind(transferId)
	// Ignore possible error, we will retry
	acs := make([]Account, 2, 2)
	var amount *inf.Dec
	if err := cql.Scan(&acs[0].bic, &acs[0].ban, &acs[1].bic, &acs[1].ban, &amount); err != nil {
		if err == gocql.ErrNotFound {
			llog.Errorf("[%v] Transfer %v not found",
				c.shortId, transferId)
		} else {
			llog.Errorf("[%v] Failed to fetch transfer %v: %v",
				c.shortId, transferId, err)
		}
		return
	}
	if amount == nil {
		llog.Fatalf("[%v] Deleting transfer %v with nil amount",
			c.shortId, transferId)
		cql := c.delete_.Bind(transferId, c.clientId)
		// This can happen because of a timestamp tie:
		// http://datanerds.io/post/cassandra-no-row-consistency/
		row := Row{}
		if applied, err := cql.MapScanCAS(row); err != nil || !applied {
			llog.Errorf("[%v] Failed to delete dead transfer %v: %v",
				c.shortId, transferId, err)
		}
		return
	}
	InitAccounts(acs, amount)
	if err := c.LockAccounts(transferId, acs, amount, false); err != nil {
		llog.Errorf("[%v] Failed to lock accounts: %v",
			c.shortId, err)
		c.ClearTransferClient(transferId)
		return
	}
	if err := c.CompleteTransfer(transferId, acs, amount); err != nil {
		llog.Errorf("[%v] Failed to complete transfer %v: %v",
			c.shortId, transferId, err)
	}
}

func payWorker(
	n_transfers int, zipfian bool, session *gocql.Session,
	oracle *Oracle, payStats *PayStats,
	wg *sync.WaitGroup) {

	defer wg.Done()

	var client Client
	var randSource FixedRandomSource
	client.Init(session, oracle, payStats)
	randSource.Init(session)

	for i := 0; i < n_transfers; i++ {

		amount := randSource.NewTransferAmount()
		acs := make([]Account, 2, 2)
		if zipfian {
			acs[0].bic, acs[0].ban = randSource.HotBicAndBan()
			acs[1].bic, acs[1].ban = randSource.HotBicAndBan(acs[0].bic, acs[0].ban)
		} else {
			acs[0].bic, acs[0].ban = randSource.BicAndBan()
			acs[1].bic, acs[1].ban = randSource.BicAndBan(acs[0].bic, acs[0].ban)
		}
		InitAccounts(acs, amount)

		cookie := StatsRequestStart()
		err := client.MakeTransfer(acs, amount)
		StatsRequestEnd(cookie)

		if err != nil {
			llog.Errorf("[%v] %v", client.shortId, err)
			atomic.AddUint64(&payStats.errors, 1)
			return
		}
	}
}

func pay(settings *Settings) error {

	llog.Infof("Making %d transfers using %d workers on %d cores \n",
		settings.count, settings.workers, runtime.NumCPU())

	cluster := gocql.NewCluster(settings.host)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: settings.user,
		Password: settings.password,
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
	var oracle *Oracle

	if settings.oracle {
		oracle = new(Oracle)
		oracle.Init(session)
	}

	transfers_per_worker := settings.count / settings.workers
	remainder := settings.count - transfers_per_worker*settings.workers

	RecoveryStart(session, oracle, &payStats)

	for i := 0; i < settings.workers; i++ {
		wg.Add(1)
		n_transfers := transfers_per_worker
		if i < remainder {
			n_transfers++
		}
		go payWorker(n_transfers, settings.zipfian, session, oracle, &payStats, &wg)
	}

	wg.Wait()
	RecoveryStop()
	if oracle != nil {
		oracle.FindBrokenAccounts(session)
	}

	llog.Infof("Errors: %v, Retries: %v, Recoveries: %v, Not found: %v\n",
		payStats.errors,
		payStats.retries,
		payStats.recoveries,
		payStats.no_such_account)

	return nil
}
