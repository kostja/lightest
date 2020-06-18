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

func IsTransientError(err error) bool {
	reqErr, isRequestErr := err.(gocql.RequestError)
	if isRequestErr && reqErr != nil {
		return true
	} else if err == gocql.ErrTimeoutNoResponse {
		return true
	} else {
		return false
	}
}

var nClients uint64
var nTransfers uint64

type Row = map[string]interface{}

var nilUuid gocql.UUID

type TransferId = gocql.UUID

func NewTransferId() TransferId {
	return gocql.TimeUUID()
}

type PayStats struct {
	errors             uint64
	no_such_account    uint64
	insufficient_funds uint64
	retries            uint64
	recoveries         uint64
}

type Client struct {
	shortId              uint64     // For logging
	clientId             gocql.UUID // For locking
	session              *gocql.Session
	oracle               *Oracle
	payStats             *PayStats
	insertTransfer       *gocql.Query
	setTransferClient    *gocql.Query
	clearTransferClient  *gocql.Query
	setTransferState     *gocql.Query
	deleteTransfer       *gocql.Query
	fetchTransfer        *gocql.Query
	fetchTransferClient  *gocql.Query
	lockAccount          *gocql.Query
	unlockAccount        *gocql.Query
	fetchAccountBalance  *gocql.Query
	updateAccountBalance *gocql.Query
}

type Account struct {
	bic            string
	ban            string
	balance        *inf.Dec
	pending_amount *inf.Dec
	lockOrder      int
	found          bool
}

type Transfer struct {
	id     TransferId
	acs    []Account
	amount *inf.Dec
	state  string
}

func (t *Transfer) InitAccounts() {
	if t.amount == nil {
		llog.Fatalf("[%v] Found transfer with nil amount", t.id)
	}
	acs := t.acs
	// Always lock accounts in lexicographical order to avoid livelocks
	if acs[1].bic > acs[0].bic ||
		acs[1].bic == acs[0].bic &&
			acs[1].ban > acs[0].ban {
		acs[1].lockOrder = 1
	} else {
		acs[0].lockOrder = 1
	}
	// Use pending amount as a flag to avoid double transfer on recover
	acs[0].pending_amount = new(inf.Dec).Neg(t.amount)
	acs[1].pending_amount = t.amount
}

func (t *Transfer) InitRandomTransfer(randSource *FixedRandomSource, zipfian bool) {
	t.amount = randSource.NewTransferAmount()
	t.acs = make([]Account, 2, 2)
	if zipfian {
		t.acs[0].bic, t.acs[0].ban = randSource.HotBicAndBan()
		t.acs[1].bic, t.acs[1].ban = randSource.HotBicAndBan(t.acs[0].bic, t.acs[0].ban)
	} else {
		t.acs[0].bic, t.acs[0].ban = randSource.BicAndBan()
		t.acs[1].bic, t.acs[1].ban = randSource.BicAndBan(t.acs[0].bic, t.acs[0].ban)
	}
	t.id = NewTransferId()
	t.state = "new"
	t.InitAccounts()
}

func (t *Transfer) InitEmptyTransfer(id TransferId) {
	t.id = id
	t.acs = make([]Account, 2, 2)
}

func (t *Transfer) String() string {
	return fmt.Sprintf("transfer from %v:%v (%v) to %v:%v (%v) - %v",
		t.acs[0].bic, t.acs[0].ban, t.acs[0].balance,
		t.acs[1].bic, t.acs[1].ban, t.acs[1].balance,
		t.amount)
}

func (c *Client) Init(session *gocql.Session, oracle *Oracle, payStats *PayStats) {
	c.clientId = gocql.TimeUUID()
	c.shortId = atomic.AddUint64(&nClients, 1)
	llog.Tracef("[%v] Assigning client id %v", c.shortId, c.clientId)
	c.session = session
	c.oracle = oracle
	c.payStats = payStats
	c.insertTransfer = session.Query(INSERT_TRANSFER)
	c.setTransferClient = session.Query(SET_TRANSFER_CLIENT)
	c.setTransferState = session.Query(SET_TRANSFER_STATE)
	c.clearTransferClient = session.Query(CLEAR_TRANSFER_CLIENT)
	c.deleteTransfer = session.Query(DELETE_TRANSFER)
	c.fetchTransfer = session.Query(FETCH_TRANSFER)
	c.fetchTransfer.SerialConsistency(gocql.Serial)
	c.fetchTransferClient = session.Query(FETCH_TRANSFER_CLIENT)
	c.fetchTransferClient.SerialConsistency(gocql.Serial)
	c.lockAccount = session.Query(LOCK_ACCOUNT)
	c.unlockAccount = session.Query(UNLOCK_ACCOUNT)
	c.updateAccountBalance = session.Query(UPDATE_BALANCE)
	c.fetchAccountBalance = session.Query(FETCH_BALANCE)
	c.fetchAccountBalance.SerialConsistency(gocql.Serial)
}

func (c *Client) RegisterTransfer(t *Transfer) error {

	llog.Tracef("[%v] [%v] Registering %v", c.shortId, t.id, t)
	// Register a new transfer
	cql := c.insertTransfer
	cql.Bind(t.id, t.acs[0].bic, t.acs[0].ban, t.acs[1].bic, t.acs[1].ban, t.amount)
	row := Row{}
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err == nil && !applied {
			// Should never happen, transfer id is globally unique
			llog.Fatalf("[%v] [%v] Failed to create: a duplicate transfer exists",
				c.shortId, t.id)
		}
		return merry.Wrap(err)
	}
	return c.SetTransferClient(t.id)
}

// Accept interfaces to allow nil client id
func (c *Client) SetTransferClient(transferId TransferId) error {

	llog.Tracef("[%v] [%v] Setting client", c.shortId, transferId)

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
			llog.Tracef("[%v] [%v] Failed to set client: no such transfer",
				c.shortId, transferId)
			return merry.Wrap(gocql.ErrNotFound)
		}
		if c.clientId != rowClientId {
			return merry.New(fmt.Sprintf("our id %v, previous id %v",
				c.clientId, rowClientId))
		} // c.clientId == rowClientId
	}
	return nil
}

// Accept interfaces to allow nil client id
func (c *Client) SetTransferState(t *Transfer, state string) error {

	llog.Tracef("[%v] [%v] Setting state %v", c.shortId, t.id, state)

	cql := c.setTransferState
	cql.Bind(state, t.id, c.clientId)
	row := Row{}
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err != nil {
			return merry.Wrap(err)
		}
		rowClientId, exists := row["client_id"]
		if !exists || rowClientId == nilUuid {
			llog.Tracef("[%v] [%v] Failed to set state: no such transfer",
				c.shortId, t.id)
			return merry.Wrap(gocql.ErrNotFound)
		}
		return merry.New(fmt.Sprintf("our id %v, previous id %v",
			c.clientId, rowClientId))
	}
	t.state = state
	return nil
}

// In case we failed for whatever reason try to clean up
// the transfer client, to allow speedy recovery
func (c *Client) ClearTransferClient(transferId TransferId) {
	llog.Tracef("[%v] [%v] Clearing client", c.shortId, transferId)

	cql := c.clearTransferClient
	cql.Bind(transferId, c.clientId)
	row := Row{}
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err != nil {
			llog.Errorf("[%v] [%v] Failed to clear transfer client: %v",
				c.shortId, transferId, err)
		} else if !applied {
			rowClientId, exists := row["client_id"]
			if !exists || rowClientId == nilUuid {
				// The transfer is gone, do not complain
			} else {
				err = merry.New(fmt.Sprintf("Client id mismatch: %v != %v",
					c.clientId, row["client_id"]))
				llog.Errorf("[%v] [%v] Failed to clear transfer client: %v",
					c.shortId, transferId, err)
			}
		}
	}
}

func (c *Client) FetchAccountBalance(acc *Account) error {
	cql := c.fetchAccountBalance
	cql.Bind(acc.bic, acc.ban)
	if err := cql.Scan(&acc.balance, &acc.pending_amount); err != nil {
		return err
	}
	acc.found = true
	return nil
}

func (c *Client) UnlockAccount(transferId TransferId, account *Account) error {
	return c.unlockAccount.Bind(account.bic, account.ban, transferId).Exec()
}

func (c *Client) LockAccounts(t *Transfer, wait bool) error {

	if t.state == "complete" {
		return nil
	}
	if t.state == "locked" {
		// The transfer is already locked - fetch balance to find out if the
		// account exists or not
		for i := 0; i < 2; i++ {
			if err := c.FetchAccountBalance(&t.acs[i]); err != nil && err != gocql.ErrNotFound {
				return merry.Wrap(err)
			}
		}
		llog.Tracef("[%v] [%v] Fetched locked %v", c.shortId, t.id, t)
		return nil
	}

	llog.Tracef("[%v] [%v] Locking %v", c.shortId, t.id, t)
	sleepDuration := time.Millisecond*time.Duration(rand.Intn(10)) + time.Millisecond
	maxSleepDuration, _ := time.ParseDuration("10s")

	// Upon failure to take lock on the second account, we should try to rollback
	// lock on the first to avoid deadlocks. We shouldn't, however, accidentally
	// rollback the lock if we haven't taken it - in this case lock0
	// and lock1 both may have been taken, and the transfer have progressed
	// to moving the funds, so rolling back the lock would break isolation.
	var previousAccount *Account

	var i = 0
	for i < 2 {
		account := &t.acs[t.acs[i].lockOrder]
		cql := c.lockAccount
		cql.Bind(t.id, account.pending_amount, account.bic, account.ban)
		row := Row{}
		// If the update is not applied because we've already locked the
		// transfer, it's a success. This is possible during recovery.
		lockFailed := func(applied bool) bool {
			if applied {
				return false
			}
			// pendingTransfer may be missing from returns (Cassandra)
			pendingTransfer, exists := row["pending_transfer"].(TransferId)
			if exists && pendingTransfer == t.id {
				return false
			}
			return true
		}
		if applied, err := cql.MapScanCAS(row); err != nil || lockFailed(applied) {
			// Remove the pending transfer from the previously
			// locked account, do not wait with locks.
			if i == 1 && previousAccount != nil {
				if err1 := c.UnlockAccount(t.id, previousAccount); err1 != nil {
					return merry.WithCause(err1, err)
				}
			}
			// Check for transient errors, such as query timeout, and retry.
			// In case of a non-transient error, return it to the client.
			// No money changed its hands and the transfer can be recovered
			// later
			if err != nil {
				if IsTransientError(err) {
					llog.Tracef("[%v] [%v] Retrying after error: %v", c.shortId, t.id, err)
				} else {
					return merry.Wrap(err)
				}
			} else {
				// Lock failed because of a conflict or account is missing.
				pendingTransfer, exists := row["pending_transfer"].(TransferId)
				if !exists || pendingTransfer == nilUuid {
					// No such account. We're not holding locks. CompleteTransfer() will delete
					// the transfer.
					return c.SetTransferState(t, "locked")
				}
				// There is a non-empty pending transfer. Check if the
				// transfer we've conflicted with is orphaned and recover
				// it, before waiting
				var clientId gocql.UUID
				c.fetchTransferClient.Bind(pendingTransfer)
				if err := c.fetchTransferClient.Scan(&clientId); err != nil {
					if err != gocql.ErrNotFound {
						return err
					}
					// Transfer not found, even though it's just aborted
					// our lock. It is OK, it might just got completed.
					llog.Tracef("[%v] [%v] Transfer %v which aborted our lock is now gone",
						c.shortId, t.id, pendingTransfer)
				} else if clientId == nilUuid {
					// The transfer has no client working on it, recover it.
					llog.Tracef("[%v] [%v] Adding %v to the recovery queue",
						c.shortId, t.id, pendingTransfer)
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

			llog.Tracef("[%v] [%v] Restarting after sleeping %v",
				c.shortId, t.id, sleepDuration)

			sleepDuration = sleepDuration * 2
			if sleepDuration > maxSleepDuration {
				sleepDuration = maxSleepDuration
			}
			t.acs[0].found = false
			t.acs[1].found = false
			previousAccount = nil
			// Reset client id in case it expired while we were sleeping
			if err := c.SetTransferClient(t.id); err != nil {
				return err
			}
		} else {
			if applied {
				previousAccount = account
			}
			// In Scylla, the previous row returned even if LWT is applied.
			// In Cassandra, make a separate query.
			if account.balance, account.found = row["balance"].(*inf.Dec); !account.found {
				// Support Cassandra which doens't provide balance
				if err = c.FetchAccountBalance(account); err != nil {
					return merry.Wrap(err)
				}
			} else if !applied {
				// Fetch previous pending amount
				account.pending_amount = row["pending_amount"].(*inf.Dec)
			}
			i++
		}
	}
	// Move transfer to 'locked', to not attempt to transfer
	// 	the money twice during recovery
	return c.SetTransferState(t, "locked")
}

func (c *Client) CompleteTransfer(t *Transfer) error {

	if t.state != "locked" && t.state != "complete" {
		llog.Fatalf("[%v] [%v] Incorrect transfer state", c.shortId, t.id)
	}

	acs := t.acs
	if t.state == "locked" {

		if c.oracle != nil {
			c.oracle.BeginTransfer(t.id, acs, t.amount)
		}

		if acs[0].found && acs[1].found {
			// Calcualte the destination state
			llog.Tracef("[%v] [%v] Calculating balances for %v", c.shortId, t.id, t)
			for i := 0; i < 2; i++ {
				acs[i].balance.Add(acs[i].balance, acs[i].pending_amount)
			}

			if acs[0].balance.Sign() >= 0 {

				llog.Tracef("[%v] [%v] Moving funds for %v", c.shortId, t.id, t)

				// From now on we can ignore 'applied' - the record may
				// not be applied only if someone completed our transfer or
				// 30 seconds have elapsed.
				cql := c.updateAccountBalance
				for i := 0; i < 2; i++ {
					cql.Bind(acs[i].balance, acs[i].bic, acs[i].ban, t.id)
					if err := cql.Exec(); err != nil {
						llog.Tracef("[%v] [%v] Failed to set account %v %v:%v to %v",
							c.shortId, t.id, i, acs[i].bic, acs[i].ban, acs[i].balance)
						return merry.Wrap(err)
					}
				}
			} else {
				llog.Tracef("[%v] [%v] Insufficient funds for %v", c.shortId, t.id, t)
				atomic.AddUint64(&c.payStats.insufficient_funds, 1)
			}
		} else {
			llog.Tracef("[%v] [%v] Account not found for %v", c.shortId, t.id, t)

			atomic.AddUint64(&c.payStats.no_such_account, 1)
		}
		if c.oracle != nil {
			c.oracle.CompleteTransfer(t.id, acs, t.amount)
		}
		if err := c.SetTransferState(t, "complete"); err != nil {
			return err
		}
	}

	llog.Tracef("[%v] [%v] Unlocking %v", c.shortId, t.id, t)

	for i := 0; i < 2; i++ {
		if err := c.UnlockAccount(t.id, &acs[i]); err != nil {
			llog.Tracef("[%v] [%v] Failed to unlock account %v %v:%v: %v",
				c.shortId, t.id, i, acs[i].bic, acs[i].ban, err)
			return err
		}
	}

	return c.DeleteTransfer(t.id)
}

func (c *Client) DeleteTransfer(transferId TransferId) error {
	// Move transfer to "complete". Typically a transfer is kept
	// for a few years, we just delete it for simplicity.
	row := Row{}
	cql := c.deleteTransfer
	cql.Bind(transferId, c.clientId)
	if applied, err := cql.MapScanCAS(row); err != nil || !applied {
		if err != nil {
			llog.Tracef("[%v] [%v] Failed to delete transfer: %v", c.shortId, transferId, err)
			return merry.Wrap(err)
		}
		rowClientId, exists := row["client_id"]
		if exists && rowClientId != nilUuid {
			return merry.New(fmt.Sprintf("[%v] [%v] Delete failed, client id %v does not match row client id %v",
				c.shortId, transferId, c.clientId, rowClientId))
		}
		llog.Tracef("[%v] [%v] Transfer is already deleted", c.shortId, transferId)
		return nil
	}
	llog.Tracef("[%v] [%v] Deleted transfer", c.shortId, transferId)
	return nil
}

func (c *Client) MakeTransfer(t *Transfer) error {

	if err := c.RegisterTransfer(t); err != nil {
		return merry.Wrap(err)
	}
	if err := c.LockAccounts(t, true); err != nil {
		return merry.Wrap(err)
	}
	return c.CompleteTransfer(t)
}

func (c *Client) RecoverTransfer(transferId TransferId) {
	cookie := StatsRequestStart()
	llog.Tracef("[%v] [%v] Recovering transfer", c.shortId, transferId)
	atomic.AddUint64(&c.payStats.recoveries, 1)
	if err := c.SetTransferClient(transferId); err != nil {
		if !merry.Is(err, gocql.ErrNotFound) {
			llog.Errorf("[%v] [%v] Failed to set client on transfer: %v",
				c.shortId, transferId, err)
		}
		return
	}
	t := new(Transfer)
	t.InitEmptyTransfer(transferId)
	cql := c.fetchTransfer
	cql.Bind(transferId)
	// Ignore possible error, we will retry
	if err := cql.Scan(&t.acs[0].bic, &t.acs[0].ban, &t.acs[1].bic,
		&t.acs[1].ban, &t.amount, &t.state); err != nil {

		if err == gocql.ErrNotFound {
			llog.Errorf("[%v] [%v] Transfer not found when fetching for recovery",
				c.shortId, transferId)
		} else {
			llog.Errorf("[%v] [%v] Failed to fetch transfer: %v",
				c.shortId, transferId, err)
		}
		return
	}
	t.InitAccounts()
	if err := c.LockAccounts(t, false); err != nil {
		llog.Errorf("[%v] [%v] Failed to lock accounts: %v",
			c.shortId, t.id, err)
		c.ClearTransferClient(t.id)
		return
	}
	if err := c.CompleteTransfer(t); err != nil {
		llog.Errorf("[%v] [%v] Failed to complete transfer: %v",
			c.shortId, t.id, err)
	} else {
		StatsRequestEnd(cookie)
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

	for i := 0; i < n_transfers; {

		t := new(Transfer)
		t.InitRandomTransfer(&randSource, zipfian)

		cookie := StatsRequestStart()
		if err := client.MakeTransfer(t); err != nil {
			if merry.Is(err, gocql.ErrNotFound) {
				llog.Tracef("[%v] [%v] Transfer not found", client.shortId, t.id, err)
			} else if IsTransientError(err) {
				llog.Tracef("[%v] [%v] Transfer failed: %v", client.shortId, t.id, err)
			} else {
				return
			}
		} else {
			i++
			StatsRequestEnd(cookie)
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
	StatsReportSummary()
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
