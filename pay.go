package main

import (
	"fmt"
	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
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
	update_balance *gocql.Query
}

func (t *Transfer) New(session *gocql.Session) {
	t.insert = session.Query(INSERT_TRANSFER)
	t.update = session.Query(UPDATE_TRANSFER)
	t.delete_ = session.Query(DELETE_TRANSFER)
	t.lock_account = session.Query(LOCK_ACCOUNT)
	t.update_balance = session.Query(UPDATE_BALANCE)
}

func (t *Transfer) Make(
	client_id gocql.UUID,
	src_bic string, src_ban string,
	dst_bic string, dst_ban string,
	amount *inf.Dec, stats *PayStats) error {

	type Row = map[string]interface{}
	var row Row

	// Register a new transfer
	transfer_id := gocql.TimeUUID()
	row = Row{}
	t.insert.Bind(transfer_id, src_bic, src_ban, dst_bic, dst_ban, amount)
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

	// Always lock the bigger account first, to avoid deadlocks
	var lockOrder [2]struct {
		bic string
		ban string
	}
	if dst_bic > src_bic || dst_bic == src_bic && dst_ban > src_ban {
		lockOrder[0].bic = src_bic
		lockOrder[0].ban = src_ban
		lockOrder[1].bic = dst_bic
		lockOrder[1].ban = dst_ban
	} else {
		lockOrder[0].bic = dst_bic
		lockOrder[0].ban = dst_ban
		lockOrder[1].bic = src_bic
		lockOrder[1].ban = src_ban
	}

	sleepDuration := time.Millisecond * time.Duration(rand.Intn(10))
	maxSleepDuration, _ := time.ParseDuration("10s")
	var i = 0
	for i < 2 {
		t.lock_account.Bind(transfer_id, lockOrder[i].bic, lockOrder[i].ban, nil)
		row = Row{}
		if applied, err := t.lock_account.MapScanCAS(row); !applied || err != nil {
			if i == 1 {
				t.lock_account.Bind(nil, lockOrder[0].bic, lockOrder[0].ban, transfer_id)
				if err1 := t.lock_account.Exec(); err1 != nil {
					return merry.WithCause(err1, err)
				}
			}
			// Here we should check for transient errors, such as
			// query timeout, and retry.
			// Not doing it in scope of a demo
			if err != nil {
				return merry.Wrap(err)
			}
			if len(row) == 0 {
				atomic.AddUint64(&stats.no_such_account, 1)
				llog.Printf("Account %v not found, ending transfer %v", src_ban, transfer_id)
				return t.SetTransferState(client_id, nil, transfer_id, "complete")
			}
			// pending_transfer != null
			// @todo: check if it was orphaned and repair
			llog.Printf("Restarting because there is a pending transfer %v", row["pending_transfer"])
			time.Sleep(sleepDuration)
			sleepDuration := sleepDuration * 2
			if sleepDuration > maxSleepDuration {
				sleepDuration = maxSleepDuration
			}
		}
		i++
	}
	return t.CompleteLockedTransfer(client_id, transfer_id, src_bic, src_ban,
		dst_bic, dst_ban, amount)
}

func (t *Transfer) CompleteLockedTransfer(
	client_id gocql.UUID, transfer_id gocql.UUID,
	src_bic string, src_ban string,
	dst_bic string, dst_ban string,
	amount *inf.Dec) error {

	// From now on we can ignore 'applied' - the record may
	// not be applied only if someone completed our transfer or
	// 300 seconds have elapsed.
	// Move transfer to 'in progress', to not attempt to transfer the money
	// twice
	t.update.Bind(client_id, "in progress", transfer_id, client_id)
	if err := t.update.Exec(); err != nil {
		return merry.Wrap(err)
	}

	new_src_balance := amount
	t.update_balance.Bind(new_src_balance, src_bic, src_ban, transfer_id)
	if err := t.update_balance.Exec(); err != nil {
		return merry.Wrap(err)
	}
	new_dst_balance := amount
	t.update_balance.Bind(new_dst_balance, dst_bic, dst_ban, transfer_id)
	if err := t.update_balance.Exec(); err != nil {
		return merry.Wrap(err)
	}
	llog.Printf("Transfer %v of %v complete", transfer_id, amount)
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
	fmt.Printf("Making %d transfers using %d workers on %d cores \n",
		n, workers, cores)

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
		return merry.Wrap(err)
	}
	var stats PayStats
	var rand FixedRandomSource
	rand.Init(session)

	worker := func(client_id gocql.UUID, n_transfers int, wg *sync.WaitGroup) {

		defer wg.Done()

		var transfer Transfer
		transfer.New(session)

		for i := 0; i < n_transfers; i++ {

			amount := rand.NewTransferAmount()
			src_bic, src_ban := rand.BicAndBan()
			dst_bic, dst_ban := rand.BicAndBan(src_bic, src_ban)

			err := transfer.Make(client_id, src_bic, src_ban, dst_bic, dst_ban, amount, &stats)
			if err != nil {
				llog.Printf("%+v", err)
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

	fmt.Printf("Errors: %v, Retries: %v, Recoveries: %v, Not found: %v\n",
		stats.errors,
		stats.retries,
		stats.recoveries,
		stats.no_such_account)

	return nil
}
