package main

import (
	"fmt"
	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
	"gopkg.in/inf.v0"
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
	insert            *gocql.Query
	update            *gocql.Query
	delete_           *gocql.Query
	check_src_account *gocql.Query
	check_dst_account *gocql.Query
	update_balance    *gocql.Query
}

func (t *Transfer) New(session *gocql.Session) {
	t.insert = session.Query(INSERT_TRANSFER)
	t.update = session.Query(UPDATE_TRANSFER)
	t.delete_ = session.Query(DELETE_TRANSFER)
	t.check_src_account = session.Query(CHECK_SRC_ACCOUNT)
	t.check_dst_account = session.Query(CHECK_DST_ACCOUNT)
	t.update_balance = session.Query(UPDATE_BALANCE)
}

func (t *Transfer) Make(
	client_id gocql.UUID,
	src_bic string, src_ban string,
	dst_bic string, dst_ban string,
	amount *inf.Dec, stats *PayStats) error {

	var applied bool
	var err error
	type Row = map[string]interface{}
	var row Row

	// Register a new transfer
	transfer_id := gocql.TimeUUID()
	row = Row{}
	t.insert.Bind(transfer_id, src_bic, src_ban, dst_bic, dst_ban, amount)
	if applied, err = t.insert.MapScanCAS(row); err != nil {
		return merry.Wrap(err)
	}
	if applied != true {
		// Should never happen, transfer id is globally unique
		return merry.New(fmt.Sprintf("Failed to create a transfer %v: a duplicate transfer exists",
			transfer_id))
	}
	// Change transfer state to pending and set client id
	t.update.Bind(client_id, "pending", transfer_id, nil)
	row = Row{}
	if applied, err = t.update.MapScanCAS(row); err != nil {
		return merry.Wrap(err)
	}
	if applied != true {
		// Should never happen, noone can pick up our transfer but us
		return merry.New(fmt.Sprintf("Failed to update transfer %v",
			transfer_id))
	}

	applied = false
	sleepDuration, _ := time.ParseDuration("0.1s")
	maxSleepDuration, _ := time.ParseDuration("10s")

	for applied == false {

		newSleepDuration := sleepDuration * 2
		if newSleepDuration > maxSleepDuration {
			newSleepDuration = maxSleepDuration
		}

		// Lock source account
		t.check_src_account.Bind(transfer_id, src_bic, src_ban, amount)
		row = Row{}
		if applied, err = t.check_src_account.MapScanCAS(row); err != nil {
			return merry.Wrap(err)
		}
		llog.Printf("%v", row)
		if applied != true {
			if len(row) == 0 {
				atomic.AddUint64(&stats.no_such_account, 1)
				return nil
			}
			// pending_transfer != null
			//	 check pending transfer state:
			//     if the client is there, sleep & restart
			//     else complete  the existing transfer
			// insufficient balance
			time.Sleep(sleepDuration)
			sleepDuration = newSleepDuration
			continue
		}
		// Lock destination account
		t.check_dst_account.Bind(transfer_id, dst_bic, dst_ban)
		row = Row{}
		if applied, err = t.check_dst_account.MapScanCAS(row); err != nil {
			return merry.Wrap(err)
		}
		llog.Printf("%v", row)
		if applied != true {
			if len(row) == 0 {
				atomic.AddUint64(&stats.no_such_account, 1)
				return nil
			}
			// check pending transfer state:
			//   if the client is there, sleep & restart
			//   else complete  the existing transfer
			time.Sleep(sleepDuration)
			sleepDuration = newSleepDuration
			continue
		}
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
	// Move transfer to "complete"
	t.delete_.Bind(transfer_id, client_id, "in progress")
	if err := t.delete_.Exec(); err != nil {
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
			fmt.Printf("%s %s\n", src_bic, src_ban)

			dst_bic, dst_ban := rand.BicAndBan(src_bic, src_ban)
			fmt.Printf("%s %s\n", dst_bic, dst_ban)

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
