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
	amount inf.Dec, stats *PayStats) error {

	var applied bool
	var err error
	row := map[string]interface{}{}

	// Register a new transfer
	transfer_id := gocql.TimeUUID()
	t.insert.Bind(transfer_id, src_bic, src_ban, dst_bic, dst_ban, amount)
	if applied, err = t.insert.MapScanCAS(row); err != nil {
		return merry.Wrap(err)
	}
	if applied != true {
		// Should never happen, transfer id is globally unique
		return merry.New(fmt.Sprintf("Failed to create a transfer %v: a duplicate transfer exists",
			transfer_id))
	}
	// Change transfer state
	t.update.Bind(client_id, "pending", transfer_id, nil)
	if applied, err = t.update.MapScanCAS(row); err != nil {
		return merry.Wrap(err)
	}
	if applied != true {
		// Should never happen, noone can pick up our transfer but us
		return merry.New(fmt.Sprintf("Failed to update transfer %v",
			transfer_id))
	}
	// Lock source account
	t.check_src_account.Bind(transfer_id, src_bic, src_ban, amount)
	if applied, err = t.check_src_account.MapScanCAS(row); err != nil {
		return merry.Wrap(err)
	}
	if applied != true {
		// pending_transfer != null
		//	 check pending transfer state:
		//     if the client is there, sleep & restart
		//     else complete  the existing transfer
		// insufficient balance
	}
	// Lock destination account
	t.check_dst_account.Bind(transfer_id, dst_bic, dst_ban)
	if applied, err = t.check_dst_account.MapScanCAS(row); err != nil {
		return merry.Wrap(err)
	}
	if applied != true {
		// check pending transfer state:
		//   if the client is there, sleep & restart
		//   else complete  the existing transfer
	}
	return t.CompleteLockedTransfer(client_id, transfer_id, src_bic, src_ban,
		dst_bic, dst_ban, amount)
}

func (t *Transfer) CompleteLockedTransfer(
	client_id gocql.UUID, transfer_id gocql.UUID,
	src_bic string, src_ban string,
	dst_bic string, dst_ban string,
	amount inf.Dec) error {

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

	worker := func(client_id gocql.UUID, wg *sync.WaitGroup) {

		defer wg.Done()

		rand := rand.New(rand.NewSource(time.Now().UnixNano()))

		var transfer Transfer
		transfer.New(session)

		for i := 0; i < n/workers; i++ {

			amount := inf.NewDec(rand.Int63n(1000), inf.Scale(rand.Int63n(100)))
			src_bic := gocql.TimeUUID().String()
			src_ban := gocql.TimeUUID().String()
			dst_bic := gocql.TimeUUID().String()
			dst_ban := gocql.TimeUUID().String()

			err := transfer.Make(client_id, src_bic, src_ban, dst_bic, dst_ban, *amount, &stats)
			if err != nil {
				llog.Printf("%+v", err)
				atomic.AddUint64(&stats.errors, 1)
				return
			}
		}

	}
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker(gocql.TimeUUID(), &wg)
	}

	wg.Wait()

	fmt.Printf("Errors: %v, Retries: %v, Recoveries: %v, Not found: %v\n",
		stats.errors,
		stats.retries,
		stats.recoveries,
		stats.no_such_account)

	return nil
}
