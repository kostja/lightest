package main

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/spf13/cobra"
	"gopkg.in/inf.v0"
	"math/rand"
	"sync"
	"time"
)

func recover1() error {
	return nil
}

type Transfer struct {
	insert            *gocql.Query
	update            *gocql.Query
	check_src_account *gocql.Query
	check_dst_account *gocql.Query
	update_balance    *gocql.Query
}

func (t *Transfer) New(session *gocql.Session) {
	t.insert = session.Query(INSERT_TRANSFER)
	t.update = session.Query(UPDATE_TRANSFER)
	t.check_src_account = session.Query(CHECK_SRC_ACCOUNT)
	t.check_dst_account = session.Query(CHECK_DST_ACCOUNT)
	t.update_balance = session.Query(UPDATE_BALANCE)
}

func (t *Transfer) Make(client_id gocql.UUID, src_bic string, src_ban string,
	dst_bic string, dst_ban string, amount inf.Dec) error {

	// Register a new transfer
	transfer_id := gocql.TimeUUID().String()
	t.insert.Bind(transfer_id, src_bic, src_ban, dst_bic, dst_ban, amount)
	if err := t.insert.Exec(); err != nil {
		return err
	}
	// Change transfer state
	t.update.Bind(client_id, "pending", transfer_id, nil)
	if err := t.update.Exec(); err != nil {
		return err
	}
	// Lock source account
	t.check_src_account.Bind(src_bic, src_ban, transfer_id)
	if err := t.check_src_account.Exec(); err != nil {
		return err
	}
	// Lock destination account
	t.check_dst_account.Bind(dst_bic, dst_ban, transfer_id)
	if err := t.check_dst_account.Exec(); err != nil {
		return err
	}
	// Move transfer to 'in progress', to not attempt to transfer the money
	// twice
	t.update.Bind(client_id, "in progress", transfer_id, client_id)
	if err := t.update.Exec(); err != nil {
		return err
	}
	//
	new_src_balance := amount
	t.update_balance.Bind(src_bic, src_ban, new_src_balance, transfer_id)
	if err := t.update_balance.Exec(); err != nil {
		return err
	}
	new_dst_balance := amount
	t.update_balance.Bind(dst_bic, dst_ban, new_dst_balance, transfer_id)
	if err := t.update_balance.Exec(); err != nil {
		return err
	}

	// Move transfer to "complete"
	t.update.Bind(nil, "complete", transfer_id, client_id)
	if err := t.update.Exec(); err != nil {
		return err
	}
	return nil
}

func pay(cmd *cobra.Command, args []string) error {

	fmt.Printf("Inside pay with args: %v\n", args)
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
		return err
	}

	worker := func(client_id gocql.UUID, wg *sync.WaitGroup) {

		defer wg.Done()

		rand := rand.New(rand.NewSource(time.Now().UnixNano()))

		var transfer Transfer
		transfer.New(session)

		for i := 0; i < 10000; i++ {

			amount := inf.NewDec(rand.Int63n(1000), inf.Scale(rand.Int63n(100)))
			src_bic := gocql.TimeUUID().String()
			src_ban := gocql.TimeUUID().String()
			dst_bic := gocql.TimeUUID().String()
			dst_ban := gocql.TimeUUID().String()

			err := transfer.Make(client_id, src_bic, src_ban, dst_bic, dst_ban, *amount)
			if err != nil {
				return
			}
		}

	}
	var wg sync.WaitGroup

	for i := 1; i <= 1; i++ {
		wg.Add(1)
		go worker(gocql.TimeUUID(), &wg)
	}

	wg.Wait()

	llog.Printf("Done")

	return nil
}
