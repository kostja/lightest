package main

import (
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"gopkg.in/inf.v0"
	"sync"
)

type TrackingAccount struct {
	bic        string
	ban        string
	balance    *inf.Dec
	transferId TransferId
}

func (acc *TrackingAccount) setTransfer(transferId TransferId) {
	if acc.transferId != nilUuid && acc.transferId != transferId {
		llog.Fatalf("setTransfer() on %v:%v: current transfer id %v, setting %v",
			acc.bic, acc.ban, acc.transferId, transferId)
	}
	acc.transferId = transferId
}

func (acc *TrackingAccount) clearTransfer(transferId TransferId) {
	if acc.transferId != nilUuid && acc.transferId != transferId {
		llog.Fatalf("clearTransfer() on %v:%v: current transfer id %v, clearing %v",
			acc.bic, acc.ban, acc.transferId, transferId)
	}
	acc.transferId = nilUuid
}

func (acc *TrackingAccount) BeginDebit(transferId TransferId, amount *inf.Dec) {
	acc.setTransfer(transferId)
}

func (acc *TrackingAccount) CompleteDebit(transferId TransferId, amount *inf.Dec) {
	acc.clearTransfer(transferId)
	acc.balance.Sub(acc.balance, amount)
}

func (acc *TrackingAccount) BeginCredit(transferId TransferId, amount *inf.Dec) {
	acc.setTransfer(transferId)
}

func (acc *TrackingAccount) CompleteCredit(transferId TransferId, amount *inf.Dec) {
	acc.clearTransfer(transferId)
	acc.balance.Add(acc.balance, amount)
}

type Oracle struct {
	acs       map[string]*TrackingAccount
	transfers map[TransferId]bool
	mux       sync.Mutex
}

func (o *Oracle) Init(session *gocql.Session) {

	o.acs = make(map[string]*TrackingAccount)
	o.transfers = make(map[TransferId]bool)
	iter := session.Query("SELECT bic, ban, balance FROM accounts").Iter()
	var bic, ban string
	var balance *inf.Dec
	for iter.Scan(&bic, &ban, &balance) {

		o.acs[bic+ban] = &TrackingAccount{
			bic:     bic,
			ban:     ban,
			balance: balance,
		}
	}
	if err := iter.Close(); err != nil {
		llog.Fatalf("%v", err)
	}
}

func (o *Oracle) lookupAccounts(acs []Account) (*TrackingAccount, *TrackingAccount) {
	from, from_found := o.acs[acs[0].bic+acs[0].ban]
	to, to_found := o.acs[acs[1].bic+acs[1].ban]
	if (!from_found || !to_found) && acs[0].found && acs[1].found {
		llog.Fatalf("One of the accounts is found, while it's missing")
	}
	return from, to
}

func (o *Oracle) BeginTransfer(transferId TransferId, acs []Account, amount *inf.Dec) {
	o.mux.Lock()
	defer o.mux.Unlock()
	if _, exists := o.transfers[transferId]; exists {
		llog.Tracef("Double execution of the same transfer %v", transferId)
		// Have processed this transfer already
		return
	}
	if from, to := o.lookupAccounts(acs); from != nil && to != nil && amount.Cmp(from.balance) <= 0 {
		from.BeginDebit(transferId, amount)
		to.BeginCredit(transferId, amount)
	}
}

func (o *Oracle) CompleteTransfer(transferId TransferId, acs []Account, amount *inf.Dec) {
	o.mux.Lock()
	defer o.mux.Unlock()
	if _, exists := o.transfers[transferId]; exists {
		// Have processed this transfer already
		return
	}
	o.transfers[transferId] = true
	if from, to := o.lookupAccounts(acs); from != nil && to != nil && amount.Cmp(from.balance) <= 0 {
		from.CompleteDebit(transferId, amount)
		to.CompleteCredit(transferId, amount)
	}
}

func (o *Oracle) FindBrokenAccounts(session *gocql.Session) {
	for _, acc := range o.acs {
		cql := session.Query("select balance from lightest.accounts where bic = ? and ban = ?")
		cql.SerialConsistency(gocql.Serial)
		var balance *inf.Dec
		cql.Bind(acc.bic, acc.ban).Scan(&balance)
		if balance.Cmp(acc.balance) != 0 {
			llog.Errorf("%v:%v balance is %v should be %v", acc.bic, acc.ban, balance, acc.balance)
		}
	}
}
