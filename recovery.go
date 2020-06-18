package main

import (
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"sync"
)

func recoveryWorker(session *gocql.Session, oracle *Oracle, payStats *PayStats,
	wg *sync.WaitGroup) {

	defer wg.Done()

	var c = Client{}
	c.Init(session, oracle, payStats)

loop:
	for {
		transfer_id, more := <-q.queue
		if !more {
			break loop
		}
		c.RecoverTransfer(transfer_id)
	}
}

type RecoveryQueue struct {
	queue    chan TransferId
	wg       sync.WaitGroup
	session  *gocql.Session
	oracle   *Oracle
	payStats *PayStats
}

func (q *RecoveryQueue) Init(session *gocql.Session, oracle *Oracle, payStats *PayStats) {

	q.session = session
	q.oracle = oracle
	q.payStats = payStats
	// Recovery is recursive, create the channels first
	q.queue = make(chan TransferId, 4096000)
}

func (q *RecoveryQueue) StartRecoveryWorker() {
	q.wg.Add(1)
	go recoveryWorker(q.session, q.oracle, q.payStats, &q.wg)
}

func (q *RecoveryQueue) Stop() {
	close(q.queue)
	q.wg.Wait()
}

var q RecoveryQueue

func RecoverTransfer(transferId TransferId) {
	q.queue <- transferId
}

func Recover() {

	var c = Client{}
	c.Init(q.session, q.oracle, q.payStats)

	for {
		iter := q.session.Query(FETCH_DEAD_TRANSFERS).Iter()
		closeIter := func() {
			if err := iter.Close(); err != nil {
				llog.Errorf("Failed to fetch dead transfers: %v", err)
			}
		}
		defer closeIter()
		if iter.NumRows() == 0 {
			break
		}
		// Ignore possible errors
		llog.Infof("Found %v outstanding transfers, recovering...", iter.NumRows())
		var transferId TransferId
		for iter.Scan(&transferId) {
			c.RecoverTransfer(transferId)
		}
	}
}

func RecoveryStart(session *gocql.Session, oracle *Oracle, payStats *PayStats) {

	q.Init(session, oracle, payStats)

	// Start background fiber working on the queue to
	// make sure we purge it even during the initial recovery
	for i := 0; i < 8; i++ {
		q.StartRecoveryWorker()
	}

	Recover()
}

func RecoveryStop() {
	Recover()
	q.Stop()
}
