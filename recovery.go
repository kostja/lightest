package main

import (
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"sync"
)

func recoveryWorker(session *gocql.Session, payStats *PayStats,
	wg *sync.WaitGroup) {

	defer wg.Done()

	var c = Client{}
	c.Init(session, payStats)

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
	queue    chan gocql.UUID
	wg       sync.WaitGroup
	session  *gocql.Session
	payStats *PayStats
}

func (q *RecoveryQueue) Init(session *gocql.Session, payStats *PayStats) {

	q.session = session
	q.payStats = payStats
	// Recovery is recursive, create the channels first
	q.queue = make(chan gocql.UUID, 4096000)
}

func (q *RecoveryQueue) StartRecoveryWorker() {
	q.wg.Add(1)
	go recoveryWorker(q.session, q.payStats, &q.wg)
}

func (q *RecoveryQueue) Stop() {
	close(q.queue)
	q.wg.Wait()
}

var q RecoveryQueue

func Recover(uuid gocql.UUID) {
	q.queue <- uuid
}

func RecoveryStart(session *gocql.Session, payStats *PayStats) {

	q.Init(session, payStats)

	// Start background fiber working on the queue to
	// make sure we purge it even during the initial recovery
	q.StartRecoveryWorker()

	var c = Client{}
	c.Init(session, payStats)

	for {
		iter := session.Query(FETCH_DEAD_TRANSFERS).Iter()
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
		var transfer_id gocql.UUID
		for iter.Scan(&transfer_id) {
			c.RecoverTransfer(transfer_id)
		}
	}
}

func RecoveryStop() {
	q.Stop()
}
