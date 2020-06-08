package main

import (
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
)

type RecoveryQueue struct {
	queue chan gocql.UUID
	done  chan bool
}

var q RecoveryQueue

func recoveryWorker(c *Client) {

loop:
	for {
		transfer_id, more := <-q.queue
		if !more {
			break loop
		}
		c.RecoverTransfer(transfer_id)
	}
	q.done <- true
}

func Recover(uuid gocql.UUID) {
	q.queue <- uuid
}

func RecoveryStart(session *gocql.Session, payStats *PayStats) {

	// Recovery is recursive, create the channels first
	q.queue = make(chan gocql.UUID, 4096)
	q.done = make(chan bool, 1)

	var c = new(Client)
	c.New(session, payStats)

	iter := session.Query(FETCH_DEAD_TRANSFERS).Iter()
	// Ignore possible errors
	if iter.NumRows() > 0 {
		llog.Infof("Found %v outstanding transfers, recovering...", iter.NumRows())
		var transfer_id gocql.UUID
		for iter.Scan(&transfer_id) {
			c.RecoverTransfer(transfer_id)
		}
	}
	if err := iter.Close(); err != nil {
		llog.Infof("Failed to fetch dead transfers: %v", err)
	}
	go recoveryWorker(c)
}

func RecoveryStop() {
	close(q.queue)
	<-q.done
}
