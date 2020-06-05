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

func recoveryWorker(session *gocql.Session, payStats *PayStats) {

	var c Client
	c.New(session, payStats)

loop:
	for {
		if transfer_id, more := <-q.queue; !more {
			break loop
		} else {
			llog.Infof("Recover: %v", transfer_id)
			if err := c.SetTransferState(nil, c.client_id, transfer_id, "pending", true); err != nil {
				llog.Infof("Failed to recover transfer %v: %v", transfer_id, err)
				continue loop
			}
		}
	}
	q.done <- true
}

func Recover(uuid gocql.UUID) {
	q.queue <- uuid
}

func RecoveryStart(session *gocql.Session, payStats *PayStats) {
	q.queue = make(chan gocql.UUID, 4096)
	q.done = make(chan bool, 1)
	go recoveryWorker(session, payStats)
}

func RecoveryStop() {
	close(q.queue)
	<-q.done
}
