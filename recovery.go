package main

import (
	"github.com/gocql/gocql"
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
	q.queue = make(chan gocql.UUID, 4096)
	q.done = make(chan bool, 1)
	go recoveryWorker(session, payStats)
}

func RecoveryStop() {
	close(q.queue)
	<-q.done
}
