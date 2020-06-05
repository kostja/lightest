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

	var client Client
	client.New(session, payStats)

loop:
	for {
		if uuid, more := <-q.queue; !more {
			break loop
		} else {
			llog.Infof("Recover: %v", uuid)
		}
	}
	q.done <- true
}

func Recover(uuid gocql.UUID) {
	q.queue <- uuid
}

func RecoveryStart(session *gocql.Session, payStats *PayStats) {
	q.queue = make(chan gocql.UUID, 100)
	q.done = make(chan bool, 1)
	go recoveryWorker(session, payStats)
}

func RecoveryStop() {
	close(q.queue)
	<-q.done
}
