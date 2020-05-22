package main

import (
	llog "github.com/sirupsen/logrus"
	"github.com/spenczar/tdigest"
	"time"
)

type stats struct {
	n_requests  int64
	starttime   time.Time
	cputime     time.Duration
	latency_min time.Duration
	latency_max time.Duration
	latency_avg time.Duration
	tdigest     *tdigest.TDigest
}

var s stats

type cookie struct {
	time time.Time
}

func StatsInit() {
	s.starttime = time.Now()
	s.tdigest = tdigest.New()
}

func StatsRequestStart() cookie {
	return cookie{
		time: time.Now(),
	}
}

func StatsRequestEnd(c cookie) {
	s.n_requests++
	elapsed := time.Since(c.time)
	s.cputime += elapsed
	if elapsed > s.latency_max {
		s.latency_max = elapsed
	}
	if s.latency_min == 0 || s.latency_min > elapsed {
		s.latency_min = elapsed
	}
	s.tdigest.Add(elapsed.Seconds(), 1)
}

func StatsReportSummaries() {

	wallclocktime := time.Since(s.starttime).Seconds()
	llog.Infof("Total time: %.3fs, %v t/sec",
		wallclocktime,
		int(float64(s.n_requests)/wallclocktime),
	)
	llog.Infof("Latency min/max/avg: %.3fs/%.3fs/%.3fs",
		s.latency_min.Seconds(),
		s.latency_max.Seconds(),
		(s.cputime.Seconds() / float64(s.n_requests)),
	)
	llog.Infof("Latency 95/99/99.9%%: %.3fs/%.3fs/%.3fs",
		s.tdigest.Quantile(0.95),
		s.tdigest.Quantile(0.99),
		s.tdigest.Quantile(0.999),
	)
}
