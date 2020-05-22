package main

type stats struct {
}

var s stats

type cookie struct {
}

func StatsInit() {
}

func StatsRequestStart() cookie {
	return cookie{}
}

func StatsRequestEnd(cookie cookie) {
}

func StatsReportSummaries() {
}
