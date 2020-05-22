module github.com/kostja/lightest

go 1.13

require (
	github.com/ansel1/merry v1.5.1
	github.com/gocql/gocql v0.0.0-20200505093417-effcbd8bcf0e
	github.com/sirupsen/logrus v1.2.0
	github.com/spenczar/tdigest v2.1.0+incompatible
	github.com/spf13/cobra v1.0.0
	gopkg.in/inf.v0 v0.9.1
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.4.0
