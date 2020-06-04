all:
	go mod vendor
	go build -mod vendor -o lightest \
	   lightest.go cql.go iso_3166.go \
	   fixed_random_source.go pop.go pay.go \
	   check.go stats.go recovery.go
