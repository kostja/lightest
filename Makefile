all:
	go mod vendor
	go build -mod vendor -o lightest lightest.go
