package main

import (
	//	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
	"math/rand"
	"sync"
	"time"
)

var accounts struct {
	once sync.Once
	bics []string
}

// Represents a random data generator for the load.
//
// When testing payments, randomly selects from an existing accounts,
// which are first downloaded from the cluster.
//
// Has a "Hot" mode, in which is biased towards returning hot keys
//
// This data structure is not goroutine safe.

type FixedRandomSource struct {
	rand rand.Source
}

func createRandomBic() string {

	var letters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var digits = []rune("0123456789")

	bic := make([]rune, 8)
	i := 0
	for ; i < 4; i++ {
		bic[i] = letters[rand.Intn(len(letters))]
	}
	cc := ISO3166[rand.Intn(len(ISO3166))]
	for _, c := range cc {
		bic[i] = c
		i++
	}
	for ; i < len(bic); i++ {
		bic[i] = digits[rand.Intn(len(digits))]
	}
	return string(bic)
}

func createRandomBan() string {

	var digits = []rune("0123456789")

	ban := make([]rune, 14)
	for i, _ := range ban {
		ban[i] = digits[rand.Intn(len(digits))]
	}
	return string(ban)

}

func (r *FixedRandomSource) Init() {

	r.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	create_new_bics := func() {
		n_bics := rand.Intn(500)
		accounts.bics = make([]string, 0, n_bics)
		for i := 0; i < n_bics; i++ {
			accounts.bics = append(accounts.bics, createRandomBic())
		}
	}
	accounts.once.Do(create_new_bics)
}

// Return a globally unique identifier
// to ensure no client id conflicts
func (r *FixedRandomSource) NewClientId() gocql.UUID {
	return gocql.TimeUUID()
}

// Return a globally unique identifier
func (r *FixedRandomSource) NewTransferId() gocql.UUID {
	return gocql.TimeUUID()
}

func (r *FixedRandomSource) NewBicAndBan() (string, string) {
	bic := accounts.bics[rand.Intn(len(accounts.bics))]
	ban := createRandomBan()
	return bic, ban
}

func (r *FixedRandomSource) NewStartBalance() *inf.Dec {
	return inf.NewDec(rand.Int63n(100000), 0)
}

func (r *FixedRandomSource) NewTransferAmount() *inf.Dec {
	return inf.NewDec(rand.Int63n(10000), inf.Scale(rand.Int63n(3)))
}

func (r *FixedRandomSource) BicAndBan() (string, string) {
	return r.NewBicAndBan()
}

func (r *FixedRandomSource) HotBicAndBan() (string, string) {
	return r.BicAndBan()
}
