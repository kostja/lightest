package main

import (
	//	"github.com/ansel1/merry"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
	"math/rand"
	"sync"
	"time"
)

type BicAndBans struct {
	bic  string
	bans []string
}

var accounts []BicAndBans
var accounts_once sync.Once

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

func (r *FixedRandomSource) Init(session *gocql.Session) {

	r.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	create_new_bics := func() {
		n_bics := rand.Intn(500)
		accounts = make([]BicAndBans, 0, n_bics)
		for i := 0; i < n_bics; i++ {
			accounts = append(accounts, BicAndBans{bic: createRandomBic()})
		}
	}
	load_existing_bics := func() {
		iter := session.Query("SELECT bic, ban FROM accounts").Iter()
		var bic, ban string
		bics := map[string][]string{}
		for iter.Scan(&bic, &ban) {
			bics[bic] = append(bics[bic], ban)
		}
		if err := iter.Close(); err != nil {
			llog.Fatal(err)
		}
		n_bics := len(bics)
		accounts = make([]BicAndBans, 0, n_bics)
		for k, v := range bics {
			accounts = append(accounts, BicAndBans{bic: k, bans: v})
		}
	}
	// Each worker gorotuine uses its own instance of FixedRandomSource,
	// but they share the data about existing BICs and BANs.
	if session != nil {
		accounts_once.Do(load_existing_bics)
	} else {
		accounts_once.Do(create_new_bics)
	}
}

// Return a globally unique identifier
// to ensure no client id conflicts
func (r *FixedRandomSource) NewClientId() gocql.UUID {
	return gocql.TimeUUID()
}

// Return a globally unique identifier, each transfer
// is unique
func (r *FixedRandomSource) NewTransferId() gocql.UUID {
	return gocql.TimeUUID()
}

// Create a new BIC and BAN pair
func (r *FixedRandomSource) NewBicAndBan() (string, string) {
	bic := accounts[rand.Intn(len(accounts))].bic
	ban := createRandomBan()
	return bic, ban
}

// Create a new random start balance
func (r *FixedRandomSource) NewStartBalance() *inf.Dec {
	return inf.NewDec(rand.Int63n(100000), 0)
}

// Crate a new random transfer
func (r *FixedRandomSource) NewTransferAmount() *inf.Dec {
	return inf.NewDec(rand.Int63n(10000), inf.Scale(rand.Int63n(3)))
}

// Find an existing BIC and BAN pair for transaction.
// To avoid yielding a duplicate pair when called
// twice in a row, pass pointers to previous BIC and BAN,
// in this case the new pair is guaranteed to be distinct.
func (r *FixedRandomSource) BicAndBan(src ...string) (string, string) {
	for {
		bic_pos := rand.Intn(len(accounts))
		ban_pos := rand.Intn(len(accounts[bic_pos].bans))

		bic, ban := accounts[bic_pos].bic, accounts[bic_pos].bans[ban_pos]
		if len(src) < 1 || bic != src[0] || len(src) < 2 || ban != src[1] {
			return bic, ban
		}
	}
}

// Find an existing BIC and BAN pair for transaction.
// Uses a normal distribution to return "hot" pairs.
// To avoid yielding a duplicate pair when called
// twice in a row, pass pointers to previous BIC and BAN,
// in this case the new pair is guaranteed to be distinct.
func (r *FixedRandomSource) HotBicAndBan(src ...string) (string, string) {
	return r.BicAndBan(src...)
}
