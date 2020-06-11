package main

import (
	//	"github.com/ansel1/merry"
	"encoding/json"
	"github.com/gocql/gocql"
	llog "github.com/sirupsen/logrus"
	"gopkg.in/inf.v0"
	mathrand "math/rand"
	"sync"
)

// Generate a string which looks like a real bank identifier code
func createRandomBic(rand *mathrand.Rand) string {

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

// Generate a string which looks like a bank account number
func createRandomBan(rand *mathrand.Rand) string {

	var digits = []rune("0123456789")

	ban := make([]rune, 14)
	for i, _ := range ban {
		ban[i] = digits[rand.Intn(len(digits))]
	}
	return string(ban)
}

type RandomSettings struct {
	bics     []string
	seed     int64
	accounts int
}

var once sync.Once
var rs *RandomSettings

func randomSettings(session *gocql.Session) *RandomSettings {

	createNewBics := func(rs *RandomSettings) {
		rand := mathrand.New(mathrand.NewSource(rs.seed))
		total_bics := 500
		if total_bics > rs.accounts {
			total_bics = rs.accounts
		}
		rs.bics = make([]string, total_bics, total_bics)
		for i := 0; i < len(rs.bics); i++ {
			rs.bics[i] = createRandomBic(rand)
		}
	}
	unstr := func(str string, v interface{}) {
		b := []byte(str)
		_ = json.Unmarshal(b, &v)
	}
	fetchSettings := func() {
		rs = new(RandomSettings)
		var str string
		if err := session.Query(FETCH_SETTING).Bind("accounts").Scan(&str); err != nil {
			llog.Fatalf("Failed to load lightest.settings: %v", err)
		}
		unstr(str, &rs.accounts)
		if err := session.Query(FETCH_SETTING).Bind("seed").Scan(&str); err != nil {
			llog.Fatalf("Failed to load lightest.settings: %v", err)
		}
		unstr(str, &rs.seed)
		createNewBics(rs)
	}
	once.Do(fetchSettings)
	return rs
}

// Represents a random data generator for the load.
//
// When testing payments, randomly selects from existing accounts.
//
// Has a "Hot" mode, in which is biased towards returning hot keys
//
// This data structure is not goroutine safe.

type FixedRandomSource struct {
	rs     *RandomSettings
	offset int // Current account counter, wraps arround accounts
	rand   *mathrand.Rand
}

func (r *FixedRandomSource) Init(session *gocql.Session) {

	// Each worker gorotuine uses its own instance of FixedRandomSource,
	// but they share the data about existing BICs.
	r.rs = randomSettings(session)
	r.rand = mathrand.New(mathrand.NewSource(r.rs.seed))
	offset := mathrand.Intn(r.rs.accounts)
	for i := 0; i < offset; i++ {
		_, _ = r.NewBicAndBan()
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
	bic := r.rs.bics[r.rand.Intn(len(r.rs.bics))]
	ban := createRandomBan(r.rand)
	r.offset++
	if r.offset > r.rs.accounts {
		r.offset = 0
		r.rand = mathrand.New(mathrand.NewSource(r.rs.seed))
	}
	return bic, ban
}

// Create a new random start balance
func (r *FixedRandomSource) NewStartBalance() *inf.Dec {
	return inf.NewDec(mathrand.Int63n(100000), 0)
}

// Crate a new random transfer
func (r *FixedRandomSource) NewTransferAmount() *inf.Dec {
	return inf.NewDec(mathrand.Int63n(10000), inf.Scale(mathrand.Int63n(3)))
}

// Find an existing BIC and BAN pair for transaction.
// To avoid yielding a duplicate pair when called
// twice in a row, pass pointers to previous BIC and BAN,
// in this case the new pair is guaranteed to be distinct.
func (r *FixedRandomSource) BicAndBan(src ...string) (string, string) {
	for {
		bic, ban := r.NewBicAndBan()
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
