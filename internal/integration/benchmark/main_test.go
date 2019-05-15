package benchmark

import (
	"encoding/hex"
	"flag"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	insertsTable   = "inserts"
	readsOneTable  = "reads_one"
	readsManyTable = "reads_many"
)

var session *r.Session
var db string

var insertsData []interface{}

const insertsDataLen = 1000000

const readsOneDataLen = 100000
const readsManyDataLen = 1000000
const readsManySequenceLen = 1000
const readsManyIndex = "filter"

// many clients
const parallelism = 10000

type readOneStruct struct {
	Id   int    `rethinkdb:"id"`
	Data string `rethinkdb:"data"`
}

type readManyStruct struct {
	Id     string `rethinkdb:"id,omitempty"`
	Data   string `rethinkdb:"data"`
	Filter int    `rethinkdb:"filter"`
}

func init() {
	flag.Parse()
	r.SetVerbose(true)
}

func TestMain(m *testing.M) {
	// seed randomness for use with tests
	rand.Seed(time.Now().UTC().UnixNano())

	initSession()
	testBenchmarkSetup()
	res := m.Run()
	testBenchmarkTeardown()

	os.Exit(res)
}

func initSession() {
	// If the test is being run by wercker look for the rethink url
	url := os.Getenv("RETHINKDB_URL")
	if url == "" {
		url = "localhost:28015"
	}

	var err error
	session, err = r.Connect(r.ConnectOpts{
		Address: url,
	})
	if err != nil {
		r.Log.Fatalln(err.Error())
	}
}

func testBenchmarkSetup() {
	db = os.Getenv("RETHINKDB_DB")
	if db == "" {
		db = "benchmarks"
	}

	_ = r.DBDrop(db).Exec(session)
	_ = r.DBCreate(db).Exec(session)

	setupWriteBench()
	setupReadOneBench()
	setupReadManyBench()
}

func testBenchmarkTeardown() {
	_, _ = r.DBDrop(db).Run(session)
}

func setupWriteBench() {
	_, _ = r.DB(db).TableDrop(insertsTable).Run(session)
	_, _ = r.DB(db).TableCreate(insertsTable).Run(session)

	insertsData = make([]interface{}, insertsDataLen)
	for i := range insertsData {
		var buf [20]byte
		_, _ = rand.Read(buf[:])

		insertsData[i] = map[string]interface{}{
			"string": hex.EncodeToString(buf[:]),
			"int":    rand.Int(),
			"float":  rand.Float32(),
		}
	}
}

func setupReadOneBench() {
	_, _ = r.DB(db).TableDrop(readsOneTable).Run(session)
	_, _ = r.DB(db).TableCreate(readsOneTable).Run(session)

	id := 0
	for i := 0; i < readsOneDataLen/1000; i++ {
		var buf [20]byte

		data := make([]readOneStruct, 1000)
		for j := range data {
			_, _ = rand.Read(buf[:])
			data[j].Id = id
			data[j].Data = hex.EncodeToString(buf[:])

			id++
		}

		_, err := r.DB(db).Table(readsOneTable).Insert(data).RunWrite(session)
		if err != nil {
			panic(err)
		}
	}
}

func setupReadManyBench() {
	_, _ = r.DB(db).TableDrop(readsManyTable).Run(session)
	_, _ = r.DB(db).TableCreate(readsManyTable).Run(session)

	fkey := 0
	for i := 0; i < readsManyDataLen/readsManySequenceLen; i++ {
		var buf [20]byte

		data := make([]readManyStruct, readsManySequenceLen)
		for j := range data {
			_, _ = rand.Read(buf[:])
			data[j].Data = hex.EncodeToString(buf[:])
			data[j].Filter = fkey

			fkey++
		}

		_, err := r.DB(db).Table(readsManyTable).Insert(data).RunWrite(session)
		if err != nil {
			panic(err)
		}
	}

	_, err := r.DB(db).Table(readsManyTable).IndexCreate(readsManyIndex).RunWrite(session)
	if err != nil {
		panic(err)
	}
	_ = r.DB(db).Table(readsManyTable).Wait().Exec(session)
}
