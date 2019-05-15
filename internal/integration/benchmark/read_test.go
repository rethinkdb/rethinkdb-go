package benchmark

import (
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
	"math/rand"
	"testing"
)

func benchRead() {
	var val readOneStruct
	_ = r.DB(db).Table(readsOneTable).Get(rand.Intn(readsOneDataLen)).ReadOne(&val, session)
}

func benchReadBatch() {
	var vals []readManyStruct
	_ = r.DB(db).Table(readsManyTable).GetAllByIndex(readsManyIndex, rand.Intn(readsManySequenceLen)).ReadAll(&vals, session)
}

func BenchmarkRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchRead()
	}
}

func BenchmarkReadParallel(b *testing.B) {
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchRead()
		}
	})
}

func BenchmarkReadBatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchReadBatch()
	}
}

func BenchmarkReadBatchParallel(b *testing.B) {
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchReadBatch()
		}
	})
}
