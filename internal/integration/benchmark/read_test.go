package benchmark

import (
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
	"math/rand"
	"sync/atomic"
	"testing"
)

func benchRead(i int) {
	var val readOneStruct
	_ = r.DB(db).Table(readsOneTable).Get(i%readsOneDataLen).ReadOne(&val, session)
}

func benchReadBatch(i int) {
	var vals []readManyStruct
	_ = r.DB(db).Table(readsManyTable).GetAllByIndex(readsManyIndex, i%readsManySequenceLen).ReadAll(&vals, session)
}

func BenchmarkRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchRead(i)
	}
}

func BenchmarkReadParallel(b *testing.B) {
	b.SetParallelism(parallelism)
	batch := readsOneDataLen / parallelism
	num := int32(0)
	b.RunParallel(func(pb *testing.PB) {
		cur := int(atomic.AddInt32(&num, 1)) * batch
		for pb.Next() {
			benchRead(cur)
			cur++
		}
	})
}

func BenchmarkReadBatch(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchReadBatch(i)
	}
}

func BenchmarkReadBatchParallel(b *testing.B) {
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		cur := rand.Intn(readsManySequenceLen)
		for pb.Next() {
			benchReadBatch(cur)
			cur++
		}
	})
}
