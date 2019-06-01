package benchmark

import (
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
	"sync/atomic"
	"testing"
)

func benchInsert(i int) {
	_, _ = r.DB(db).Table(insertsTable).Insert(insertsData[i%insertsDataLen]).RunWrite(session)
}

func benchInsertBatch(i int, batch int) {
	start := (i * batch) % insertsDataLen
	end := start + batch
	if end > insertsDataLen {
		end = insertsDataLen
	}
	_, _ = r.DB(db).Table(insertsTable).Insert(insertsData[start:end]).RunWrite(session)
}

func BenchmarkInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchInsert(i)
	}
}

func BenchmarkInsertParallel(b *testing.B) {
	b.SetParallelism(parallelism)
	batch := insertsDataLen / parallelism
	num := int32(0)
	b.RunParallel(func(pb *testing.PB) {
		cur := int(atomic.AddInt32(&num, 1)) * batch
		for pb.Next() {
			benchInsert(cur)
			cur++
		}
	})
}

func BenchmarkInsertBatch1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchInsertBatch(i, 1000)
	}
}

func BenchmarkInsertBatch1000Parallel(b *testing.B) {
	b.SetParallelism(parallelism)
	batch := insertsDataLen / parallelism
	num := int32(0)
	b.RunParallel(func(pb *testing.PB) {
		cur := int(atomic.AddInt32(&num, 1)) * batch
		for pb.Next() {
			benchInsertBatch(cur, 1000)
			cur++
		}
	})
}
