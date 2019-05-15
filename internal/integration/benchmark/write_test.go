package benchmark

import (
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
	"math/rand"
	"testing"
)

func benchInsert() {
	_, _ = r.DB(db).Table(insertsTable).Insert(insertsData[rand.Intn(insertsDataLen)]).RunWrite(session)
}

func benchInsertBatch(batch int) {
	start := rand.Intn(insertsDataLen)
	end := start + batch
	if end > insertsDataLen {
		end = insertsDataLen
	}
	_, _ = r.DB(db).Table(insertsTable).Insert(insertsData[start:end]).RunWrite(session)
}

func BenchmarkInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchInsert()
	}
}

func BenchmarkInsertParallel(b *testing.B) {
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchInsert()
		}
	})
}

func BenchmarkInsertBatch1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchInsertBatch(1000)
	}
}

func BenchmarkInsertBatch1000Parallel(b *testing.B) {
	b.SetParallelism(parallelism)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchInsertBatch(1000)
		}
	})
}
