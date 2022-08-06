package main

import (
	"fmt"
	"mos/storage/engine"
	"testing"
)

func PutEntriesToIndex(n int) {
	index := make(map[string]*engine.Entry)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%64d", i)
		value := &engine.Entry{
			ID:     uint64(i),
			Offset: uint64(i),
			Size:   uint64(i),
		}
		index[key] = value
	}
}

func BenchmarkIndexMemoryUsage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PutEntriesToIndex(100000)
	}
}
