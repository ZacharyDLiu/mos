package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	n := 1000
	index := make(map[string]*Entry, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%16d", i)
		entry := &Entry{
			ID:     uint64(i),
			Offset: 0,
			Size:   0,
		}
		index[key] = entry
	}
	err := SaveIndex(index, "test")
	require.Nil(t, err)

	actual, err := LoadIndex("test")
	require.Nil(t, err)
	require.Equal(t, index, actual)
}

func PutEntriesToIndex(n int) {
	index := make(map[string]*Entry)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%16d", i)
		entry := &Entry{
			ID:     uint64(i),
			Offset: uint64(i),
			Size:   uint64(i),
		}
		index[key] = entry
	}
}

func TestIndexAllocation(t *testing.T) {
	PutEntriesToIndex(100000)
}
