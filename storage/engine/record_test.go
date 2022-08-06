package engine

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord(t *testing.T) {
	flag := byte(0)
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("%016d", i))
		value := []byte(fmt.Sprintf("%065536d", i))
		expected := NewRecordWithoutChecksum(flag, key, value)
		bytes := EncodeRecordWithChecksum(expected)
		actual := DecodeRecord(bytes)
		require.Equal(t, expected, actual)
	}
}
