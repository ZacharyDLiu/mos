package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func prepareDataFile(dir string, id int) error {
	df, err := NewDataFile(dir, id, false)
	if err != nil {
		return err
	}
	defer df.Close()

	flag := byte(0)
	key := []byte(fmt.Sprintf("%016d", 123))
	value := []byte(fmt.Sprintf("%065536d", 123))
	record := NewRecordWithoutChecksum(flag, key, value)
	for i := 0; i < 10000; i++ {
		_, _, err := df.AppendRecord(record)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestDataFileWriteAndRead(t *testing.T) {
	dir := "test"
	df, err := NewDataFile(dir, 0, false)
	require.Nil(t, err)

	flag := byte(0)
	key := []byte(fmt.Sprintf("%016d", 123))
	value := []byte(fmt.Sprintf("%065536d", 123))
	expected := NewRecordWithoutChecksum(flag, key, value)
	for i := 0; i < 10000; i++ {
		offset, size, err := df.AppendRecord(expected)
		require.Nil(t, err)

		actual, err := df.ReadEntireRecordAt(offset, size)
		require.Nil(t, err)
		require.Equal(t, expected, actual)
	}
	name := df.Name()
	err = df.Close()
	require.Nil(t, err)
	err = os.Remove(name)
	require.Nil(t, err)
}

func TestDataFileReadAllRecords(t *testing.T) {
	dir := "test"
	id := 0
	err := prepareDataFile(dir, id)
	require.Nil(t, err)
	// read only
	{
		df, err := NewDataFile(dir, id, true)
		require.Nil(t, err)

		flag := byte(0)
		key := []byte(fmt.Sprintf("%016d", 123))
		value := []byte(fmt.Sprintf("%065536d", 123))
		expected := NewRecordWithoutChecksum(flag, key, value)
		offset := int64(0)
		for i := 0; i < 10000; i++ {
			actual, err := df.ReadRecordAt(offset)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
		for i := 0; i < 10000; i++ {
			actual, err := df.ReadRecordAt(offset)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}

		err = df.Close()
		require.Nil(t, err)
	}
	// not read only
	{
		df, err := NewDataFile(dir, id, false)
		require.Nil(t, err)

		flag := byte(0)
		key := []byte(fmt.Sprintf("%016d", 123))
		value := []byte(fmt.Sprintf("%065536d", 123))
		expected := NewRecordWithoutChecksum(flag, key, value)
		offset := int64(0)
		for i := 0; i < 10000; i++ {
			actual, err := df.ReadRecordAt(offset)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
		for i := 0; i < 10000; i++ {
			actual, err := df.ReadRecordAt(offset)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}

		err = df.Close()
		require.Nil(t, err)
	}
	name := filepath.Join(dir, fmt.Sprintf(dataFileExtension, id))
	err = os.Remove(name)
	require.Nil(t, err)
}

func TestRecoverDataFile(t *testing.T) {
	dir := "test"
	id := 0
	err := prepareDataFile(dir, id)
	require.Nil(t, err)

	var size int64
	// not corrupted
	{
		df, err := NewDataFile(dir, id, false)
		require.Nil(t, err)

		recovered, err := RecoverDataFile(df)
		require.Nil(t, err)
		require.Equal(t, false, recovered)
		size = df.Size()
		err = df.Close()
		require.Nil(t, err)
	}
	// corrupted
	{
		name := "test/00000000.data"
		file, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
		require.Nil(t, err)
		_, err = file.WriteString("test string to corrupt data file")
		require.Nil(t, err)
		err = file.Close()
		require.Nil(t, err)

		df, err := NewDataFile(dir, id, false)
		require.Nil(t, err)

		recovered, err := RecoverDataFile(df)
		require.Nil(t, err)
		require.Equal(t, true, recovered)
		newSize := df.Size()
		require.Equal(t, size, newSize)

		flag := byte(0)
		key := []byte(fmt.Sprintf("%016d", 123))
		value := []byte(fmt.Sprintf("%065536d", 123))
		expected := NewRecordWithoutChecksum(flag, key, value)

		offset := int64(0)
		for i := 0; i < 10000; i++ {
			actual, err := df.ReadRecordAt(offset)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}

		for i := 0; i < 10000; i++ {
			offset, size, err := df.AppendRecord(expected)
			require.Nil(t, err)

			actual, err := df.ReadEntireRecordAt(offset, size)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}

		err = df.Close()
		require.Nil(t, err)
	}
	name := filepath.Join(dir, fmt.Sprintf(dataFileExtension, id))
	err = os.Remove(name)
	require.Nil(t, err)
}
