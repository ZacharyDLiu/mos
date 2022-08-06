package engine

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBasicOperations(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := Open(nil)
	require.Nil(t, err)
	require.NotNil(t, s)

	expected := []byte(fmt.Sprintf("%065536d", 123))
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("%016d", i))
		err := s.Put(key, expected)
		require.Nil(t, err)

		actual, err := s.Get(key)
		require.Nil(t, err)
		require.Equal(t, expected, actual)

		err = s.Delete(key)
		require.Nil(t, err)

		_, err = s.Get(key)
		require.NotNil(t, err)
		require.Equal(t, ErrKeyNotFound, err)
	}
	err = s.Close()
	require.Nil(t, err)
}

func TestBaseline(t *testing.T) {
	file, err := os.Create("baseline")
	require.Nil(t, err)
	defer os.Remove("baseline")

	key := []byte(fmt.Sprintf("%16d", 123))
	value := []byte(fmt.Sprintf("%065536d", 123))
	r := NewRecordWithoutChecksum(byte(0), key, value)
	data := EncodeRecordWithChecksum(r)
	start := time.Now()
	for i := 0; i < 100000; i++ {
		_, err := file.Write(data)
		require.Nil(t, err)
	}
	fmt.Println(time.Since(start))
	err = file.Close()
	require.Nil(t, err)
}

func TestMKVPut(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := Open(nil)
	require.Nil(t, err)
	require.NotNil(t, s)

	key := []byte(fmt.Sprintf("%016d", 123))
	value := []byte(fmt.Sprintf("%065536d", 123))
	start := time.Now()
	for i := 0; i < 100000; i++ {
		err := s.Put(key, value)
		require.Nil(t, err)
	}
	fmt.Println(time.Since(start))
	err = s.Close()
	require.Nil(t, err)
}

func TestConcurrent(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	db, err := Open(nil)
	require.Nil(t, err)
	require.NotNil(t, db)

	var wg sync.WaitGroup
	expected := []byte(fmt.Sprintf("%065536d", 123))
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("%016d", i))
			err := db.Put(key, expected)
			require.Nil(t, err)

			actual, err := db.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)

			err = db.Delete(key)
			require.Nil(t, err)

			_, err = db.Get(key)
			require.NotNil(t, err)
			require.Equal(t, ErrKeyNotFound, err)
		}(i)
	}
	wg.Wait()
	err = db.Close()
	require.Nil(t, err)
}

func TestLastWriteWins(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	db, err := Open(nil)
	require.Nil(t, err)
	require.NotNil(t, db)

	var wg sync.WaitGroup
	key := []byte(fmt.Sprintf("%16d", 123))
	last := 666
	expected := []byte(fmt.Sprintf("%065536d", last))
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i == last {
				time.Sleep(10 * time.Second)
			}
			value := []byte(fmt.Sprintf("%065536d", i))
			err := db.Put(key, value)
			require.Nil(t, err)
		}(i)
	}
	wg.Wait()
	actual, err := db.Get(key)
	require.Nil(t, err)
	require.Equal(t, expected, actual)
	err = db.Close()
	require.Nil(t, err)
}

func TestReopen(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)
	config.SyncWrite = true

	expected := []byte(fmt.Sprintf("%065536d", 123))
	// with close
	{
		s, err := Open(config)
		require.Nil(t, err)
		require.NotNil(t, s)

		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("%016d", i))
			err := s.Put(key, expected)
			require.Nil(t, err)

			actual, err := s.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
		err = s.Close()
		require.Nil(t, err)
	}
	// read after close
	{
		s, err := Open(config)
		require.Nil(t, err)
		require.NotNil(t, s)

		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("%016d", i))
			actual, err := s.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
		err = s.Close()
		require.Nil(t, err)
	}
	err = os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)
	// without close
	{
		s, err := Open(config)
		require.Nil(t, err)
		require.NotNil(t, s)

		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("%016d", i))
			err := s.Put(key, expected)
			require.Nil(t, err)

			actual, err := s.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
		err = s.lock.Unlock()
		require.Nil(t, err)
	}
	// read after without close
	{
		s, err := Open(config)
		require.Nil(t, err)
		require.NotNil(t, s)

		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("%016d", i))
			actual, err := s.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
		err = s.Close()
		require.Nil(t, err)
	}
}

func TestMerge(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := Open(nil)
	require.Nil(t, err)
	require.NotNil(t, s)

	expected := []byte(fmt.Sprintf("%065536d", 123))
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%016d", i))
		err := s.Put(key, expected)
		require.Nil(t, err)

		actual, err := s.Get(key)
		require.Nil(t, err)
		require.Equal(t, expected, actual)

		err = s.Delete(key)
		require.Nil(t, err)

		_, err = s.Get(key)
		require.NotNil(t, err)
		require.Equal(t, ErrKeyNotFound, err)
	}
	err = s.Merge()
	require.Nil(t, err)
	require.Equal(t, 0, s.cur.Size())
	err = s.Close()
	require.Nil(t, err)
}

func TestRecover(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)
	config.SyncWrite = true

	expected := []byte(fmt.Sprintf("%065536d", 123))
	var size int64
	var name string
	{
		s, err := Open(config)
		require.Nil(t, err)
		require.NotNil(t, s)

		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("%016d", i))
			err := s.Put(key, expected)
			require.Nil(t, err)

			actual, err := s.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
		size = s.cur.Size()
		name = s.cur.Name()
		err = s.Close()
		require.Nil(t, err)
	}
	file, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
	require.Nil(t, err)
	_, err = file.WriteString("test string to corrupt data file")
	require.Nil(t, err)
	err = file.Close()
	require.Nil(t, err)
	{
		s, err := Open(config)
		require.Nil(t, err)
		require.NotNil(t, s)

		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("%016d", i))
			actual, err := s.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		}
		require.Equal(t, size, s.cur.Size())
		for i := 1000; i < 2000; i++ {
			key := []byte(fmt.Sprintf("%016d", i))
			err := s.Put(key, expected)
			require.Nil(t, err)

			actual, err := s.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)

			err = s.Delete(key)
			require.Nil(t, err)

			_, err = s.Get(key)
			require.NotNil(t, err)
			require.Equal(t, ErrKeyNotFound, err)
		}
		err = s.Close()
		require.Nil(t, err)
	}
}

func BenchmarkBasicOperations(b *testing.B) {
	config := DefaultConfig()
	os.RemoveAll(config.RootDirectory)

	s, _ := Open(nil)
	value := []byte(fmt.Sprintf("%065536d", 123))
	key := []byte(fmt.Sprintf("%16d", 123))
	b.ResetTimer()
	b.Run("put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = s.Put(key, value)
		}
	})
	b.Run("get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = s.Get(key)
		}
	})
	b.Run("delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = s.Delete(key)
		}
	})
	_ = s.Close()
}

func TestLoadIndexFromDataFiles(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := Open(nil)
	require.Nil(t, err)

	// write 1000000 64KiB object
	value := []byte(fmt.Sprintf("%65536d", 123))
	for i := 0; i < 1000000; i++ {
		key := []byte(fmt.Sprintf("%016d", i))
		err := s.Put(key, value)
		require.Nil(t, err)
	}
	expected := s.index
	err = s.Close()
	require.Nil(t, err)

	files, err := LoadDataFiles(config.RootDirectory)
	require.Nil(t, err)
	index := make(map[string]*Entry)
	start := time.Now()
	err = LoadIndexFromDataFiles(index, files)
	require.Nil(t, err)
	fmt.Println(time.Since(start))

	require.Equal(t, expected, index)

	for _, file := range files {
		file.Close()
	}
}

func TestLoadIndex(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := Open(nil)
	require.Nil(t, err)

	// write 1000000 64KiB object
	value := []byte(fmt.Sprintf("%65536d", 123))
	for i := 0; i < 1000000; i++ {
		key := []byte(fmt.Sprintf("%016d", i))
		err := s.Put(key, value)
		require.Nil(t, err)
	}
	expected := s.index
	err = s.Close()
	require.Nil(t, err)

	files, err := LoadDataFiles(config.RootDirectory)
	require.Nil(t, err)
	start := time.Now()
	index, err := LoadIndex(config.RootDirectory)
	require.Nil(t, err)
	fmt.Println(time.Since(start))

	require.Equal(t, expected, index)

	for _, file := range files {
		file.Close()
	}
}

func TestLoadIndexFromHintFiles(t *testing.T) {
	config := DefaultConfig()
	err := os.RemoveAll(config.RootDirectory)
	require.Nil(t, err)

	s, err := Open(nil)
	require.Nil(t, err)

	// write 1000000 64KiB object
	value := []byte(fmt.Sprintf("%65536d", 123))
	for i := 0; i < 1000000; i++ {
		key := []byte(fmt.Sprintf("%016d", i))
		err := s.Put(key, value)
		require.Nil(t, err)
	}
	expected := s.index
	err = s.Close()
	require.Nil(t, err)

	filenames, err := getHintFilenames(config.RootDirectory)
	require.Nil(t, err)
	files, err := LoadDataFiles(config.RootDirectory)
	require.Nil(t, err)
	index := make(map[string]*Entry)
	start := time.Now()
	err = LoadIndexFromHintFiles(index, filenames)
	err = LoadIndexFromDataFile(index, files[len(files)-1])
	require.Nil(t, err)
	fmt.Println(time.Since(start))

	require.Equal(t, expected, index)
	for _, file := range files {
		file.Close()
	}
}
