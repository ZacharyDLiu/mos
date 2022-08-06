package main

import (
	"fmt"
	"math/rand"
	"mos/storage/engine"
	"os"
	"sync"
	"testing"
	"time"

	"git.mills.io/prologic/bitcask"
	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
)

const rootDir = "/tmp/test"

func TestBadgerWrite(t *testing.T) {
	path := rootDir + "/badger"
	err := os.RemoveAll(path)
	require.Nil(t, err)
	err = os.MkdirAll(path, 0777)
	require.Nil(t, err)

	db, err := badger.Open(badger.DefaultOptions(path))
	require.Nil(t, err)
	defer db.Close()

	value := []byte(fmt.Sprintf("%065536d", 123))
	n := 100000
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			key := []byte(fmt.Sprintf("%016d", i))
			err := db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, value)
			})
			require.Nil(t, err)
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}

func TestBadgerRead(t *testing.T) {
	path := rootDir + "/badger"
	db, err := badger.Open(badger.DefaultOptions(path))
	require.Nil(t, err)
	defer db.Close()

	expected := []byte(fmt.Sprintf("%065536d", 123))
	n := 100000
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			key := []byte(fmt.Sprintf("%016d", rand.Intn(n)))
			_ = db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				require.Nil(t, err)
				return item.Value(func(val []byte) error {
					require.Equal(t, expected, val)
					return nil
				})
			})
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}

func TestBitcaskWrite(t *testing.T) {
	path := rootDir + "/bitcask"
	err := os.RemoveAll(path)
	require.Nil(t, err)
	err = os.MkdirAll(path, 0777)
	require.Nil(t, err)

	db, err := bitcask.Open(path, bitcask.WithMaxDatafileSize(1<<32), bitcask.WithMaxValueSize(1<<17))
	require.Nil(t, err)
	defer db.Close()

	value := []byte(fmt.Sprintf("%065536d", 123))
	n := 100000
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			key := []byte(fmt.Sprintf("%016d", i))
			err := db.Put(key, value)
			require.Nil(t, err)
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}

func TestBitcaskRead(t *testing.T) {
	path := rootDir + "/bitcask"
	db, err := bitcask.Open(path, bitcask.WithMaxDatafileSize(1<<32), bitcask.WithMaxValueSize(1<<17))
	require.Nil(t, err)
	defer db.Close()

	expected := []byte(fmt.Sprintf("%065536d", 123))
	n := 100000
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			key := []byte(fmt.Sprintf("%016d", rand.Intn(n)))
			actual, err := db.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}

func TestMKVWrite(t *testing.T) {
	path := rootDir + "/mkv"
	err := os.RemoveAll(path)
	require.Nil(t, err)
	err = os.MkdirAll(path, 0777)
	require.Nil(t, err)

	cfg := engine.DefaultConfig()
	cfg.RootDirectory = path
	db, err := engine.Open(cfg)
	require.Nil(t, err)
	defer db.Close()

	value := []byte(fmt.Sprintf("%065536d", 123))
	n := 100000
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			key := []byte(fmt.Sprintf("%016d", i))
			err := db.Put(key, value)
			require.Nil(t, err)
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}

func TestMKVRead(t *testing.T) {
	path := rootDir + "/mkv"
	cfg := engine.DefaultConfig()
	cfg.RootDirectory = path
	db, err := engine.Open(cfg)
	require.Nil(t, err)
	defer db.Close()

	expected := []byte(fmt.Sprintf("%065536d", 123))
	n := 100000
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			key := []byte(fmt.Sprintf("%016d", rand.Intn(n)))
			actual, err := db.Get(key)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}

func TestLevelDBWrite(t *testing.T) {
	path := rootDir + "/leveldb"
	err := os.RemoveAll(path)
	require.Nil(t, err)
	err = os.MkdirAll(path, 0777)
	require.Nil(t, err)

	db, err := leveldb.OpenFile(path, nil)
	require.Nil(t, err)
	defer db.Close()

	value := []byte(fmt.Sprintf("%065536d", 123))
	n := 100000
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			key := []byte(fmt.Sprintf("%016d", i))
			err := db.Put(key, value, nil)
			require.Nil(t, err)
			wg.Done()
		}(i)
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}

func TestLevelDBRead(t *testing.T) {
	path := rootDir + "/leveldb"
	db, err := leveldb.OpenFile(path, nil)
	require.Nil(t, err)
	defer db.Close()

	expected := []byte(fmt.Sprintf("%065536d", 123))
	n := 100000
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			key := []byte(fmt.Sprintf("%016d", rand.Intn(n)))
			actual, err := db.Get(key, nil)
			require.Nil(t, err)
			require.Equal(t, expected, actual)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Println(time.Since(start))
}
