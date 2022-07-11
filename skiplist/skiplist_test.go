package skiplist

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestEmpty(t *testing.T) {
	key := []byte("test")
	list := NewSkipList()
	value, ok := list.Get(key)
	assert.Equal(t, false, ok, "Empty skip list can not get a key")
	assert.Empty(t, value, "Empty skip list can not get a key")
	assert.Equal(t, 0, list.size(), "Empty skip list should have a size of 0")
}

func TestSerialization(t *testing.T) {
	list := NewSkipList()
	const n = 1000
	for i := 0; i < n; i++ {
		list.Insert(Element{
			Key:   []byte(fmt.Sprintf("%2d", i)),
			Value: []byte(fmt.Sprintf("%65536d", i)),
		})
	}
	for i := 0; i < n; i++ {
		value, ok := list.Get([]byte(fmt.Sprintf("%2d", i)))
		require.Equal(t, true, ok)
		require.Equal(t, []byte(fmt.Sprintf("%65536d", i)), value)
	}
	assert.Equal(t, n, list.size())
}

func TestOrder(t *testing.T) {
	list := NewSkipList()
	for ch := 'a'; ch <= 'z'; ch++ {
		key := []byte(fmt.Sprintf("%c", ch))
		value := []byte(fmt.Sprintf("%c", ch))
		list.Insert(Element{
			Key:   key,
			Value: value,
		})
	}
	iter := list.Begin()
	for ch := 'a'; ch <= 'z'; ch++ {
		assert.Equal(t, true, iter.Valid())
		key := iter.Key()
		value := iter.Value()
		assert.Equal(t, []byte(fmt.Sprintf("%c", ch)), key)
		assert.Equal(t, []byte(fmt.Sprintf("%c", ch)), value)
		iter.Next()
	}
	assert.Equal(t, false, iter.Valid())
}

func TestOverwrite(t *testing.T) {
	list := NewSkipList()
	for i := 0; i < 10; i++ {
		list.Insert(Element{
			Key:   []byte("test"),
			Value: []byte(fmt.Sprintf("%65536d", i)),
		})
	}
	value, ok := list.Get([]byte("test"))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte(fmt.Sprintf("%65536d", 9)), value)
	assert.Equal(t, 1, list.size())
}

func TestConcurrency(t *testing.T) {
	list := NewSkipList()
	const n = 1000
	var mutex sync.RWMutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mutex.Lock()
			list.Insert(Element{
				Key:   []byte(fmt.Sprintf("%2d", i)),
				Value: []byte(fmt.Sprintf("%65536d", i)),
			})
			mutex.Unlock()
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mutex.RLock()
			value, ok := list.Get([]byte(fmt.Sprintf("%2d", i)))
			require.Equal(t, true, ok)
			require.Equal(t, []byte(fmt.Sprintf("%65536d", i)), value)
			mutex.RUnlock()
		}(i)
	}
	wg.Wait()
	require.Equal(t, n, list.size())
}

func TestConcurrencyOrder(t *testing.T) {
	list := NewSkipList()
	var mutex sync.Mutex
	var wg sync.WaitGroup
	for ch := 'a'; ch <= 'z'; ch++ {
		wg.Add(1)
		go func(ch int32) {
			defer wg.Done()
			mutex.Lock()
			key := []byte(fmt.Sprintf("%c", ch))
			value := []byte(fmt.Sprintf("%c", ch))
			list.Insert(Element{
				Key:   key,
				Value: value,
			})
			mutex.Unlock()
		}(ch)
	}
	wg.Wait()
	iter := list.Begin()
	for ch := 'a'; ch <= 'z'; ch++ {
		assert.Equal(t, true, iter.Valid())
		key := iter.Key()
		value := iter.Value()
		assert.Equal(t, []byte(fmt.Sprintf("%c", ch)), key)
		assert.Equal(t, []byte(fmt.Sprintf("%c", ch)), value)
		iter.Next()
	}
	assert.Equal(t, false, iter.Valid())
}

func TestConcurrencyOverWrite(t *testing.T) {
	list := NewSkipList()
	var mutex sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mutex.Lock()
			list.Insert(Element{
				Key:   []byte("test"),
				Value: []byte(fmt.Sprintf("%65536d", i)),
			})
			mutex.Unlock()
		}(i)
	}
	wg.Wait()
	value, ok := list.Get([]byte("test"))
	assert.Equal(t, true, ok)
	results := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		results[i] = []byte(fmt.Sprintf("%65536d", i))
	}
	assert.Contains(t, results, value)
	assert.Equal(t, 1, list.size())
}

func getRandomKey(rng *rand.Rand) []byte {
	b := make([]byte, 8)
	num := rng.Uint64()
	binary.LittleEndian.PutUint64(b, num)
	return b
}

func BenchmarkReadWriteMap(b *testing.B) {
	value := []byte(fmt.Sprintf("%05d", 123))
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rng.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(getRandomKey(rng))]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(getRandomKey(rng))] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}

func BenchmarkReadWrite(b *testing.B) {
	value := []byte(fmt.Sprintf("%05d", 123))
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			list := NewSkipList()
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rng.Float32() < readFrac {
						mutex.RLock()
						_, ok := list.Get(getRandomKey(rng))
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						list.Insert(Element{
							Key:   getRandomKey(rng),
							Value: value,
						})
						mutex.Unlock()
					}
				}
			})
		})
	}
}
