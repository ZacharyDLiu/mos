package engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/pkg/errors"
)

const lockFile = ".lock"
const hintFileExtension = "%08d.hint"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrDirLocked   = errors.New("dir is locked")
)

type MKV struct {
	mutex     sync.RWMutex
	lock      *flock.Flock
	config    *Config
	meta      *Meta
	cur       *DataFile
	dataFiles map[int]*DataFile
	index     map[string]*Entry
	isMerging bool
	ticker    *time.Ticker
	closeChan chan struct{}
}

func Open(config *Config, options ...Option) (*MKV, error) {
	if config == nil {
		config = DefaultConfig()
	}
	for _, option := range options {
		option(config)
	}
	if err := os.MkdirAll(config.RootDirectory, 0700); err != nil {
		return nil, errors.Wrap(err, "open KVEngine error")
	}

	lock := flock.New(filepath.Join(config.RootDirectory, lockFile))
	ok, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrDirLocked
	}

	meta, err := LoadMeta(config.RootDirectory)
	if err != nil {
		return nil, err
	}
	files, err := LoadDataFiles(config.RootDirectory)
	if err != nil {
		return nil, err
	}
	var cur *DataFile
	dataFiles := make(map[int]*DataFile)
	index := make(map[string]*Entry)
	if len(files) == 0 {
		cur, err = NewDataFile(config.RootDirectory, 0, false)
		if err != nil {
			return nil, errors.Wrap(err, "open kv engine error: ")
		}
	} else {
		cur = files[len(files)-1]
		for i, file := range files {
			if i == len(files)-1 {
				continue
			}
			dataFiles[file.ID()] = file
		}
		recovered, err := RecoverDataFile(cur)
		if err != nil {
			return nil, errors.Wrap(err, "open kv engine error: ")
		}
		if recovered {
			if Exists(filepath.Join(config.RootDirectory, indexFileName)) {
				if err := os.Remove(filepath.Join(config.RootDirectory, indexFileName)); err != nil {
					return nil, errors.Wrap(err, "open kv engine error: ")
				}
			}
		}
		if meta.IndexUpToDate && Exists(filepath.Join(config.RootDirectory, indexFileName)) {
			index, err = LoadIndex(config.RootDirectory)
			if err != nil {
				return nil, errors.Wrap(err, "open kv engine error: ")
			}
		} else {
			if err := LoadIndexFromDataFiles(index, files); err != nil {
				return nil, errors.Wrap(err, "open kv engine error: ")
			}
		}
	}
	m := &MKV{
		lock:      lock,
		config:    config,
		cur:       cur,
		meta:      meta,
		dataFiles: dataFiles,
		index:     index,
		isMerging: false,
	}
	if config.AutoMerging {
		m.ticker = time.NewTicker(config.MergeInterval)
		m.closeChan = make(chan struct{})
		go m.runBackGround()
	}
	return m, nil
}

func LoadDataFiles(dir string) ([]*DataFile, error) {
	names, err := filepath.Glob(fmt.Sprintf("%s/*.data", dir))
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}
	sort.Strings(names)
	files := make([]*DataFile, len(names))
	for i, name := range names {
		id, err := ParseID(name)
		if err != nil {
			return nil, err
		}
		var file *DataFile
		if i == len(names)-1 {
			file, err = NewDataFile(dir, id, false)
			if err != nil {
				return nil, err
			}
		} else {
			file, err = NewDataFile(dir, id, true)
			if err != nil {
				return nil, err
			}
		}
		files[i] = file
	}

	return files, nil
}

func LoadIndexFromDataFiles(index map[string]*Entry, files []*DataFile) error {
	for _, file := range files {
		offset := int64(0)
		for {
			record, err := file.ReadRecordAt(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if record.IsDeleted() {
				delete(index, string(record.key))
			}
			entry := &Entry{
				ID:     uint64(file.ID()),
				Offset: uint64(offset),
				Size:   uint64(record.Size()),
			}
			index[string(record.key)] = entry
			offset += record.Size()
		}
	}
	return nil
}

func LoadIndexFromDataFile(index map[string]*Entry, file *DataFile) error {
	offset := int64(0)
	for {
		record, err := file.ReadRecordAt(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if record.IsDeleted() {
			delete(index, string(record.key))
		}
		entry := &Entry{
			ID:     uint64(file.ID()),
			Offset: uint64(offset),
			Size:   uint64(record.Size()),
		}
		index[string(record.key)] = entry
		offset += record.Size()
	}
	return nil
}

func getHintFilenames(dir string) ([]string, error) {
	names, err := filepath.Glob(fmt.Sprintf("%s/*.hint", dir))
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return names, nil
	}
	sort.Strings(names)
	return names, nil
}

func LoadIndexFromHintFiles(index map[string]*Entry, filenames []string) error {
	for _, filename := range filenames {
		if err := LoadHint(filename, index); err != nil {
			return err
		}
	}
	return nil
}

func LoadHint(name string, index map[string]*Entry) error {
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		key, entry, err := ReadIndex(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		index[string(key)] = entry
	}
	return nil
}

func ParseID(name string) (int, error) {
	base := filepath.Base(name)
	ext := filepath.Ext(name)
	if ext != ".data" {
		return 0, errors.New("invalid data file extension")
	}
	id, err := strconv.ParseInt(strings.TrimSuffix(base, ext), 10, 32)
	if err != nil {
		return 0, err
	}
	return int(id), nil
}

func (m *MKV) createHintFile(id int) error {
	hint := make(map[string]*Entry)
	for key, entry := range m.index {
		if int(entry.ID) == id {
			hint[key] = entry
		}
	}
	return SaveHint(hint, m.config.RootDirectory, id)
}

func SaveHint(hint map[string]*Entry, dir string, id int) error {
	name := filepath.Join(dir, fmt.Sprintf(hintFileExtension, id))
	file, err := os.Create(name)
	if err != nil {
		return err
	}
	defer file.Close()
	for key, entry := range hint {
		bytes := make([]byte, 2+len(key)+sizeEnd)
		binary.BigEndian.PutUint16(bytes[0:2], uint16(len(key)))
		copy(bytes[2:2+len(key)], key)
		payload := EncodeEntry(entry)
		copy(bytes[2+len(key):], payload)
		_, err := file.Write(bytes)
		if err != nil {
			return err
		}
	}
	return file.Sync()
}

func (m *MKV) mayCreateNewDataFile() error {
	if m.cur.Size() < m.config.DataFileMaxSize {
		return nil
	}
	err := m.cur.Close()
	if err != nil {
		return err
	}
	id := m.cur.ID()
	df, err := NewDataFile(m.config.RootDirectory, id, true)
	if err != nil {
		return err
	}
	m.dataFiles[id] = df
	_ = m.createHintFile(id)
	id += 1
	cur, err := NewDataFile(m.config.RootDirectory, id, false)
	if err != nil {
		return err
	}
	m.cur = cur
	return nil
}

func (m *MKV) Put(key []byte, value []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if err := m.mayCreateNewDataFile(); err != nil {
		return err
	}
	record := NewRecordWithoutChecksum(NormalFlag, key, value)
	offset, size, err := m.cur.AppendRecord(record)
	if err != nil {
		return err
	}
	if m.config.SyncWrite {
		if err := m.cur.Sync(); err != nil {
			return err
		}
	}
	entry := &Entry{
		ID:     uint64(m.cur.ID()),
		Offset: uint64(offset),
		Size:   uint64(size),
	}
	old, ok := m.index[string(key)]
	if ok {
		m.meta.ReusableSpace += int64(old.Size)
	}
	m.index[string(key)] = entry
	return nil
}

func (m *MKV) PutData(data []byte, key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if err := m.mayCreateNewDataFile(); err != nil {
		return err
	}
	offset, size, err := m.cur.Append(data)
	if err != nil {
		return err
	}
	if m.config.SyncWrite {
		if err := m.cur.Sync(); err != nil {
			return err
		}
	}
	entry := &Entry{
		ID:     uint64(m.cur.ID()),
		Offset: uint64(offset),
		Size:   uint64(size),
	}
	old, ok := m.index[key]
	if ok {
		m.meta.ReusableSpace += int64(old.Size)
	}
	m.index[key] = entry
	return nil
}

func (m *MKV) Get(key []byte) ([]byte, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	entry, ok := m.index[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	id := int(entry.ID)
	offset := int64(entry.Offset)
	size := int64(entry.Size)
	var df *DataFile
	if id == m.cur.ID() {
		df = m.cur
	} else {
		df = m.dataFiles[id]
	}
	record, err := df.ReadEntireRecordAt(offset, size)
	if err != nil {
		return nil, err
	}
	return record.Value(), err
}

func (m *MKV) Delete(key []byte) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if err := m.mayCreateNewDataFile(); err != nil {
		return err
	}
	record := NewRecordWithoutChecksum(NormalFlag, key, []byte{})
	record.SetDeleted()
	if _, _, err := m.cur.AppendRecord(record); err != nil {
		return err
	}
	old, ok := m.index[string(key)]
	if ok {
		m.meta.ReusableSpace += int64(old.Size)
	}
	delete(m.index, string(key))
	return nil
}

func (m *MKV) Walk(f func(key string, entry *Entry) error) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for key, entry := range m.index {
		if err := f(key, entry); err != nil {
			return err
		}
	}
	return nil
}

func (m *MKV) mayNeedMerge() {
	size := m.cur.Size()
	for _, df := range m.dataFiles {
		size += df.Size()
	}
	if m.meta.ReusableSpace >= m.config.MergeSpaceThreshold && float64(m.meta.ReusableSpace)/float64(size) >= m.config.MergeRatioThreshold && !m.isMerging {
		m.Merge()
	}
}

func (m *MKV) closeCurrent() error {
	err := m.cur.Close()
	if err != nil {
		return err
	}
	id := m.cur.ID()
	df, err := NewDataFile(m.config.RootDirectory, id, true)
	if err != nil {
		return err
	}
	m.dataFiles[id] = df
	return nil
}

func (m *MKV) openNewDataFile() error {
	cur, err := NewDataFile(m.config.RootDirectory, m.cur.ID()+1, false)
	if err != nil {
		return err
	}
	m.cur = cur
	return nil
}

func (m *MKV) Merge() error {
	m.mutex.Lock()
	if m.isMerging {
		m.mutex.Unlock()
		return nil
	}
	m.isMerging = true
	m.mutex.Unlock()
	defer func() {
		m.isMerging = false
	}()
	m.mutex.RLock()
	err := m.closeCurrent()
	if err != nil {
		m.mutex.RUnlock()
		return err
	}
	filesToMerge := make([]int, 0, len(m.dataFiles))
	for k := range m.dataFiles {
		filesToMerge = append(filesToMerge, k)
	}
	err = m.openNewDataFile()
	if err != nil {
		m.mutex.RUnlock()
		return err
	}
	m.mutex.RUnlock()
	sort.Ints(filesToMerge)

	tmpDir, err := ioutil.TempDir(m.config.RootDirectory, "merge")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	// Create a merged database
	config := DefaultConfig()
	config.RootDirectory = tmpDir
	tmpDB, err := Open(config)
	if err != nil {
		return err
	}
	for key, entry := range m.index {
		if int(entry.ID) > filesToMerge[len(filesToMerge)-1] {
			continue
		}
		value, err := m.Get([]byte(key))
		if err != nil {
			return err
		}
		err = tmpDB.Put([]byte(key), value)
		if err != nil {
			return err
		}
	}
	if err = tmpDB.Close(); err != nil {
		return err
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if err := m.close(); err != nil {
		return err
	}

	// Remove data files
	for _, file := range m.dataFiles {
		if file.ID() > filesToMerge[len(filesToMerge)-1] {
			continue
		}
		err = os.Remove(file.Name())
		if err != nil {
			return err
		}
	}

	// Rename all merged data files
	files, err := ioutil.ReadDir(tmpDB.config.RootDirectory)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.Name() == lockFile {
			continue
		}
		err := os.Rename(filepath.Join(tmpDB.config.RootDirectory, file.Name()), filepath.Join(m.config.RootDirectory, file.Name()))
		if err != nil {
			return err
		}
	}
	m.meta.ReusableSpace = 0
	m.meta.IndexUpToDate = true
	return m.reload()
}

func (m *MKV) reload() error {
	files, err := LoadDataFiles(m.config.RootDirectory)
	if err != nil {
		return err
	}
	var cur *DataFile
	dataFiles := make(map[int]*DataFile)
	index := make(map[string]*Entry)
	// load data files
	if len(files) == 0 {
		cur, err = NewDataFile(m.config.RootDirectory, 0, false)
		if err != nil {
			return err
		}
	} else {
		cur = files[len(files)-1]
		for i, file := range files {
			if i == len(files)-1 {
				continue
			}
			dataFiles[file.ID()] = file
		}
		index, err = LoadIndex(m.config.RootDirectory)
		if err != nil {
			return err
		}
	}
	m.cur = cur
	m.dataFiles = dataFiles
	m.index = index
	return nil
}

func (m *MKV) runBackGround() {
	var once sync.Once
	for {
		select {
		case <-m.ticker.C:
			once.Do(func() {
				fmt.Println("merging")
				m.mayNeedMerge()
			})
		case <-m.closeChan:
			return
		}
	}
}

func (m *MKV) Close() error {
	m.mutex.Lock()
	defer func() {
		m.mutex.Unlock()
		m.lock.Unlock()
	}()
	if err := m.close(); err != nil {
		return err
	}
	if m.config.AutoMerging {
		m.ticker.Stop()
		m.closeChan <- struct{}{}
	}
	return nil
}

func (m *MKV) close() error {
	if err := SaveIndex(m.index, m.config.RootDirectory); err != nil {
		return err
	}
	m.meta.IndexUpToDate = true
	if err := SaveMeta(m.meta, m.config.RootDirectory); err != nil {
		return err
	}
	for _, df := range m.dataFiles {
		if err := df.Close(); err != nil {
			return err
		}
	}
	return m.cur.Close()
}
