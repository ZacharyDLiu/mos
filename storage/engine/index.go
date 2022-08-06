package engine

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

const indexFileName = "index"

func SaveIndex(index map[string]*Entry, dir string) error {
	name := filepath.Join(dir, indexFileName)
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	for key, entry := range index {
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

func ReadIndex(r io.Reader) ([]byte, *Entry, error) {
	header := make([]byte, 2)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, nil, err
	}
	ksize := binary.BigEndian.Uint16(header)
	key := make([]byte, ksize)
	_, err = io.ReadFull(r, key)
	if err != nil {
		return nil, nil, err
	}
	payload := make([]byte, sizeEnd)
	_, err = io.ReadFull(r, payload)
	if err != nil {
		return nil, nil, err
	}
	return key, DecodeEntry(payload), nil
}

func LoadIndex(dir string) (map[string]*Entry, error) {
	name := filepath.Join(dir, indexFileName)
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	index := make(map[string]*Entry)
	for {
		key, entry, err := ReadIndex(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		index[string(key)] = entry
	}
	return index, nil
}
