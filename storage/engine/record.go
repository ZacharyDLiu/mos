package engine

import (
	"encoding/binary"
	"hash/crc32"
)

const bitDeleted = 0

const (
	NormalFlag = byte(0)
)

const (
	flagPos        = 0
	keySizeBegin   = 1
	valueSizeBegin = 1 + 2
	keyBegin       = 1 + 2 + 4
	checksumSize   = 4
)

type Record struct {
	flag     byte
	ksize    uint16
	vsize    uint32
	key      []byte
	value    []byte
	checksum uint32
}

func NewRecordWithoutChecksum(flag byte, key []byte, value []byte) *Record {
	return &Record{
		flag:  flag,
		ksize: uint16(len(key)),
		vsize: uint32(len(value)),
		key:   key,
		value: value,
	}
}

func generateChecksum(flag byte, key []byte, value []byte) uint32 {
	payload := make([]byte, keyBegin+len(key)+len(value))
	payload[0] = flag
	binary.BigEndian.PutUint16(payload[keySizeBegin:valueSizeBegin], uint16(len(key)))
	binary.BigEndian.PutUint32(payload[valueSizeBegin:keyBegin], uint32(len(value)))
	copy(payload[keyBegin:keyBegin+len(key)], key)
	copy(payload[keyBegin+len(key):keyBegin+len(key)+len(value)], value)
	return crc32.ChecksumIEEE(payload)
}

func (r *Record) Size() int64 {
	return int64(keyBegin + len(r.key) + len(r.value) + checksumSize)
}

func (r *Record) Value() []byte {
	return r.value
}

func (r *Record) Corrupted() bool {
	checksum := generateChecksum(r.flag, r.key, r.value)
	return r.checksum != checksum
}

func (r *Record) IsDeleted() bool {
	return (r.flag>>bitDeleted)&1 == 1
}

func (r *Record) SetDeleted() {
	r.flag |= 1 << bitDeleted
}

func DecodeRecord(bytes []byte) *Record {
	flag := bytes[flagPos]
	ksize := binary.BigEndian.Uint16(bytes[keySizeBegin:valueSizeBegin])
	vsize := binary.BigEndian.Uint32(bytes[valueSizeBegin:keyBegin])
	record := &Record{
		flag:  flag,
		ksize: ksize,
		vsize: vsize,
	}
	valueStart := keyBegin + ksize
	checksumStart := uint32(valueStart) + vsize
	record.key = bytes[keyBegin:valueStart]
	record.value = bytes[valueStart:checksumStart]
	record.checksum = binary.BigEndian.Uint32(bytes[checksumStart : checksumStart+checksumSize])
	return record
}

func EncodeRecordWithChecksum(record *Record) []byte {
	bytes := make([]byte, record.Size())
	bytes[flagPos] = record.flag
	binary.BigEndian.PutUint16(bytes[keySizeBegin:valueSizeBegin], record.ksize)
	binary.BigEndian.PutUint32(bytes[valueSizeBegin:keyBegin], record.vsize)
	valueStart := keyBegin + record.ksize
	checksumStart := uint32(valueStart) + record.vsize
	copy(bytes[keyBegin:valueStart], record.key)
	copy(bytes[valueStart:checksumStart], record.value)
	checksum := crc32.ChecksumIEEE(bytes[:checksumStart])
	binary.BigEndian.PutUint32(bytes[checksumStart:checksumStart+checksumSize], checksum)
	return bytes
}
