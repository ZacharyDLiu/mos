package engine

import "encoding/binary"

const (
	idBegin     = 0
	offsetBegin = 8
	sizeBegin   = 8 + 8
	sizeEnd     = 8 + 8 + 8
)

type Entry struct {
	ID     uint64
	Offset uint64
	Size   uint64
}

func DecodeEntry(bytes []byte) *Entry {
	id := binary.BigEndian.Uint64(bytes[idBegin:offsetBegin])
	offset := binary.BigEndian.Uint64(bytes[offsetBegin:sizeBegin])
	size := binary.BigEndian.Uint64(bytes[sizeBegin:sizeEnd])
	return &Entry{
		ID:     id,
		Offset: offset,
		Size:   size,
	}
}

func EncodeEntry(entry *Entry) []byte {
	bytes := make([]byte, sizeEnd)
	binary.BigEndian.PutUint64(bytes[idBegin:offsetBegin], entry.ID)
	binary.BigEndian.PutUint64(bytes[offsetBegin:sizeBegin], entry.Offset)
	binary.BigEndian.PutUint64(bytes[sizeBegin:sizeEnd], entry.Size)
	return bytes
}
