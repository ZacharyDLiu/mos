package engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"
)

const (
	dataFileExtension = "%08d.data"
)

var errReadOnly = errors.New("DataFile is read only")

// DataFile is used as a log file
type DataFile struct {
	id     int
	file   *os.File
	reader *mmap.ReaderAt
	end    int64
}

func NewDataFile(dir string, id int, readOnly bool) (*DataFile, error) {
	filename := filepath.Join(dir, fmt.Sprintf(dataFileExtension, id))
	var (
		file   *os.File
		reader *mmap.ReaderAt
		end    int64
		err    error
	)
	if !readOnly {
		file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
	} else {
		file, err = os.Open(filename)
		if err != nil {
			return nil, err
		}
		reader, err = mmap.Open(filename)
		if err != nil {
			return nil, err
		}
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	end = stat.Size()
	return &DataFile{
		id:     id,
		file:   file,
		reader: reader,
		end:    end,
	}, nil
}

func (df *DataFile) ID() int {
	return df.id
}

func (df *DataFile) Name() string {
	return df.file.Name()
}

func (df *DataFile) Size() int64 {
	return df.end
}

func (df *DataFile) Close() error {
	defer df.file.Close()
	if df.reader != nil {
		return df.reader.Close()
	}
	return nil
}

func (df *DataFile) Sync() error {
	return df.file.Sync()
}

func (df *DataFile) ReadEntireRecordAt(offset int64, size int64) (*Record, error) {
	bytes := make([]byte, size)
	var err error
	if df.reader != nil {
		_, err = df.reader.ReadAt(bytes, offset)
	} else {
		_, err = df.file.ReadAt(bytes, offset)
	}
	if err != nil {
		return nil, err
	}
	return DecodeRecord(bytes), nil
}

func (df *DataFile) ReadRecordAt(offset int64) (*Record, error) {
	var ra io.ReaderAt
	//if df.reader != nil {
	//	ra = df.reader
	//} else {
	//	ra = df.file
	//}
	ra = df.file

	header := make([]byte, keyBegin)
	n, err := ra.ReadAt(header, offset)
	if err != nil {
		return nil, err
	}
	offset += int64(n)
	flag := header[flagPos]
	ksize := binary.BigEndian.Uint16(header[keySizeBegin:valueSizeBegin])
	vsize := binary.BigEndian.Uint32(header[valueSizeBegin:keyBegin])

	payload := make([]byte, uint32(ksize)+vsize)
	n, err = ra.ReadAt(payload, offset)
	if err != nil {
		return nil, err
	}
	offset += int64(n)

	checksum := make([]byte, checksumSize)
	n, err = ra.ReadAt(checksum, offset)
	if err != nil {
		return nil, err
	}
	offset += int64(n)
	return &Record{
		flag:     flag,
		ksize:    ksize,
		vsize:    vsize,
		key:      payload[:ksize],
		value:    payload[ksize:],
		checksum: binary.BigEndian.Uint32(checksum),
	}, nil
}

func (df *DataFile) Read(p []byte) (n int, err error) {
	return df.file.Read(p)
}

func (df *DataFile) AppendRecord(record *Record) (int64, int64, error) {
	if df.reader != nil {
		return 0, 0, errReadOnly
	}
	bytes := EncodeRecordWithChecksum(record)
	offset := df.end
	size, err := df.file.WriteAt(bytes, offset)
	if err != nil {
		return 0, 0, err
	}
	df.end += int64(size)
	return offset, int64(size), nil
}

func (df *DataFile) Append(data []byte) (int64, int64, error) {
	if df.reader != nil {
		return 0, 0, errReadOnly
	}
	offset := df.end
	size, err := df.file.WriteAt(data, offset)
	if err != nil {
		return 0, 0, err
	}
	df.end += int64(size)
	return offset, int64(size), nil
}

func RecoverDataFile(file *DataFile) (bool, error) {
	corrupted := false
	offset := int64(0)
	var err error
	for !corrupted {
		record, err := file.ReadRecordAt(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return false, err
		}
		corrupted = record.Corrupted()
		if !corrupted {
			offset += record.Size()
		}
	}
	if offset == file.Size() {
		return false, nil
	}
	data := make([]byte, offset)
	_, err = io.ReadFull(file, data)
	if err != nil {
		return false, err
	}
	err = os.WriteFile(file.Name(), data, 0600)
	if err != nil {
		return false, err
	}
	file.end = offset
	return true, nil
}
