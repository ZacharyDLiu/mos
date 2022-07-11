package file

import "os"

type MmapFile struct {
	Data []byte
	File *os.File
}

func OpenMmapFile(name string, flag int, size int) (*MmapFile, error) {
	file, err := os.OpenFile(name, flag, 0666)
	if err != nil {
		return nil, err
	}
	if
}
