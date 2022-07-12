package file

import (
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"reflect"
	"unsafe"
)

type MmapFile struct {
	Data []byte
	File *os.File
	End  int
}

// OpenMmapFile open or create a file for read and write. if the size parameter is greater than the file size
// we truncate the file to the given size, otherwise we mmap
func OpenMmapFile(name string, size int) (*MmapFile, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := int(stat.Size())
	length := fileSize + size
	err = file.Truncate(int64(length))
	if err != nil {
		return nil, err
	}
	data, err := unix.Mmap(int(file.Fd()), 0, length, unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &MmapFile{Data: data, File: file, End: fileSize}, nil
}

func (m *MmapFile) Read(offset int, size int) ([]byte, error) {
	if len(m.Data[offset:]) < size {
		return nil, io.EOF
	}
	return m.Data[offset : offset+size], nil
}

func (m *MmapFile) Append(data []byte, sync bool) (int, error) {
	if m.End+len(data) > len(m.Data) {
		err := m.Truncate(m.End + len(data))
		if err != nil {
			return 0, err
		}
	}
	size := copy(m.Data[m.End:], data)
	m.End += size
	if sync {
		err := m.Sync()
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func (m *MmapFile) Truncate(size int) error {
	err := m.Sync()
	if err != nil {
		return err
	}
	err = m.File.Truncate(int64(size))
	if err != nil {
		return err
	}
	m.Data, err = mremap(m.Data, size)
	return err
}

func (m *MmapFile) Sync() error {
	return unix.Msync(m.Data, unix.MS_SYNC)
}

func (m *MmapFile) Close() {
	m.Sync()
	munmap(m.Data)
	m.File.Close()
}

// mremap is a Linux-specific system call to remap pages in memory. This can be used in place of munmap + mmap.
func mremap(data []byte, size int) ([]byte, error) {
	// taken from <https://github.com/torvalds/linux/blob/f8394f232b1eab649ce2df5c5f15b0e528c92091/include/uapi/linux/mman.h#L8>
	const MREMAP_MAYMOVE = 0x1

	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	mmapAddr, mmapSize, errno := unix.Syscall6(
		unix.SYS_MREMAP,
		header.Data,
		uintptr(header.Len),
		uintptr(size),
		uintptr(MREMAP_MAYMOVE),
		0,
		0,
	)
	if errno != 0 {
		return nil, errno
	}
	if mmapSize != uintptr(size) {
		return nil, fmt.Errorf("mremap size mismatch: requested: %d got: %d", size, mmapSize)
	}

	header.Data = mmapAddr
	header.Cap = size
	header.Len = size
	return data, nil
}

// munmap unmaps a previously mapped slice.
//
// unix.Munmap maintains an internal list of mmapped addresses, and only calls munmap
// if the address is present in that list. If we use mremap, this list is not updated.
// To bypass this, we call munmap ourselves.
func munmap(data []byte) error {
	if len(data) == 0 || len(data) != cap(data) {
		return unix.EINVAL
	}
	_, _, errno := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}
