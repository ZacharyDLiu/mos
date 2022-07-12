package file

type Handle struct {
	offset int
	size   int
}

type Storage struct {
	DataStorage  *MmapFile
	IndexStorage map[string]Handle
}

func NewStorage(name string, size int) (*Storage, error) {
	mmap, err := OpenMmapFile(name, size)
	if err != nil {
		return nil, err
	}
	index := make(map[string]Handle, size)
	return &Storage{DataStorage: mmap, IndexStorage: index}, nil
}

func (s *Storage) Put(key []byte, value []byte) error {
	offset := s.DataStorage.End + len(key)
	data := make([]byte, len(key)+len(value))
	copy(data[:len(key)], key)
	copy(data[len(key):], value)
	_, err := s.DataStorage.Append(data, true)
	if err != nil {
		return err
	}
	s.IndexStorage[string(key)] = Handle{size: len(value), offset: offset}
	return nil
}

func (s *Storage) Get(key []byte) ([]byte, error) {
	h := s.IndexStorage[string(key)]
	return s.DataStorage.Read(h.offset, h.size)
}

func (s *Storage) Close() {
	s.DataStorage.Close()
}
