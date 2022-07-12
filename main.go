package main

import (
	"fmt"
	"test/file"
)

func main() {
	s, err := file.NewStorage("test", 1<<20)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer s.Close()
	err = s.Put([]byte("key1"), []byte("random value for test"))
	if err != nil {
		fmt.Println(err)
		return
	}
	value, err := s.Get([]byte("key1"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(value), string(value) == "random value for test")
}
