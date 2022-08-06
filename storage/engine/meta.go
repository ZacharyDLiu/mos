package engine

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
)

type Meta struct {
	IndexUpToDate bool  `json:"index_up_to_date"`
	ReusableSpace int64 `json:"reusable_space"`
}

const metaFileName = "meta.json"

func LoadMeta(dir string) (*Meta, error) {
	name := filepath.Join(dir, metaFileName)
	if !Exists(name) {
		return new(Meta), nil
	}
	bytes, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}
	var meta = new(Meta)
	if err := json.Unmarshal(bytes, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func SaveMeta(meta *Meta, dir string) error {
	name := filepath.Join(dir, metaFileName)
	bytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(name, bytes, 0600)
}
