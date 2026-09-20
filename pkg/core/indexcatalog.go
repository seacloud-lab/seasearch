package core

import (
	"github.com/zincsearch/zincsearch/pkg/cluster"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
)

func GetIndexMetadata(name string) (*meta.Index, error) {
	if !cluster.AssignCheck(name) {
		return nil, ErrIndexServerMismatch
	}
	return metadata.Index.Get(name)
}

func ListIndexMetadata() ([]*meta.Index, error) {
	indexes, err := metadata.Index.List(0, 0)
	if err != nil {
		return nil, err
	}
	result := make([]*meta.Index, 0, len(indexes))
	for _, index := range indexes {
		if cluster.AssignCheck(index.Name) {
			result = append(result, index)
		}
	}
	return result, nil
}

func ListIndexNames() ([]string, error) {
	names, err := metadata.Index.ListNames(0, 0)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(names))
	for _, name := range names {
		if cluster.AssignCheck(name) {
			result = append(result, name)
		}
	}
	return result, nil
}
