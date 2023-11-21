package vector

import (
	"os"
	"path/filepath"
)

const IndexExt = ".index"
const TempExt = ".temp"

// VecPrefix Used to distinguish between vec index and bluge data stored in oss
const VecPrefix = "vec_index"
const (
	None   Action = -1
	Insert Action = 0
	Update Action = 1
	Delete Action = 2
)

const IvfPQ = "ivf_pq"
const Flat = "flat"

type Action int

// VecAction used for write vector index
type VecAction struct {
	// document Id
	DocId  string
	Vector []float32
	// insert | update | delete
	Action Action
	// zinc index name
	Index string
	Field string
}

func checkPath(localPath string) error {
	localDir, _ := filepath.Split(localPath)
	_, err := os.Stat(localDir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(localDir, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}
