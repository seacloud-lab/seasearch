package vector

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/zincsearch/zincsearch/pkg/config"
)

// ObjStore
// fileName format: $zincIndexName/$vectorIndexName/index.index
type ObjStore interface {
	// ExistsFile check the file exists
	ExistsFile(fileName string) (bool, error)
	// LoadFile get file from obj storage
	// the file will be cached into disk. It will return cached file fullPath
	LoadFile(fileName string) (string, io.Closer, error)
	// SaveFile save a disk file into obj storage, and cache it.
	SaveFile(inputFile string, fileName string) error
	Remove(name string) error
	Close() error
}

func GetVectorStorage() (ObjStore, error) {
	if config.Global.StorageType == "s3" {
		return createS3Storage()
	}
	if config.Global.StorageType == "oss" {
		return createOssStorage()
	}
	if config.Global.StorageType == "disk" {
		return createDiskStore()
	}
	return nil, fmt.Errorf("unsupport storage type")
}

type diskStore struct {
	rootPath string
}

func createDiskStore() (ObjStore, error) {
	return &diskStore{
		rootPath: path.Join(config.Global.DataPath, VecPrefix),
	}, nil
}

func (d *diskStore) Close() error {
	return nil
}

func (d *diskStore) ExistsFile(fileName string) (bool, error) {
	_, err := os.Stat(path.Join(d.rootPath, fileName))
	if err != nil {
		return false, nil
	}
	return true, nil
}

type emptyCloser struct {
}

func (e emptyCloser) Close() error {
	return nil
}

func (d *diskStore) LoadFile(fileName string) (string, io.Closer, error) {
	localPath := path.Join(d.rootPath, fileName)
	_, err := os.Stat(localPath)
	if err != nil {
		return "", nil, err
	}
	return localPath, emptyCloser{}, nil
}

func (d *diskStore) SaveFile(inputFile string, fileName string) error {
	localPath := path.Join(d.rootPath, fileName)
	err := checkPath(localPath)
	if err != nil {
		return err
	}
	return os.Rename(inputFile, localPath)
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

func (d *diskStore) Remove(name string) error {
	localPath := path.Join(d.rootPath, name)
	return os.RemoveAll(localPath)
}
