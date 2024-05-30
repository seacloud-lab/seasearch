package vector

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
)

// ObjStore
// fileName format: $zincIndexName/$vectorIndexName/index.index
type ObjStore interface {
	// ExistsFile check the file exists
	ExistsFile(name string) (bool, error)
	// LoadFile get file from obj storage
	// the file will be cached into disk. It will return cached file fullPath
	LoadFile(name string) (string, io.Closer, error)
	// SaveFile save a disk file into obj storage, and cache it.
	SaveFile(inputFile string, name string) error
	Remove(name string) error
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

func getFileName() string {
	return fmt.Sprintf("index%s", IndexExt)
}

type diskStore struct {
	rootPath string
}

func createDiskStore() (ObjStore, error) {
	return &diskStore{
		rootPath: path.Join(config.Global.DataPath, VecPrefix),
	}, nil
}

func (d *diskStore) ExistsFile(fileName string) (bool, error) {
	_, err := os.Stat(path.Join(d.rootPath, fileName, getFileName()))
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

func (d *diskStore) LoadFile(name string) (string, io.Closer, error) {
	localPath := path.Join(d.rootPath, name, getFileName())
	_, err := os.Stat(localPath)
	if err != nil {
		log.Error().Err(err).Msgf("Load vec index object %s err: ", name)
		return "", nil, fmt.Errorf("load vec index err: get object err: %w", err)
	}
	return localPath, emptyCloser{}, nil
}

func (d *diskStore) SaveFile(inputFile string, name string) error {
	localPath := path.Join(d.rootPath, name, getFileName())
	err := checkPath(localPath)
	if err != nil {
		log.Error().Err(err).Msgf("Save vec index %s file err: check path err: ", name)
		return fmt.Errorf("save vec index err: check path err: %w", err)
	}
	err = os.Rename(inputFile, localPath)
	if err != nil {
		log.Error().Err(err).Msgf("Save vec index %s file err: rename file err: ", name)
		return err
	}
	return nil
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
	err := os.RemoveAll(localPath)
	if err != nil {
		log.Error().Err(err).Msgf("Remove vec index file %s err: ", name)
		return err
	}
	return nil
}

func ListVecSegments(vecIndexName string) (map[int64]struct{}, error) {
	var files []string
	var err error
	prefix := path.Join(VecPrefix, vecIndexName)
	switch config.Global.StorageType {
	case "oss":
		files, err = listOssVecStoreSegments(prefix)
	case "s3":
		files, err = listS3VecStoreSegments(prefix)
	default:
		files, err = listDisVecStoreSegments(prefix)
	}
	if err != nil {
		return nil, err
	}

	// .../.../vec_index/$index_name/$vector_field/$segment_id/stored_vec/000000000.seg
	ids := make(map[int64]struct{})
	for _, f := range files {
		// we ignore faiss index file
		if strings.HasSuffix(f, "index.index") {
			continue
		}
		idStr := filepath.Base(filepath.Dir(filepath.Dir(f)))
		id, err := strconv.ParseInt(idStr, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("got invalid dir name %s, parse seg id err: %w", idStr, err)
		}
		ids[id] = struct{}{}
	}

	return ids, nil
}

func listDisVecStoreSegments(prefix string) ([]string, error) {
	walkPath := path.Join(config.Global.DataPath, prefix)
	_, err := os.Stat(walkPath)
	if err != nil {
		// band new ivf_pq index without any segments
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	result := make([]string, 0)
	err = filepath.Walk(walkPath, func(p string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return err
		}
		result = append(result, p)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
