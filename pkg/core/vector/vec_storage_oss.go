package vector

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/haiwen/goutils/objclient"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/lru_cache"
)

type OssStorage struct {
	cli       objclient.Client
	prefix    string
	cache     *lru_cache.LruCache
	cachePath string
}

func (o *OssStorage) ExistsFile(name string) (bool, error) {
	key := path.Join(o.prefix, name, getFileName())
	exists, err := o.cli.Exist(context.Background(), key)
	if err != nil {
		log.Error().Err(err).Msgf("check vec index exists %s err: ", name)
		return false, err
	}
	return exists, nil
}

func (o *OssStorage) LoadFile(name string) (string, io.Closer, error) {
	// check local
	localPath := path.Join(o.cachePath, name, getFileName())
	if cf, ok := o.cache.GetCacheFile(localPath); ok {
		return cf.GetPath(), cf, nil
	}
	key := path.Join(o.prefix, name, getFileName())
	reader, err := o.cli.Read(context.Background(), key)
	if err != nil {
		log.Error().Err(err).Msgf("Load vec index object %s err: ", name)
		return "", nil, fmt.Errorf("load vec index err: get object err: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()
	cf, err := o.cache.CacheFile(localPath, reader)
	if err != nil {
		return "", nil, fmt.Errorf("load file err: %w", err)
	}
	return cf.GetPath(), cf, nil
}

func (o *OssStorage) SaveFile(inputFile string, name string) error {
	f, err := os.OpenFile(inputFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Error().Err(err).Msgf("Save vec index %s file err: open temp file err: ", name)
		return fmt.Errorf("save vec index err: open temp file err: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()
	key := path.Join(o.prefix, name, getFileName())
	err = o.cli.Write(context.Background(), key, f, &objclient.WriteOptions{})
	if err != nil {
		log.Error().Err(err).Msgf("Save vec index file %s err: save file to obj store err: ", name)
		return fmt.Errorf("save vec index err: save file to obj store err: %w", err)
	}
	localPath := path.Join(o.cachePath, name, getFileName())
	_, err = o.cache.UpdateCacheFile(inputFile, localPath)
	return err
}

// Remove vector index, the input name should be index_name/vec_index_name
func (o *OssStorage) Remove(name string) error {
	prefix := path.Join(o.prefix, name)
	objs, err := o.cli.List(context.Background(), prefix)
	if err != nil {
		log.Error().Err(err).Msgf("Remove vec index file %s err: ", name)
		return fmt.Errorf("remove vec index file err: %w", err)
	}
	keys := make([]string, 0, len(objs))
	// remove local file
	for _, obj := range objs {
		err = o.cache.Remove(path.Join(config.Global.DataPath, obj.Key))
		if err != nil {
			return err
		}
		keys = append(keys, obj.Key)
	}
	// remove obj storage
	err = o.cli.Remove(context.Background(), keys...)
	if err != nil {
		log.Error().Err(err).Msgf("Remove vec index file %s err: delete object err: ", name)
		return fmt.Errorf("remove vec index file err: delete object err: %w", err)
	}
	return nil
}

func createOssStorage() (ObjStore, error) {
	o := &OssStorage{
		cache:     lru_cache.Instance,
		prefix:    VecPrefix,
		cachePath: path.Join(config.Global.DataPath, VecPrefix),
	}
	var err error
	o.cli, err = createOssClient()
	if err != nil {
		return nil, err
	}
	return o, nil
}

func listOssVecStoreSegments(prefix string) ([]string, error) {
	o, err := createOssClient()
	if err != nil {
		log.Error().Err(err).Msgf("list vec store segments %s err: create oss client err: ", prefix)
		return nil, fmt.Errorf("create oss client err: %w", err)
	}

	objs, err := o.List(context.Background(), prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}
	res := make([]string, 0, len(objs))
	for _, obj := range objs {
		res = append(res, obj.Key)
	}
	return res, nil
}

func createOssClient() (objclient.Client, error) {
	var objConf objclient.OSSConfig
	objConf.KeyID = config.Global.Oss.AccessId
	objConf.Key = config.Global.Oss.AccessSecret
	objConf.Bucket = config.Global.Oss.Bucket
	objConf.Endpoint = config.Global.Oss.Endpoint

	return objclient.NewOSSClient(objConf)
}
