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

type s3Storage struct {
	cli       objclient.Client
	prefix    string
	cachePath string
	cache     *lru_cache.LruCache
}

func (s *s3Storage) ExistsFile(name string) (bool, error) {
	key := path.Join(s.prefix, name, getFileName())
	exists, err := s.cli.Exist(context.Background(), key)
	if err != nil {
		log.Error().Err(err).Msgf("check vec index exists %s err: ", name)
		return false, err
	}
	return exists, nil
}

func (s *s3Storage) LoadFile(name string) (string, io.Closer, error) {
	// check local
	localPath := path.Join(s.cachePath, name, getFileName())
	if cf, ok := s.cache.GetCacheFile(localPath); ok {
		return cf.GetPath(), cf, nil
	}
	key := path.Join(s.prefix, name, getFileName())
	reader, err := s.cli.Read(context.Background(), key)
	if err != nil {
		log.Error().Err(err).Msgf("Load vec index object %s err: ", name)
		return "", nil, fmt.Errorf("load vec index err: get object err: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()
	cf, err := s.cache.CacheFile(localPath, reader)
	if err != nil {
		return "", nil, fmt.Errorf("load file err: %w", err)
	}
	return cf.GetPath(), cf, nil
}

func (s *s3Storage) SaveFile(inputFile string, name string) error {
	f, err := os.OpenFile(inputFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Error().Err(err).Msgf("Save vec index file %s err: open temp file err: ", name)
		return fmt.Errorf("save vec index err: open temp file err: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	key := path.Join(s.prefix, name, getFileName())
	err = s.cli.Write(context.Background(), key, f, &objclient.WriteOptions{
		Size: info.Size(),
	})
	if err != nil {
		log.Error().Err(err).Msgf("Save vec index file %s err: save file to obj store err: ", name)
		return fmt.Errorf("save vec index err: save file to obj store err: %w", err)
	}

	// cache file
	localPath := path.Join(s.cachePath, name, getFileName())
	return s.cache.UpdateCacheFile(inputFile, localPath)
}

// Remove vector index, the input name should be index_name/vec_index_name
func (s *s3Storage) Remove(name string) error {
	prefix := path.Join(s.prefix, name)
	objs, err := s.cli.List(context.Background(), prefix)
	if err != nil {
		log.Error().Err(err).Msgf("Remove vec index file %s err: list objects err: ", name)
		return fmt.Errorf("remove vec index file err: list objects err: %w", err)
	}

	keys := make([]string, 0, len(objs))
	// remove local file
	for _, obj := range objs {
		err := s.cache.Remove(path.Join(config.Global.DataPath, obj.Key))
		if err != nil {
			return err
		}
		keys = append(keys, obj.Key)
	}
	// remove obj storage
	err = s.cli.Remove(context.Background(), keys...)
	if err != nil {
		log.Error().Err(err).Msgf("Remove vec index file %s err: delete object err: ", name)
		return fmt.Errorf("remove vec index file err: delete object err: %w", err)
	}
	return nil
}

func createS3Storage() (ObjStore, error) {
	s3 := &s3Storage{
		cache:     lru_cache.Instance,
		prefix:    VecPrefix,
		cachePath: path.Join(config.Global.DataPath, VecPrefix),
	}
	var err error
	s3.cli, err = createS3Client()
	if err != nil {
		return nil, err
	}
	return s3, nil
}

func listS3VecStoreSegments(prefix string) ([]string, error) {
	s3, err := createS3Client()
	if err != nil {
		log.Error().Err(err).Msgf("list vec store segments %s err: create s3 client err: ", prefix)
		return nil, fmt.Errorf("create s3 client err: %w", err)
	}

	objs, err := s3.List(context.Background(), prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list folder: %w", err)
	}
	res := make([]string, 0, len(objs))
	for _, obj := range objs {
		res = append(res, obj.Key)
	}
	return res, nil
}

func createS3Client() (objclient.Client, error) {
	var objConf objclient.S3Config
	objConf.KeyID = config.Global.S3.AccessId
	objConf.Key = config.Global.S3.AccessSecret
	objConf.Bucket = config.Global.S3.Bucket
	if config.Global.S3.UseV4Signature != "" {
		objConf.V4Signature = config.Global.S3.UseV4Signature
	}
	if config.Global.S3.UseHttps != "" {
		objConf.HTTPS = config.Global.S3.UseHttps
	}
	if config.Global.S3.PathStyleRequest != "" {
		objConf.PathStyleRequest = config.Global.S3.PathStyleRequest
	}
	objConf.Endpoint = config.Global.S3.Endpoint
	objConf.Region = config.Global.S3.AwsRegion
	objConf.SSECKey = config.Global.S3.SsecKey

	return objclient.NewS3Client(objConf)
}
