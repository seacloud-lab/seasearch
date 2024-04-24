package vector

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/lru_cache"
)

type OssStorage struct {
	client    *oss.Client
	bucket    *oss.Bucket
	prefix    string
	cache     *lru_cache.LruCache
	cachePath string
}

func (o *OssStorage) ExistsFile(name string) (bool, error) {
	key := path.Join(o.prefix, name, getFileName())
	exists, err := o.bucket.IsObjectExist(key)
	if err != nil {
		log.Err(err).Msgf("Exists vec index %s err: ", name)
		return exists, fmt.Errorf("get exeists err: %w", err)
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
	reader, err := o.bucket.GetObject(key)
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
	err = o.bucket.PutObject(key, f)
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
	objs, err := o.listObjects(prefix)
	if err != nil {
		log.Error().Err(err).Msgf("Remove vec index file %s err: ", name)
		return fmt.Errorf("remove vec index file err: %w", err)
	}
	// remove local file
	for _, obj := range objs {
		err = o.cache.Remove(path.Join(config.Global.DataPath, obj.Key))
		if err != nil {
			return err
		}
	}
	// remove obj storage
	for _, obj := range objs {
		err = o.bucket.DeleteObject(obj.Key)
		if err != nil {
			log.Error().Err(err).Msgf("Remove vec index file %s err: delete object err: ", name)
			return fmt.Errorf("remove vec index file err: delete object err: %w", err)
		}
	}
	return nil
}

func (o *OssStorage) listObjects(prefix string) ([]oss.ObjectProperties, error) {
	opts := []oss.Option{oss.Prefix(prefix), oss.MaxKeys(10)}

	var info []oss.ObjectProperties
	for i := 0; ; i++ {
		result, err := o.bucket.ListObjectsV2(opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		info = append(info, result.Objects...)
		if !result.IsTruncated {
			break
		} else if i == 0 {
			opts = append(opts, oss.ContinuationToken(result.NextContinuationToken))
		} else {
			opts[len(opts)-1] = oss.ContinuationToken(result.NextContinuationToken)
		}
	}

	return info, nil
}

func createOssStorage() (ObjStore, error) {
	accessKeyID := config.Global.Oss.AccessId
	accessKeySecret := config.Global.Oss.AccessSecret
	bucketName := config.Global.Oss.Bucket
	endPoint := config.Global.Oss.Endpoint

	client, err := oss.New(endPoint, accessKeyID, accessKeySecret)
	if err != nil {
		return nil, err
	}
	o := &OssStorage{
		cache:     lru_cache.Instance,
		client:    client,
		prefix:    VecPrefix,
		cachePath: path.Join(config.Global.DataPath, VecPrefix),
	}
	o.bucket, err = client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	return o, nil
}
