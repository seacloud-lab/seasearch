package vector

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
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
	key := path.Join(o.prefix, name)
	return o.bucket.IsObjectExist(key)
}

func (o *OssStorage) LoadFile(fileName string) (string, io.Closer, error) {
	// check local
	localPath := path.Join(o.cachePath, fileName)
	if cf, ok := o.cache.GetCacheFile(localPath); ok {
		return cf.GetPath(), cf, nil
	}
	key := path.Join(o.prefix, fileName)
	reader, err := o.bucket.GetObject(key)
	if err != nil {
		return "", nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	cf, err := o.cache.CacheFile(localPath, reader)
	if err != nil {
		return "", nil, err
	}
	return cf.GetPath(), cf, nil
}

func (o *OssStorage) SaveFile(inputFile string, fileName string) error {
	f, err := os.OpenFile(inputFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	key := path.Join(o.prefix, fileName)
	err = o.bucket.PutObject(key, f)
	if err != nil {
		return err
	}
	localPath := path.Join(o.cachePath, fileName)
	_, err = o.cache.UpdateCacheFile(inputFile, localPath)
	return err
}

// Remove vector index, the input name should be index_name/vec_index_name
func (o *OssStorage) Remove(name string) error {
	prefix := path.Join(o.prefix, name)
	objs, err := o.listObjects(prefix)
	if err != nil {
		return err
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
			return err
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
