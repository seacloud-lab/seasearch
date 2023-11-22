package vector

import (
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

func (o *OssStorage) Close() error {
	return o.cache.Close()
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

func (o *OssStorage) Remove(name string) error {
	key := path.Join(o.prefix, name)
	err := o.bucket.DeleteObject(key)

	err2 := o.cache.Remove(path.Join(o.cachePath, name))
	if err == nil {
		err = err2
	}
	return err
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
		cache: lru_cache.GetCache(path.Join(config.Global.DataPath, VecPrefix), func(ext string) bool {
			return ext == IndexExt
		}, TempExt),
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
