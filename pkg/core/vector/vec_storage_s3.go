package vector

import (
	"context"
	"io"
	"os"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/lru_cache"
)

type s3Storage struct {
	cli        *minio.Client
	prefix     string
	cachePath  string
	bucketName string
	cache      *lru_cache.LruCache
}

func (s *s3Storage) ExistsFile(name string) (bool, error) {
	key := path.Join(s.prefix, name)
	_, err := s.cli.StatObject(context.Background(), s.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		if rsp, ok := err.(minio.ErrorResponse); ok {
			if rsp.StatusCode == 404 {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func (s *s3Storage) LoadFile(fileName string) (string, io.Closer, error) {
	// check local
	localPath := path.Join(s.cachePath, fileName)
	if cf, ok := s.cache.GetCacheFile(localPath); ok {
		return cf.GetPath(), cf, nil
	}
	key := path.Join(s.prefix, fileName)
	reader, err := s.cli.GetObject(context.Background(), s.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return "", nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	cf, err := s.cache.CacheFile(localPath, reader)
	if err != nil {
		return "", nil, err
	}
	return cf.GetPath(), cf, nil
}

func (s *s3Storage) SaveFile(inputFile string, name string) error {
	f, err := os.OpenFile(inputFile, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	key := path.Join(s.prefix, name)
	_, err = s.cli.PutObject(context.Background(), s.bucketName, key, f, info.Size(), minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	// cache file
	localPath := path.Join(s.cachePath, name)
	_, err = s.cache.UpdateCacheFile(inputFile, localPath)
	return err
}

// Remove vector index, the input name should be index_name/vec_index_name
func (s *s3Storage) Remove(name string) error {
	prefix := path.Join(s.prefix, name)
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}

	objs := s.cli.ListObjects(context.Background(), s.bucketName, opts)

	// s3 list objects return a channel that can only traverse once, so we temp store the keys.
	keys := make([]string, 0)
	// remove local file
	for obj := range objs {
		if obj.Err != nil {
			return obj.Err
		}
		err := s.cache.Remove(path.Join(config.Global.DataPath, obj.Key))
		if err != nil {
			return err
		}
		keys = append(keys, obj.Key)
	}
	// remove obj storage
	for _, key := range keys {
		err := s.cli.RemoveObject(context.Background(), s.bucketName, key, minio.RemoveObjectOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func createS3Storage() (ObjStore, error) {
	accessKeyID := config.Global.S3.AccessId
	accessKeySecret := config.Global.S3.AccessSecret
	bucketName := config.Global.S3.Bucket
	endPoint := config.Global.S3.Endpoint
	useV4Sig := config.Global.S3.UseV4Signature
	useHTTPS := config.Global.S3.UseHttps
	pathStyleRequest := config.Global.S3.PathStyleRequest
	awsRegion := config.Global.S3.AwsRegion

	var bucketLookup minio.BucketLookupType
	if pathStyleRequest {
		bucketLookup = minio.BucketLookupPath
	}
	if endPoint == "" {
		if awsRegion != "" {
			endPoint = "s3." + awsRegion + ".amazonaws.com"
		} else {
			endPoint = "s3.amazonaws.com"
		}
	}

	s3 := &s3Storage{
		cache:      lru_cache.Instance,
		bucketName: bucketName,
		prefix:     VecPrefix,
		cachePath:  path.Join(config.Global.DataPath, VecPrefix),
	}
	var err error
	if useV4Sig {
		s3.cli, err = minio.New(endPoint, &minio.Options{
			Creds:        credentials.NewStaticV4(accessKeyID, accessKeySecret, ""),
			Secure:       useHTTPS,
			Region:       awsRegion,
			BucketLookup: bucketLookup,
		})
	} else {
		s3.cli, err = minio.New(endPoint, &minio.Options{
			Creds:        credentials.NewStaticV2(accessKeyID, accessKeySecret, ""),
			Secure:       useHTTPS,
			BucketLookup: bucketLookup,
		})
	}
	if err != nil {
		return nil, err
	}

	return s3, nil
}
