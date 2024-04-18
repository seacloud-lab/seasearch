package directory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/lru_cache"
)

func GetS3Config(rootPath string, indexName string, timeRange ...int64) bluge.Config {
	cfg := index.DefaultConfig(path.Join(rootPath, indexName))
	cfg = cfg.WithPersisterNapTimeMSec(50)
	cfg.DirectoryFunc = func() index.Directory {
		dir, err := CreateS3Backend(rootPath, indexName)
		if err != nil {
			panic(err)
		}
		return dir
	}

	if len(timeRange) == 2 {
		if timeRange[0] <= timeRange[1] {
			cfg = cfg.WithTimeRange(timeRange[0], timeRange[1])
		}
	}
	cfg.IndexName = indexName
	return bluge.DefaultConfigWithIndexConfig(cfg)
}

type S3Backend struct {
	prefix string
	path   string
	cache  *lru_cache.LruCache
	s3Client
}

type s3Client struct {
	client     *minio.Client
	bucketName string
}

func CreateS3Backend(dataPath string, indexName string) (*S3Backend, error) {
	cli, err := createS3Client()
	if err != nil {
		return nil, err
	}
	s3 := &S3Backend{}
	s3.cache = lru_cache.Instance
	s3.prefix = indexName
	s3.path = path.Join(dataPath, indexName)
	s3.s3Client = *cli
	return s3, nil
}

var createS3Once = sync.Once{}
var s3cli *s3Client

func createS3Client() (*s3Client, error) {
	var err error
	createS3Once.Do(func() {
		accessKeyID := config.Global.S3.AccessId
		accessKeySecret := config.Global.S3.AccessSecret
		bucketName := config.Global.S3.Bucket
		endPoint := config.Global.S3.Endpoint
		useV4Sig := config.Global.S3.UseV4Signature
		useHTTPS := config.Global.S3.UseHttps
		pathStyleRequest := config.Global.S3.PathStyleRequest
		awsRegion := config.Global.S3.AwsRegion
		s3cli, err = newS3client(
			accessKeyID, accessKeySecret, bucketName,
			endPoint, awsRegion, useHTTPS, pathStyleRequest, useV4Sig)
	})

	return s3cli, err
}

func newS3client(accessKeyID, accessKeySecret, bucketName, endPoint, region string, useHTTPS, pathStyleRequest, useV4Sig bool) (*s3Client, error) {
	var bucketLookup minio.BucketLookupType
	if pathStyleRequest {
		bucketLookup = minio.BucketLookupPath
	}
	if endPoint == "" {
		if region != "" {
			endPoint = "s3." + region + ".amazonaws.com"
		} else {
			endPoint = "s3.amazonaws.com"
		}
	}
	if useV4Sig {
		cli, err := minio.New(endPoint, &minio.Options{
			Creds:        credentials.NewStaticV4(accessKeyID, accessKeySecret, ""),
			Secure:       useHTTPS,
			Region:       region,
			BucketLookup: bucketLookup,
		})
		return &s3Client{client: cli, bucketName: bucketName}, err
	}
	cli, err := minio.New(endPoint, &minio.Options{
		Creds:        credentials.NewStaticV2(accessKeyID, accessKeySecret, ""),
		Secure:       useHTTPS,
		BucketLookup: bucketLookup,
	})
	return &s3Client{client: cli, bucketName: bucketName}, err
}

// RemoveS3Index
// used for delete full Index
func RemoveS3Index(indexName string) error {
	s3, err := createS3Client()
	if err != nil {
		log.Error().Err(err).Msgf("Remove index %s err: create s3 client err: ", indexName)
		return fmt.Errorf("create s3 client err: %w", err)
	}
	objs, err := s3.listObjects(indexName)
	if err != nil {
		log.Error().Err(err).Msgf("Remove index %s err: ", indexName)
		return err
	}
	dataPath := config.Global.DataPath
	// remove local cache
	for _, obj := range objs {
		err = lru_cache.Instance.Remove(path.Join(dataPath, obj.Key))
		if err != nil {
			return err
		}
	}
	// remove obj storage
	for _, obj := range objs {
		err = s3.remove(obj.Key)
		if err != nil {
			log.Error().Err(err).Msgf("Remove index %s err: remove object err: ", indexName)
			return fmt.Errorf("failed to remove object: %w", err)
		}
	}
	return nil
}

// S3Exists
// used for test index delete
func S3Exists(indexName string) (bool, error) {
	s3, err := createS3Client()
	if err != nil {
		return false, err
	}
	objs, err := s3.listObjects(indexName)
	if err != nil {
		return false, err
	}
	return len(objs) > 0, nil
}

func (b *s3Client) read(key string) (io.ReadCloser, error) {
	return b.client.GetObject(context.Background(), b.bucketName, key, minio.GetObjectOptions{})
}

func (b *s3Client) write(key string, r io.Reader, l int) error {
	opts := minio.PutObjectOptions{}
	return b.writeWithMeta(key, opts, r, int64(l))
}

func (b *s3Client) writeWithMeta(key string, opts minio.PutObjectOptions, r io.Reader, l int64) error {
	// we should always set objectSize instead of -1 to reduce memory overhead
	_, err := b.client.PutObject(context.Background(), b.bucketName, key, r, l, opts)
	if err != nil {
		return err
	}
	return nil
}

func (b *s3Client) listObjects(prefix string) ([]minio.ObjectInfo, error) {
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	objs := b.client.ListObjects(context.Background(), b.bucketName, opts)

	var info []minio.ObjectInfo
	for obj := range objs {
		if obj.Err != nil {
			return nil, fmt.Errorf("failed to list folder: %w", obj.Err)
		}
		info = append(info, obj)
	}

	return info, nil
}

func (b *s3Client) remove(key string) error {
	opts := minio.RemoveObjectOptions{}
	err := b.client.RemoveObject(context.Background(), b.bucketName, key, opts)
	return err
}

func (b *S3Backend) Setup(readOnly bool) error {
	return b.cache.Setup(b.path, readOnly)
}

func (b *S3Backend) List(kind string) ([]uint64, error) {
	list, err := b.listObjects(b.prefix)
	if err != nil {
		log.Error().Err(err).Msgf("List index %s err: ", b.prefix)
		return nil, fmt.Errorf("list objects err: %w", err)
	}
	var itemList uint64Slice

	for _, item := range list {
		if filepath.Ext(item.Key) != kind {
			continue
		}
		stringID := filepath.Base(item.Key)
		stringID = stringID[:len(stringID)-len(kind)]
		parsedID, err := strconv.ParseUint(stringID, 16, 64)
		if err != nil {
			log.Error().Err(err).Msgf("List index %s err: failed to parse object id %s :", b.prefix, stringID)
			continue
		}
		itemList = append(itemList, parsedID)
	}
	sort.Sort(sort.Reverse(itemList))
	return itemList, nil
}

func (b *S3Backend) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	key := fileName(kind, id)
	if cf, ok := b.cache.GetCacheFile(path.Join(b.path, key)); ok {
		return cf.LoadReadOnlyData()
	}

	reader, err := b.read(path.Join(b.prefix, key))
	if err != nil {
		log.Error().Err(err).Msgf("Load index %s file err: Read file from s3 err: ", b.prefix)
		return nil, nil, fmt.Errorf("load file err: read file from s3 err: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()

	cf, err := b.cache.CacheFile(path.Join(b.path, key), reader)
	if err != nil {
		return nil, nil, fmt.Errorf("load file err: %w", err)
	}
	return cf.LoadReadOnlyData()
}

func (b *S3Backend) Persist(kind string, id uint64, w index.WriterTo, closeCh chan struct{}) error {
	key := fileName(kind, id)
	fipath := path.Join(b.path, key)
	backendKey := path.Join(b.prefix, key)

	errCh1 := make(chan error, 1)
	errCh2 := make(chan error, 1)
	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func(ch chan error) {
		defer wg.Done()
		buffer := bytes.Buffer{}
		_, err := w.WriteTo(&buffer, closeCh)
		if err != nil {
			ch <- err
			return
		}
		err = b.write(backendKey, &buffer, buffer.Len())
		if err != nil {
			ch <- err
			return
		}
		close(ch)
	}(errCh1)

	go func(ch chan error) {
		defer wg.Done()

		var err error
		writer, err := b.cache.OpenWriter(fipath)
		if err != nil {
			ch <- fmt.Errorf("open writer error: %w", err)
			return
		}

		_, err = w.WriteTo(writer, closeCh)
		if err != nil {
			ch <- fmt.Errorf("write to file error: %w", err)
			return
		}

		err = writer.Close()
		if err != nil {
			ch <- fmt.Errorf("close file error: %w", err)
			return
		}
		close(ch)
	}(errCh2)

	wg.Wait()

	err1, ok := <-errCh1
	if ok {
		log.Error().Err(err1).Msgf("Persist index %s error: persist to obj store err: ", b.prefix)
	}

	err2, ok := <-errCh2
	if ok {
		log.Error().Err(err2).Msgf("Persist index %s error: persist to fs cache err: ", b.prefix)
	}

	if err1 != nil {
		_ = b.cache.Remove(fipath)
		return err1
	}
	return nil
}

func (b *S3Backend) Remove(kind string, id uint64) error {
	key := fileName(kind, id)
	err := b.cache.Remove(path.Join(b.path, key))
	if err != nil {
		return err
	}

	err = b.remove(path.Join(b.prefix, key))
	if err != nil {
		log.Error().Err(err).Msgf("Remove index file %s from obj store err: ", path.Join(b.prefix, key))
		return fmt.Errorf("remove object err: %w", err)
	}
	return nil
}

func (b *S3Backend) Stats() (numItems uint64, numBytes uint64) {
	objs, err := b.listObjects(b.prefix)
	if err != nil {
		log.Error().Err(err).Msgf("Stats index %s err: list objects err: ", b.prefix)
		return 0, 0
	}
	objectCount := uint64(0)
	sizeOfObjects := uint64(0)

	for _, obj := range objs {
		size := uint64(obj.Size)
		objectCount++
		sizeOfObjects += size
	}
	return objectCount, sizeOfObjects
}

func (b *S3Backend) Sync() error {
	return b.cache.Sync(b.path)
}

func (b *S3Backend) Lock() error {
	return nil
}

func (b *S3Backend) Unlock() error {
	return nil
}

func fileName(kind string, id uint64) string {
	return fmt.Sprintf("%012x", id) + kind
}
