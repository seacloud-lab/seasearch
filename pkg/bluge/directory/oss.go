package directory

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	segment "github.com/blugelabs/bluge_segment_api"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/lru_cache"
)

type OssBackend struct {
	client *oss.Client
	bucket *oss.Bucket
	path   string
	pid    *os.File
	lock   sync.Mutex
	prefix string
	cache  *lru_cache.LruCache
}

func GetOssConfig(rootPath string, indexName string, timeRange ...int64) bluge.Config {
	cfg := index.DefaultConfig(path.Join(rootPath, indexName))
	cfg = cfg.WithPersisterNapTimeMSec(50)
	cfg.DirectoryFunc = func() index.Directory {
		dir, err := CreateOSSBackend(rootPath, indexName)
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
	return bluge.DefaultConfigWithIndexConfig(cfg)
}

func CreateOSSBackend(dataPath string, indexName string) (*OssBackend, error) {
	accessKeyID := config.Global.Oss.AccessId
	accessKeySecret := config.Global.Oss.AccessSecret
	bucketName := config.Global.Oss.Bucket
	endPoint := config.Global.Oss.Endpoint

	o, err := newOSSBackend(endPoint, accessKeyID, accessKeySecret, bucketName)
	if err != nil {
		return nil, err
	}

	o.cache = lru_cache.Instance
	o.prefix = indexName
	o.path = path.Join(dataPath, indexName)

	return o, nil
}

func newOSSBackend(endPoint, accessKeyID, accessKeySecret, bucketName string) (*OssBackend, error) {

	client, err := oss.New(endPoint, accessKeyID, accessKeySecret)
	if err != nil {
		return nil, err
	}

	backend := new(OssBackend)
	backend.client = client
	backend.bucket, err = client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (b *OssBackend) read(key string) (io.ReadCloser, error) {
	return b.bucket.GetObject(key)
}

func (b *OssBackend) write(key string, r io.Reader) error {
	err := b.bucket.PutObject(key, io.NopCloser(r))
	if err != nil {
		return err
	}

	return nil
}

func (b *OssBackend) listObjects(prefix string) ([]oss.ObjectProperties, error) {
	opts := []oss.Option{oss.Prefix(prefix), oss.MaxKeys(10)}

	var info []oss.ObjectProperties
	for i := 0; ; i++ {
		result, err := b.bucket.ListObjectsV2(opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		for _, obj := range result.Objects {
			info = append(info, obj)
		}

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

func (b *OssBackend) remove(key string) error {
	err := b.bucket.DeleteObject(key)
	return err
}

func (b *OssBackend) Setup(readOnly bool) error {
	return nil
}

func (b *OssBackend) List(kind string) ([]uint64, error) {
	list, err := b.listObjects(b.prefix)
	if err != nil {
		return nil, err
	}
	var itemList []uint64

	for _, item := range list {
		if filepath.Ext(item.Key) != kind {
			continue
		}
		stringID := filepath.Base(item.Key)
		stringID = stringID[:len(stringID)-len(kind)]
		parsedID, err := strconv.ParseUint(stringID, 16, 64)
		if err != nil {
			log.Error().Err(err).Msg("List: failed to parse object id: ")
			continue
		}
		itemList = append(itemList, parsedID)
	}
	return itemList, nil
}

func (b *OssBackend) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	key := fileName(kind, id)
	reader, err := b.read(path.Join(b.prefix, key))
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	f, err := b.cache.CacheFile(path.Join(b.path, key), reader)
	if err != nil {
		return nil, nil, err
	}
	return f.LoadReadOnlyData()
}

func (b *OssBackend) Persist(kind string, id uint64, w index.WriterTo, closeCh chan struct{}) error {
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
		err = b.write(backendKey, &buffer)
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
		log.Warn().Err(err1).Msg("persist to obj store failed")
	}

	err2, ok := <-errCh2
	if ok {
		log.Warn().Err(err2).Msg("persist to fs cache failed")
	}

	if err1 != nil {
		_ = b.cache.Remove(fipath)
		return err1
	}

	return nil
}

func (b *OssBackend) Remove(kind string, id uint64) error {
	key := fileName(kind, id)

	err := b.remove(path.Join(b.prefix, key))

	err2 := b.cache.Remove(path.Join(b.path, key))
	if err == nil {
		err = err2
	}
	return err
}

func (b *OssBackend) Stats() (numItems uint64, numBytes uint64) {
	objs, err := b.listObjects(b.prefix)
	if err != nil {
		log.Warn().Err(err).Msg("could not get obj_store stats")
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

func (b *OssBackend) Sync() error {
	return b.cache.Sync(b.path)
}

func (b *OssBackend) Lock() error {
	var err error
	b.pid, err = b.cache.Lock(b.path)
	return err
}

func (b *OssBackend) Unlock() error {
	var err error
	err = b.pid.Close()
	if err != nil {
		return fmt.Errorf("error closing pid file: %w", err)
	}
	return b.cache.Unlock(b.path)
}
