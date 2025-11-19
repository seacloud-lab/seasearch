package directory

import (
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
	"github.com/haiwen/goutils/objclient"
	"github.com/rs/zerolog/log"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/lru_cache"
)

type OssBackend struct {
	path   string
	prefix string
	cache  *lru_cache.LruCache
	objclient.Client
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
	cfg.IndexName = indexName
	return bluge.DefaultConfigWithIndexConfig(cfg)
}

func CreateOSSBackend(dataPath string, indexName string) (*OssBackend, error) {
	cli, err := createOssClient()
	if err != nil {
		return nil, err
	}
	o := &OssBackend{}
	o.cache = lru_cache.Instance
	o.prefix = indexName
	o.path = path.Join(dataPath, indexName)
	o.Client = cli
	return o, nil
}

var createOssOnce = sync.Once{}
var ossBackend objclient.Client

func createOssClient() (objclient.Client, error) {
	var err error
	createOssOnce.Do(func() {
		var objConf objclient.OSSConfig
		objConf.KeyID = config.Global.Oss.AccessId
		objConf.Key = config.Global.Oss.AccessSecret
		objConf.Bucket = config.Global.Oss.Bucket
		objConf.Endpoint = config.Global.Oss.Endpoint
		ossBackend, err = objclient.NewOSSClient(objConf)
	})
	return ossBackend, err
}

// RemoveOssIndex
// used for delete all Index files
func RemoveOssIndex(indexName string) error {
	o, err := createOssClient()
	if err != nil {
		log.Error().Err(err).Msgf("Remove index %s err: create oss client err: ", indexName)
		return fmt.Errorf("create oss client err: %w", err)
	}
	objs, err := o.List(context.Background(), indexName)
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
	// remove object key cache
	objectKeys.Remove(indexName)
	// remove obj storage
	for _, obj := range objs {
		err = o.Remove(context.Background(), obj.Key)
		if err != nil {
			log.Error().Err(err).Msgf("Remove index %s err: remove object err: ", indexName)
			return fmt.Errorf("failed to remove object: %w", err)
		}
	}
	return nil
}

// OssExists
// used for test index delete
func OssExists(indexName string) (bool, error) {
	o, err := createOssClient()
	if err != nil {
		return false, err
	}
	objs, err := o.List(context.Background(), indexName)
	if err != nil {
		return false, err
	}
	return len(objs) > 0, nil
}

func (b *OssBackend) Setup(readOnly bool) error {
	if readOnly {
		// read only, check if there are any objects here.
		keys, err := objectKeys.List(context.Background(), b.Client, b.prefix)
		if err != nil {
			log.Error().Err(err).Msgf("Setup index %s err: ", b.prefix)
			return fmt.Errorf("setup backend err: %w", err)
		}
		// there are not any objects, that's a brand new index,
		// so it cannot be opened with readOnly = true
		if len(keys) == 0 {
			return ErrPathNotExists
		}
	}
	// make sure local cache is ready for this.
	return b.cache.Setup(b.path)
}

func (b *OssBackend) List(kind string) ([]uint64, error) {
	keys, err := objectKeys.List(context.Background(), b.Client, b.prefix)
	if err != nil {
		log.Error().Err(err).Msgf("List index %s err: ", b.prefix)
		return nil, err
	}
	var itemList uint64Slice

	for _, key := range keys {
		if filepath.Ext(key) != kind {
			continue
		}
		stringID := filepath.Base(key)
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

func (b *OssBackend) Load(kind string, id uint64) (*segment.Data, io.Closer, error) {
	key := fileName(kind, id)
	if cf, ok := b.cache.GetCacheFile(path.Join(b.path, key)); ok {
		return cf.LoadReadOnlyData()
	}

	reader, err := b.Client.Read(context.Background(), path.Join(b.prefix, key))
	if err != nil {
		log.Error().Err(err).Msgf("Load index %s file err: Read file from oss err: ", b.prefix)
		return nil, nil, fmt.Errorf("load file err: read file from oss err: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()

	f, err := b.cache.CacheFile(path.Join(b.path, key), reader)
	if err != nil {
		return nil, nil, fmt.Errorf("load file err: %w", err)
	}
	return f.LoadReadOnlyData()
}

func (b *OssBackend) Persist(kind string, id uint64, w index.WriterTo, closeCh chan struct{}) error {
	key := fileName(kind, id)
	fipath := path.Join(b.path, key)
	backendKey := path.Join(b.prefix, key)

	cacheF, err := b.cache.OpenWriter(fipath)
	if err != nil {
		return fmt.Errorf("open cache writer error: %w", err)
	}
	defer cacheF.Close()
	_, err = w.WriteTo(cacheF, closeCh)
	if err != nil {
		_ = b.cache.Remove(fipath)
		return err
	}
	stat, _ := cacheF.Stat()
	// The return value of WriteTo is not accurate.
	// So we need to get the real bytes size.
	n := stat.Size()
	cacheF.Seek(0, io.SeekStart)

	err = b.Client.Write(context.Background(), backendKey, cacheF, &objclient.WriteOptions{Size: n})
	if err != nil {
		log.Error().Err(err).Msgf("Persist index %s error: persist to obj store err: ", b.prefix)
		return err
	}
	objectKeys.Insert(backendKey)
	return nil
}

func (b *OssBackend) Remove(kind string, id uint64) error {
	key := fileName(kind, id)
	err := b.cache.Remove(path.Join(b.path, key))
	if err != nil {
		return err
	}

	objectKeys.Remove(path.Join(b.prefix, key))

	err = b.Client.Remove(context.Background(), path.Join(b.prefix, key))
	if err != nil {
		log.Error().Err(err).Msgf("Remove index file %s from obj store err: ", path.Join(b.prefix, key))
		return fmt.Errorf("remove object err: %w", err)
	}
	return nil
}

func (b *OssBackend) Stats() (numItems uint64, numBytes uint64) {
	objs, err := b.Client.List(context.Background(), b.prefix)
	if err != nil {
		log.Error().Err(err).Msgf("Stats index %s err: ", b.prefix)
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
	return nil
}

func (b *OssBackend) Unlock() error {
	return nil
}
