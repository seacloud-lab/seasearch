package text

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/zutils"

	"github.com/blugelabs/bluge"
	"github.com/rs/zerolog/log"
)

var (
	ErrReadOnly = errors.New("text index is read-only")
	ErrClosed   = errors.New("text index is closed")
)

// IndexShard owns first-shard runtime state. Layout and statistics remain in
// store.Index metadata and are supplied by the caller when needed.
type IndexShard struct {
	id      string
	name    string
	options Options

	// shards mirrors metadata shard IDs to locate cached runtime resources.
	// Structural changes must update metadata and this collection together.
	shards []*IndexSecondShard
	lock   sync.RWMutex
	closed bool
}

// IndexSecondShard owns a cached Writer and retains only its immutable shard ID.
type IndexSecondShard struct {
	id     int64
	writer *bluge.Writer
	lock   sync.Mutex
}

func (index *Index) GetShardByDocID(docID string) *IndexShard {
	shardKey := index.shardHashing.Lookup(docID)
	return index.shards[shardKey]
}

func (index *Index) GetShardByShardKey(shardKey string) *IndexShard {
	return index.shards[shardKey]
}

func (s *IndexShard) GetLatestShardID() int64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return int64(len(s.shards)) - 1
}

func (s *IndexShard) GetID() string {
	return s.id
}

// GetWriter returns a borrowed cached Writer. The caller must hold the owning
// store Index reference throughout its use, and must not close the Writer.
func (s *IndexShard) GetWriter(shardID ...int64) (*bluge.Writer, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.closed {
		return nil, ErrClosed
	}
	if s.options.ReadOnly {
		return nil, ErrReadOnly
	}
	if len(shardID) > 1 {
		return nil, fmt.Errorf("expected at most one second shard ID")
	}
	id := int64(len(s.shards)) - 1
	if len(shardID) == 1 {
		id = shardID[0]
	}
	if id < 0 || id >= int64(len(s.shards)) {
		return nil, fmt.Errorf("second shard %s/%d not found", s.name, id)
	}
	second := s.shards[id]
	second.lock.Lock()
	defer second.lock.Unlock()

	if second.writer == nil {
		cfg, err := s.openConfig(id)
		if err != nil {
			return nil, err
		}
		writer, err := bluge.OpenWriter(cfg)
		if err != nil {
			return nil, fmt.Errorf("open Writer for %s/%d: %w", s.name, id, err)
		}
		second.writer = writer
	}
	return second.writer, nil
}

func (s *IndexShard) openConfig(shardID int64) (bluge.Config, error) {
	var cfg bluge.Config
	if config.Global.StorageType != s.options.StorageType {
		return cfg, fmt.Errorf("the storage backend of the configuration is %s, but try to get %s", config.Global.StorageType, s.options.StorageType)
	}
	name := fmt.Sprintf("%s/%s/%06x", s.options.StoreName, s.id, shardID)
	switch s.options.StorageType {
	case "disk":
		cfg = directory.GetDiskConfig(config.Global.DataPath, name, 0, 0)
	case "s3":
		cfg = directory.GetS3Config(config.Global.DataPath, name, 0, 0)
	case "oss":
		cfg = directory.GetOssConfig(config.Global.DataPath, name, 0, 0)
	default:
		return cfg, fmt.Errorf("invalid storage type: %s", s.options.StorageType)
	}
	if s.options.DefaultSearchAnalyzer != nil {
		cfg.DefaultSearchAnalyzer = s.options.DefaultSearchAnalyzer
	}
	return cfg, nil
}

func (s *IndexShard) openReader(ctx context.Context, shardID int64) (*bluge.Reader, error) {
	err := ctx.Err()
	if err != nil {
		return nil, err
	}
	if !s.options.ReadOnly {
		writer, err := s.GetWriter(shardID)
		if err != nil {
			return nil, err
		}
		return writer.Reader()
	}
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.closed {
		return nil, ErrClosed
	}
	if shardID < 0 || shardID >= int64(len(s.shards)) {
		return nil, fmt.Errorf("second shard %s/%d not found", s.name, shardID)
	}
	cfg, err := s.openConfig(shardID)
	if err != nil {
		return nil, err
	}
	for attempt := range 3 {
		err = ctx.Err()
		if err != nil {
			return nil, err
		}
		reader, openErr := bluge.OpenReader(cfg)
		if openErr == nil {
			return reader, nil
		}
		if errors.Is(openErr, directory.ErrPathNotExists) {
			return nil, nil // New, empty shard.
		}
		err = openErr
		if !strings.Contains(err.Error(), "unable to find a usable snapshot") || attempt == 2 {
			break
		}
		log.Warn().Err(err).Str("shard", s.name).Int64("second shard", shardID).Msg("retrying text Reader open")
		timer := time.NewTimer(time.Duration(zutils.RandInt(100, 300)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("open Reader for %s/%d: %w", s.name, shardID, err)
}

// Close requires the caller to have drained all uses of this shard first.
func (s *IndexShard) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.closed = true
	for _, second := range s.shards {
		second.lock.Lock()
		if second.writer != nil {
			err := second.writer.Close()
			if err != nil {
				log.Warn().Err(err).Str("index", s.name).Int64("shard", second.id).Msg("failed to close text Writer")
			}
			second.writer = nil
		}
		second.lock.Unlock()
	}
}
