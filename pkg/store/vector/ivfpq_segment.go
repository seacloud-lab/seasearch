package vector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/zincsearch/zincsearch/faiss_wrapper"
	"github.com/zincsearch/zincsearch/pkg/bluge/directory"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/uquery/query"
	"github.com/zincsearch/zincsearch/pkg/vector"
	"github.com/zincsearch/zincsearch/pkg/zutils"
	"github.com/zincsearch/zincsearch/pkg/zutils/base62"

	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/blugelabs/bluge"
	"github.com/rs/zerolog/log"
)

// we store original vectors with a fixed field.
const internalVecField = "vec"

type IVFPQSegment struct {
	baseIndex *fieldIndex
	id        int
	index     faiss.Index
	// cache for search
	cachedVectorsCache []float32
	// vector idx -> docId
	ids []int64

	// for growing segment, the lock is used for synchronizing cache.
	// for sealed segment, the lock is used for avoiding concurrent read and write for faiss index.
	sync.RWMutex

	vecStoreWriter *bluge.Writer
	writerLock     sync.RWMutex
}

// segmentCall is request-owned. Only its embedded runtime may be cached; ref
// and the call itself never escape the synchronized operation that borrowed it.
type segmentCall struct {
	*IVFPQSegment
	ref *meta.VectorSegment
	ctx context.Context
}

func openIVFPQSegment(vecIndex *fieldIndex, ref *meta.VectorSegment) (*IVFPQSegment, error) {
	seg := &IVFPQSegment{baseIndex: vecIndex, id: ref.Id}
	// Segment IDs/configuration are values; the metadata-bearing call is local.
	call := &segmentCall{IVFPQSegment: seg, ref: ref, ctx: context.Background()}
	// load faiss index
	if ref.Status == vector.StatusSealed {
		err := call.loadFaissIndexFile()
		if err != nil {
			return nil, errors.Join(fmt.Errorf("open seg faiss index err: %w", err), seg.Close())
		}
	}
	return seg, nil
}

func (s *segmentCall) openVecStoreReader() (*bluge.Reader, error) {
	s.writerLock.RLock()
	if s.vecStoreWriter != nil {
		reader, err := s.vecStoreWriter.Reader()
		s.writerLock.RUnlock()
		return reader, err
	}
	s.writerLock.RUnlock()
	dataPath := config.Global.DataPath
	name := path.Join(vector.VecPrefix, s.baseIndex.StoreName(), fmt.Sprintf("%04x", s.ref.Id), "stored_vec")
	var cfg bluge.Config
	switch config.Global.StorageType {
	case "disk":
		cfg = directory.GetDiskConfig(dataPath, name)
	case "s3":
		cfg = directory.GetS3Config(dataPath, name)
	case "oss":
		cfg = directory.GetOssConfig(dataPath, name)
	default:
		return nil, fmt.Errorf("invalid storage type: %s", config.Global.StorageType)
	}

	var err error
	var r *bluge.Reader
	for i := 0; i < 3; i++ {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		r, err = bluge.OpenReader(cfg)
		if err != nil {
			// it's a new empty index, without any document
			if errors.Is(err, directory.ErrPathNotExists) && s.ref.Status == vector.StatusGrowing && s.ref.Count == 0 {
				return nil, nil
			}
			// When opening the reader, the writer may be writing and generating new snapshots concurrently.
			// The snapshots listed by the reader may have expired and been cleaned up at this time.
			// In this case, we should try to get the latest snapshot again.
			if strings.Contains(err.Error(), "unable to find a usable snapshot") {
				log.Warn().Msgf("failed to open reader for %s: %v, retrying", s.getName(), err)
				timer := time.NewTimer(time.Duration(zutils.RandInt(100, 300)) * time.Millisecond)
				select {
				case <-s.ctx.Done():
					timer.Stop()
					return nil, s.ctx.Err()
				case <-timer.C:
				}
				continue
			}
			return nil, fmt.Errorf("open %s err: %w", s.getName(), err)
		}
		return r, nil
	}
	return r, err
}

func (s *segmentCall) openVecStoreWriter() (*bluge.Writer, error) {
	s.writerLock.Lock()
	defer s.writerLock.Unlock()
	if s.vecStoreWriter != nil {
		return s.vecStoreWriter, nil
	}
	dataPath := config.Global.DataPath
	name := path.Join(vector.VecPrefix, s.baseIndex.StoreName(), fmt.Sprintf("%04x", s.ref.Id), "stored_vec")
	var cfg bluge.Config
	switch config.Global.StorageType {
	case "disk":
		cfg = directory.GetDiskConfig(dataPath, name)
	case "s3":
		cfg = directory.GetS3Config(dataPath, name)
	case "oss":
		cfg = directory.GetOssConfig(dataPath, name)
	default:
		return nil, fmt.Errorf("invalid storage type: %s", config.Global.StorageType)
	}

	writer, err := bluge.OpenWriter(cfg)
	if err != nil {
		return nil, err
	}
	s.vecStoreWriter = writer
	return writer, nil
}

func (s *segmentCall) getName() string {
	return fmt.Sprintf("%s_%d", s.baseIndex.Name(), s.ref.Id)
}

// saved distinguishes a successful publication from a later cleanup error.
func (s *segmentCall) saveFaissIndex(index faiss.Index) (saved bool, err error) {
	f, err := os.CreateTemp(s.baseIndex.resources.tmpDir, "temp_index")
	if err != nil {
		return false, err
	}
	defer func() {
		err = errors.Join(err, removeMaintenanceFile(f.Name()))
	}()
	if err := f.Close(); err != nil {
		return false, err
	}

	err = faiss.WriteIndex(index, f.Name())
	if err != nil {
		return false, err
	}
	err = s.baseIndex.resources.storage.SaveFile(f.Name(), s.getFaissIndexPath())
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *segmentCall) loadFaissIndexFile() error {
	localFile, closer, err := s.baseIndex.resources.storage.LoadFile(s.getFaissIndexPath())
	if err != nil {
		return err
	}
	defer func() {
		_ = closer.Close()
	}()
	s.index, err = faiss.ReadIndex(localFile, faiss.IOFlagReadOnly)
	return err
}

func (s *segmentCall) getFaissIndexPath() string {
	return path.Join(s.baseIndex.StoreName(), fmt.Sprintf("%04x", s.ref.Id), "faiss")
}

func (s *segmentCall) Batch(batch *segBatch) error {
	s.Lock()
	defer s.Unlock()
	var writeErr error
	if len(batch.deleteIds) > 0 {
		writeErr = s.removeIDs(batch.deleteIds)
	}
	if writeErr == nil && len(batch.addVectors) > 0 {
		writeErr = s.addVectors(batch.addVectors, batch.addIds)
	}
	// Invalidate caches and refresh confirmed statistics even after partial work.
	// Statistics cleanup must finish even when the operation was cancelled.
	cleanup := *s
	cleanup.ctx = context.WithoutCancel(s.ctx)
	return errors.Join(writeErr, cleanup.save())
}

// addVectors
// require Lock.
func (s *segmentCall) addVectors(vectors [][]float32, ids []int64) error {
	// this should never have happened
	if s.ref.Status != vector.StatusGrowing {
		return fmt.Errorf("cannot add vectors to a non-Growing segment")
	}
	batch := bluge.NewBatch()
	for i, vec := range vectors {
		doc := bluge.NewDocument(base62.Encode(ids[i]))
		var bts []byte
		if s.baseIndex.UseFloat16() {
			bts = zutils.VectorToFloat16Bytes(vec)
		} else {
			bts = zutils.VectorToBytes(vec)
		}
		doc.AddField(bluge.NewStoredOnlyField(internalVecField, bts))
		batch.Insert(doc)
	}
	writer, err := s.openVecStoreWriter()
	if err != nil {
		return err
	}

	err = writer.Batch(batch)
	if err != nil {
		return fmt.Errorf("add vectors to bluge err: %w", err)
	}
	return nil
}

// removeIDs
// require Lock.
func (s *segmentCall) removeIDs(ids []int64) error {
	if s.ref.Status == vector.StatusSealed {
		selector, err := faiss.NewIDSelectorBatch(ids)
		if err != nil {
			return err
		}
		defer selector.Delete()
		_, err = s.index.RemoveIDs(selector)
		if err != nil {
			return fmt.Errorf("remove seg faiss index err: %w", err)
		}
	}
	// remove vec_store
	batch := bluge.NewBatch()
	for _, id := range ids {
		batch.Delete(bluge.Identifier(base62.Encode(id)))
	}
	writer, err := s.openVecStoreWriter()
	if err != nil {
		return err
	}
	err = writer.Batch(batch)
	if err != nil {
		return fmt.Errorf("remove seg bluge index err: %w", err)
	}
	return nil
}

// GetExistsIds
// Filter out the id that exists in this segment. It‘s lock free.
func (s *segmentCall) GetExistsIds(ids []int64) (result []int64, err error) {
	r, err := s.openVecStoreReader()
	if err != nil {
		return nil, err
	}
	if r == nil {
		return []int64{}, nil
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	q := bluge.NewBooleanQuery()
	for _, id := range ids {
		sub := bluge.NewTermQuery(base62.Encode(id)).SetField("_id")
		q.AddShould(sub)
	}
	req := bluge.NewAllMatches(q)
	dmi, err := r.Search(s.ctx, req)
	if err != nil {
		return nil, err
	}
	result = make([]int64, 0)
	for {
		next, err := dmi.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
		var id string
		err = next.VisitStoredFields(func(f string, value []byte) bool {
			if f == "_id" {
				id = string(value)
				return false
			}
			return true
		})
		if err != nil {
			return nil, err
		}
		result = append(result, base62.Decode(id))
	}
	return result, nil
}

// save
// require Lock.
func (s *segmentCall) save() (err error) {
	if s.ref.Status != vector.StatusSealed {
		// the vectors have been updated, we clear the cache
		s.freeCache()

		// update count
		r, err := s.openVecStoreReader()
		if err != nil {
			return err
		}
		if r == nil {
			s.ref.Count = 0
			return nil
		}
		defer func() { err = errors.Join(err, r.Close()) }()
		c, err := r.Count()
		if err != nil {
			return err
		}
		s.ref.Count = int64(c)
		return nil
	}
	s.ref.Count = s.index.Ntotal()
	_, err = s.saveFaissIndex(s.index)
	return err
}

func (s *IVFPQSegment) Close() error {
	s.Lock()
	defer s.Unlock()
	if s.index != nil {
		s.index.Delete()
		s.index = nil
	}
	s.writerLock.Lock()
	defer s.writerLock.Unlock()
	var err error
	if s.vecStoreWriter != nil {
		err = s.vecStoreWriter.Close()
		s.vecStoreWriter = nil
		if err != nil {
			err = fmt.Errorf("close stored-vector Writer: %w", err)
		}
	}
	s.ids = nil
	s.cachedVectorsCache = nil
	return err
}

// freeCache
// require Lock
func (s *segmentCall) freeCache() {
	s.ids = nil
	s.cachedVectorsCache = nil
}

// Search
// query vectors
func (s *segmentCall) Search(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	if s.ref.Status == vector.StatusGrowing {
		return s.searchFlat(vec, k)
	}
	return s.searchIvfPq(vec, k, nprobe)
}

func (s *segmentCall) searchFlat(vec []float32, k int64) (map[string]float32, error) {
	s.Lock()
	ids, xb, err := s.getVecCaches()
	s.Unlock()

	if err != nil {
		return nil, err
	}
	if len(xb) == 0 || len(ids) == 0 {
		return make(map[string]float32), nil
	}

	return flatSearch(ids, xb, vec, int(k), s.baseIndex.settings.Dims), nil
}

// getVecCaches
// require Lock
func (s *segmentCall) getVecCaches() ([]int64, []float32, error) {
	var cacheVectors []float32
	var cacheIds []int64

	if s.cachedVectorsCache != nil && s.ids != nil {
		cacheVectors = s.cachedVectorsCache
		cacheIds = s.ids
	} else {
		var err error
		s.cachedVectorsCache, s.ids, err = s.getAllVectors()
		if err != nil {
			// getVectors may return partial data with an iteration/Reader error.
			// Never let a later seal reuse that partial result as a full segment.
			s.freeCache()
			return nil, nil, err
		}
		cacheVectors = s.cachedVectorsCache
		cacheIds = s.ids
	}
	return cacheIds, cacheVectors, nil
}

func flatSearch(baseIds []int64, base []float32, query []float32, k, dim int) map[string]float32 {
	if len(baseIds) == 0 || k <= 0 {
		return map[string]float32{}
	}
	k = min(k, len(baseIds))
	distances, ids := faiss_wrapper.Knn(query, base, 1, len(baseIds), k, dim)
	result := make(map[string]float32)
	for i, id := range ids {
		// it means there isn't enough K vectors.
		if id == -1 {
			break
		}
		result[base62.Encode(baseIds[id])] = distances[i]
	}
	return result
}

func (s *segmentCall) searchIvfPq(vec []float32, k int64, nprobe int) (map[string]float32, error) {
	// nprobe is a native index parameter: concurrent queries must not mutate it.
	s.Lock()
	defer s.Unlock()

	var ids []int64
	var err error
	ps, err := faiss.NewParameterSpace()
	if err != nil {
		return nil, err
	}
	defer ps.Delete()

	if err := ps.SetIndexParameter(s.index, "nprobe", float64(nprobe)); err != nil {
		return nil, err
	}

	var result = make(map[string]float32)
	_, ids, err = s.index.Search(vec, int64(k))
	if err != nil {
		return nil, err
	}

	docIds := make([]string, 0)
	for i := range ids {
		if ids[i] == -1 {
			continue
		}
		docIds = append(docIds, base62.Encode(ids[i]))
	}
	if len(docIds) == 0 {
		return result, nil
	}
	// for ivf_pq, we need to get vector and calculate the real distance
	q, err := query.TermsQuery(map[string]interface{}{
		"_id": docIds,
	}, nil)
	if err != nil {
		return nil, err
	}
	req := bluge.NewAllMatches(q)
	vectors, ids, err := s.getVectors(s.ref.Count, req)
	if err != nil {
		return nil, err
	}
	for i := range ids {
		realVector := vectors[i*s.baseIndex.settings.Dims : (i+1)*s.baseIndex.settings.Dims]
		dis, err := L2Distance(vec, realVector)
		if err != nil {
			return nil, err
		}
		result[base62.Encode(ids[i])] = dis
	}
	return result, nil
}

func L2Distance(slice1, slice2 []float32) (float32, error) {
	if len(slice1) != len(slice2) {
		return 0, fmt.Errorf("invalid vectors")
	}

	var sum float32
	for i := 0; i < len(slice1); i++ {
		diff := slice1[i] - slice2[i]
		sum += diff * diff
	}

	return sum, nil
}

func (s *segmentCall) SearchByIDs(vec []float32, k int64, filter []string) ([]DocDistance, error) {
	zq, err := query.TermsQuery(map[string]any{"_id": filter}, nil)
	if err != nil {
		return nil, err
	}
	req := bluge.NewAllMatches(zq)
	vectors, ids, err := s.getVectors(-1, req)
	if err != nil {
		return nil, err
	}

	dimensions := s.baseIndex.settings.Dims
	docIDs, distances, err := ExactSearch(ids, vectors, vec, dimensions, int(k))
	if err != nil {
		return nil, fmt.Errorf("failed to exact search: %w", err)
	}
	var items []DocDistance
	for i, docID := range docIDs {
		items = append(items, DocDistance{
			DocID:    base62.Encode(docID),
			Distance: distances[i],
		})
	}
	return items, nil
}

// Seal
// build an ivf_pq index, require Lock.
func (s *segmentCall) Seal() error {
	s.Lock()
	defer s.Unlock()
	if err := s.ctx.Err(); err != nil {
		return err
	}

	ids, vectors, err := s.getVecCaches()
	if err != nil {
		return err
	}
	if len(vectors) == 0 || len(ids) == 0 {
		// do nothing
		log.Warn().Msgf("try to seal ivf_pq index %s segment %d but there are no vectors", s.baseIndex.Name(), s.ref.Id)
		return nil
	}

	nlist := int(4 * math.Sqrt(float64(len(ids))))
	idx, err := faiss.IndexFactory(s.baseIndex.settings.Dims, fmt.Sprintf("IVF%d,PQ%dx%d", nlist, s.baseIndex.settings.M, s.baseIndex.settings.NBits), faiss.MetricL2)
	if err != nil {
		return err
	}
	err = idx.Train(vectors)
	if err != nil {
		idx.Delete()
		return err
	}
	if err := s.ctx.Err(); err != nil {
		idx.Delete()
		return err
	}

	err = idx.AddWithIDs(vectors, ids)
	if err != nil {
		idx.Delete()
		return err
	}
	if err := s.ctx.Err(); err != nil {
		idx.Delete()
		return err
	}
	saved, err := s.saveFaissIndex(idx)
	if !saved {
		idx.Delete()
		return err
	}
	old := s.index
	s.index = idx
	if old != nil {
		old.Delete()
	}
	s.ref.Status = vector.StatusSealed
	s.freeCache()
	return err
}

// Recall
// calculate recall for whole segment, require RLock.
func (s *segmentCall) Recall(count int, k int64, nprobe int) (float32, error) {
	if s.ref.Status == vector.StatusGrowing {
		return 1, nil
	}
	// get some really vectors as query vector
	xq, err := s.getQueryVectors(count)
	if err != nil {
		return 0, err
	}

	xb, idb, err := s.getAllVectors()
	if err != nil {
		return 0, err
	}

	correct := 0
	total := 0
	for _, q := range xq {
		if err := s.ctx.Err(); err != nil {
			return 0, err
		}
		results, err := s.searchIvfPq(q, k, nprobe)
		if err != nil {
			return 0, err
		}

		flatRes := flatSearch(idb, xb, q, int(k), s.baseIndex.settings.Dims)
		total += len(flatRes)
		for id := range results {
			if _, ok := flatRes[id]; ok {
				correct++
			}
		}
	}

	if total == 0 {
		return 1, nil
	}
	recall := float32(correct) / float32(total)

	return recall, nil

}

// getQueryVectors
// get some vectors from vec_store for Recall
func (s *segmentCall) getQueryVectors(n int) ([][]float32, error) {
	q := bluge.NewMatchAllQuery()
	req := bluge.NewTopNSearch(n, q)
	vectors, _, err := s.getVectors(int64(n), req)
	if err != nil {
		return nil, err
	}
	res := make([][]float32, 0, n)
	dim := s.baseIndex.settings.Dims
	for i := 0; i < len(vectors)/dim; i++ {
		res = append(res, vectors[i*dim:(i+1)*dim])
	}
	return res, nil
}

// getAllVectors
// get all vectors from vec_store
func (s *segmentCall) getAllVectors() ([]float32, []int64, error) {
	q := bluge.NewMatchAllQuery()
	req := bluge.NewAllMatches(q)
	return s.getVectors(s.ref.Count, req)
}

func (s *segmentCall) getVectors(count int64, searchReq bluge.SearchRequest) (vectors []float32, ids []int64, err error) {
	reader, err := s.openVecStoreReader()
	if err != nil {
		return nil, nil, err
	}
	if reader == nil {
		return []float32{}, []int64{}, nil
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	if count > 0 {
		vectors = make([]float32, 0, int(count)*s.baseIndex.settings.Dims)
		ids = make([]int64, 0, count)
	}

	dmi, err := reader.Search(s.ctx, searchReq)
	if err != nil {
		return nil, nil, err
	}
	for {
		next, err := dmi.Next()
		if err != nil {
			return vectors, ids, err
		}
		if next == nil {
			break
		}
		var id string
		var vec []float32
		err = next.VisitStoredFields(func(f string, value []byte) bool {
			if f == "_id" {
				id = string(value)
				return vec == nil
			}
			if f == internalVecField {
				if s.baseIndex.UseFloat16() {
					vec = zutils.Float16BytesToVector(value)
				} else {
					vec = zutils.BytesToVector(value)
				}
				return id == ""
			}
			return true
		})
		if err != nil {
			return vectors, ids, err
		}
		if len(vec) != s.baseIndex.settings.Dims {
			return vectors, ids, fmt.Errorf("invalid stored vector dimensions: %d", len(vec))
		}
		vectors = append(vectors, vec...)
		ids = append(ids, base62.Decode(id))
	}

	return vectors, ids, nil
}
