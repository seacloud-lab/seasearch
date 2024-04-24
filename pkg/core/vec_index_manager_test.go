package core

import (
	"testing"
	"time"

	"github.com/DataIntelligenceCrew/go-faiss"
	"github.com/dgraph-io/ristretto/z"
	"github.com/stretchr/testify/assert"
)

func TestGC(t *testing.T) {
	tmpDir := t.TempDir()
	vecIdxManager = &VecIndexManager{
		cache:         make(map[string]VectorIndex),
		ready:         make(map[string]chan struct{}),
		rebuildTaskMp: make(map[string]struct{}),
		rebuildTaskCh: make(chan *rebuildTask, 10),
		storage:       nil,
		closer:        z.NewCloser(3),
		tmpDir:        tmpDir,
	}
	indexA, _ := faiss.IndexFactory(4, "IDMap,Flat", faiss.MetricL2)
	vecIdxManager.cache["A"] = &IvfPqIndex{
		baseIndex: baseIndex{
			name:  "A",
			aTime: time.Now().Add(-time.Hour * 2),
		},
		index: indexA,
	}
	indexB, _ := faiss.IndexFactory(4, "IDMap,Flat", faiss.MetricL2)
	B := &IvfPqIndex{
		baseIndex: baseIndex{
			name:  "B",
			aTime: time.Now(),
		},
		index: indexB,
	}
	vecIdxManager.cache["B"] = B
	found, err := execGC()
	assert.Nil(t, err)
	assert.True(t, found)

	_, AExists := vecIdxManager.cache["A"]
	assert.False(t, AExists)
	_, BExists := vecIdxManager.cache["B"]
	assert.True(t, BExists)

	B.aTime = time.Now().Add(-time.Hour * 2)
	found, err = execGC()
	assert.Nil(t, err)
	assert.True(t, found)

	_, BExists = vecIdxManager.cache["B"]
	assert.False(t, BExists)
}
