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
	manager = &VecIndexManager{
		cache:         make(map[string]*VecIndex),
		ready:         make(map[string]chan struct{}),
		rebuildTaskMp: make(map[string]struct{}),
		rebuildTaskCh: make(chan *rebuildTask, 10),
		storage:       nil,
		closer:        z.NewCloser(3),
		tmpDir:        tmpDir,
	}
	indexA, _ := faiss.IndexFactory(4, "IDMap,Flat", faiss.MetricL2)
	manager.cache["A"] = &VecIndex{
		name:  "A",
		atime: time.Now().Add(-time.Hour * 2),
		index: indexA,
	}
	indexB, _ := faiss.IndexFactory(4, "IDMap,Flat", faiss.MetricL2)
	B := &VecIndex{
		name:  "B",
		atime: time.Now(),
		index: indexB,
	}
	manager.cache["B"] = B
	found, err := execGC()
	assert.Nil(t, err)
	assert.True(t, found)

	_, AExists := manager.cache["A"]
	assert.False(t, AExists)
	_, BExists := manager.cache["B"]
	assert.True(t, BExists)

	B.atime = time.Now().Add(-time.Hour * 2)
	found, err = execGC()
	assert.Nil(t, err)
	assert.True(t, found)

	_, BExists = manager.cache["B"]
	assert.False(t, BExists)
}
