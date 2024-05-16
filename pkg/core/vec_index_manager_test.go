package core

import (
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/z"
	"github.com/stretchr/testify/assert"
)

func TestGC(t *testing.T) {
	tmpDir := t.TempDir()
	vecIdxManager = &VecIndexManager{
		cache:        make(map[string]VectorIndex),
		ready:        make(map[string]chan struct{}),
		sealedTaskMp: make(map[string]struct{}),
		sealedCh:     make(chan *rebuildTask, 10),
		storage:      nil,
		closer:       z.NewCloser(3),
		tmpDir:       tmpDir,
	}
	vecIdxManager.cache["A"] = &IvfPqIndex{
		baseIndex: baseIndex{
			name:  "A",
			aTime: time.Now().Add(-time.Hour * 2),
		},
	}
	B := &IvfPqIndex{
		baseIndex: baseIndex{
			name:  "B",
			aTime: time.Now(),
		},
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
