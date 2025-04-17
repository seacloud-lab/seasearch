package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	nodeMap = &NodeMap{
		mp: make(map[int]string),
	}
	assignMap = &AssignMap{
		mp: make(map[string]int),
	}

}

func TestQueryNodes(t *testing.T) {
	nodes := []nodeInfo{{id: 0}, {id: 1}, {id: 2}}
	assignMap.mp["0c"] = 1

	conf.General.ParallelQueryNodeLimit = 2
	list, err := getQueryNodes(nodes, "test_index")
	assert.NoError(t, err)

	// first node is 1
	assert.Equal(t, 1, list.Next().id)
	assert.Equal(t, 2, list.Next().id)
	assert.Equal(t, 1, list.Next().id)
	assert.Equal(t, 2, list.Next().id)
}

func TestQueryNodes2(t *testing.T) {
	nodes := []nodeInfo{{id: 0}, {id: 1}, {id: 2}, {id: 3}, {id: 4}}
	assignMap.mp["0c"] = 2

	conf.General.ParallelQueryNodeLimit = 6
	list, err := getQueryNodes(nodes, "test_index")
	assert.NoError(t, err)

	// first node is 2
	assert.Equal(t, 2, list.Next().id)
	assert.Equal(t, 3, list.Next().id)
	assert.Equal(t, 4, list.Next().id)
	assert.Equal(t, 0, list.Next().id)
	assert.Equal(t, 1, list.Next().id)
	assert.Equal(t, 2, list.Next().id)
}

func TestQueryNodes3(t *testing.T) {
	nodes := []nodeInfo{{id: 0}, {id: 1}, {id: 2}, {id: 3}, {id: 4}}
	assignMap.mp["0c"] = 2

	conf.General.ParallelQueryNodeLimit = 3
	list, err := getQueryNodes(nodes, "test_index")
	assert.NoError(t, err)

	// first node is 2
	assert.Equal(t, 2, list.Next().id)
	assert.Equal(t, 3, list.Next().id)
	assert.Equal(t, 4, list.Next().id)
	assert.Equal(t, 2, list.Next().id)
	assert.Equal(t, 3, list.Next().id)
	assert.Equal(t, 4, list.Next().id)
}

func BenchmarkGenQueryNodes(b *testing.B) {
	nodes := []nodeInfo{{id: 0}, {id: 1}, {id: 2}, {id: 3}, {id: 4}}
	conf.General.ParallelQueryNodeLimit = 3
	for range b.N {
		getQueryNodes(nodes, "test_index")
	}
}
