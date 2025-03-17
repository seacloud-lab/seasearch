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
	nodes := []nodeInfo{{Id: 0}, {Id: 1}, {Id: 2}}
	assignMap.mp["0c"] = 1

	list, err := getQueryNodes(nodes, 2, "test_index")
	assert.NoError(t, err)

	// first node is 1
	assert.Equal(t, 1, list.Next().Id)
	assert.Equal(t, 2, list.Next().Id)
	assert.Equal(t, 1, list.Next().Id)
	assert.Equal(t, 2, list.Next().Id)
}

func TestQueryNodes2(t *testing.T) {
	nodes := []nodeInfo{{Id: 0}, {Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}
	assignMap.mp["0c"] = 2

	list, err := getQueryNodes(nodes, 6, "test_index")
	assert.NoError(t, err)

	// first node is 2
	assert.Equal(t, 2, list.Next().Id)
	assert.Equal(t, 3, list.Next().Id)
	assert.Equal(t, 4, list.Next().Id)
	assert.Equal(t, 0, list.Next().Id)
	assert.Equal(t, 1, list.Next().Id)
	assert.Equal(t, 2, list.Next().Id)
}

func TestQueryNodes3(t *testing.T) {
	nodes := []nodeInfo{{Id: 0}, {Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}
	assignMap.mp["0c"] = 2

	list, err := getQueryNodes(nodes, 3, "test_index")
	assert.NoError(t, err)

	// first node is 2
	assert.Equal(t, 2, list.Next().Id)
	assert.Equal(t, 3, list.Next().Id)
	assert.Equal(t, 4, list.Next().Id)
	assert.Equal(t, 2, list.Next().Id)
	assert.Equal(t, 3, list.Next().Id)
	assert.Equal(t, 4, list.Next().Id)
}

func BenchmarkGenQueryNodes(b *testing.B) {
	nodes := []nodeInfo{{Id: 0}, {Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}}
	for range b.N {
		getQueryNodes(nodes, 3, "test_index")
	}
}
