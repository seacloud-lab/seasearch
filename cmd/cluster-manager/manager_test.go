package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewNodeDistribute(t *testing.T) {
	// 3 is the new node
	curNodes := []int{1, 2, 3}
	curAssignMap := map[string]int{
		"a1": 1,
		"a2": 1,
		"a3": 1,
		"a4": 1,
		"a5": 2,
		"a6": 2,
		"a7": 2,
	}

	result := distribute(curNodes, curAssignMap)
	assert.Equal(t, 249, len(result))

	count3 := 0
	count2 := 3
	count1 := 4
	for _, to := range result {
		if to == 1 {
			count1++
		} else if to == 2 {
			count2++
		} else if to == 3 {
			count3++
		} else {
			t.FailNow()
		}
	}
	assert.Equal(t, 85, count3)
	assert.Equal(t, 85, count2)
	assert.Equal(t, 86, count1)
}

func TestRemoveNodeDistribute(t *testing.T) {
	curNodes := []int{1, 2}
	curAssignMap := map[string]int{
		"a1": 1,
		"a2": 1,
		"a3": 1,
		"a4": 2,
		"a5": 2,
		"a6": 3,
		"a7": 3,
	}

	result := distribute(curNodes, curAssignMap)
	assert.Equal(t, 251, len(result))

	count2 := 2
	count1 := 3
	for _, to := range result {
		if to == 1 {
			count1++
		} else if to == 2 {
			count2++
		} else {
			t.FailNow()
		}
	}

	assert.Equal(t, 128, count1)
	assert.Equal(t, 128, count2)
}

func TestChangeNodeDistribute(t *testing.T) {
	// 3 is removed, 4 is new node
	curNodes := []int{1, 2, 4}
	curAssignMap := map[string]int{
		"a1": 1,
		"a2": 1,
		"a3": 1,
		"a4": 2,
		"a5": 2,
		"a6": 3,
		"a7": 3,
	}

	result := distribute(curNodes, curAssignMap)
	assert.Equal(t, 251, len(result))

	count4 := 0
	count2 := 2
	count1 := 3

	for _, to := range result {
		if to == 1 {
			count1++
		} else if to == 2 {
			count2++
		} else if to == 4 {
			count4++
		} else {
			t.FailNow()
		}
	}
	assert.Equal(t, 85, count4)
	assert.Equal(t, 85, count2)
	assert.Equal(t, 86, count1)
}

func TestTwoNodeJoinDistribute(t *testing.T) {
	curNodes := []int{1, 2}
	curAssignMap := map[string]int{
		"a1": 1,
		"a2": 1,
		"a3": 1,
		"a4": 1,
		"a5": 1,
		"a6": 1,
		"a7": 1,
	}

	result := distribute(curNodes, curAssignMap)

	assert.Equal(t, 249, len(result))

	count2 := 0
	count1 := 7
	for _, to := range result {
		if to == 1 {
			count1++
		} else if to == 2 {
			count2++
		} else {
			t.FailNow()
		}
	}

	assert.Equal(t, 128, count1)
	assert.Equal(t, 128, count2)
}

func TestTwoNodeRetireDistribute(t *testing.T) {
	curNodes := []int{2}
	curAssignMap := map[string]int{
		"a1": 1,
		"a2": 1,
		"a3": 1,
		"a4": 1,
		"a5": 2,
		"a6": 2,
		"a7": 2,
	}

	result := distribute(curNodes, curAssignMap)
	assert.Equal(t, 253, len(result))

	for _, to := range result {
		assert.Equal(t, 2, to)
	}
}

func TestSingleNodeInit(t *testing.T) {

	curNodes := []int{1}
	curAssignMap := map[string]int{}

	result := distribute(curNodes, curAssignMap)
	assert.Equal(t, 256, len(result))

	for _, to := range result {
		assert.Equal(t, 1, to)
	}
}

func TestTwoNodesInit(t *testing.T) {

	curNodes := []int{1, 2}
	curAssignMap := map[string]int{}

	result := distribute(curNodes, curAssignMap)
	assert.Equal(t, 256, len(result))

	count1 := 0
	count2 := 0
	for _, to := range result {
		if to == 1 {
			count1++
		} else if to == 2 {
			count2++
		} else {
			t.FailNow()
		}
	}
	assert.Equal(t, 128, count1)
	assert.Equal(t, 128, count2)
}

func TestStability(t *testing.T) {
	curNodes := []int{1, 2, 3}
	curAssignMap := map[string]int{
		"a1": 1,
		"a2": 2,
		"a3": 3,
		"a4": 1,
		"a5": 2,
		"a6": 3,
		"a7": 1,
	}

	result := distribute(curNodes, curAssignMap)
	assert.Equal(t, 249, len(result))

	for partition := range result {
		_, ok := curAssignMap[partition]
		assert.False(t, ok)
	}
}
