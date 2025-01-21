package zutils

import (
	"math/rand"
)

func RandInt(min int, max int) int {
	randomNum := rand.Intn(max-min+1) + min
	return randomNum
}
