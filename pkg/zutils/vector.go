package zutils

import "math"

// HourVector accepts an hour (1 to 12) and returns its 2D unit vector representation.
func HourVector(hour int) []float32 {
	// 12 hours equal a full circle (2 * Pi radians)
	// Angle moves clockwise from the top
	angle := float64(hour) * (2 * math.Pi / 12)

	x := math.Sin(angle)
	y := math.Cos(angle)

	return []float32{float32(x), float32(y)}
}

// MinuteVector accepts a minute (0 to 59) and returns its 2D unit vector representation.
func MinuteVector(minute int) []float32 {
	// 60 minutes equal a full circle (2 * Pi radians)
	angle := float64(minute) * (2 * math.Pi / 60)

	x := math.Sin(angle)
	y := math.Cos(angle)

	return []float32{float32(x), float32(y)}
}
