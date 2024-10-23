package zutils

import (
	"context"
	"io"
	"time"

	"sync/atomic"
)

// TimeoutReader will call the cancel function if Read() was blocked for about
// 30 seconds.
type TimeoutReader struct {
	r      io.ReadCloser
	cancel context.CancelFunc
	readed atomic.Int64
	closed atomic.Bool
}

// newTimeoutReader returns a new timeout reader.
// Caller should close the reader after using.
func NewTimeoutReader(r io.ReadCloser, cancel context.CancelFunc) *TimeoutReader {
	reader := new(TimeoutReader)
	reader.r = r
	reader.cancel = cancel
	go reader.timer()
	return reader
}

func (reader *TimeoutReader) Read(data []byte) (int, error) {
	n, err := reader.r.Read(data)
	reader.readed.Add(int64(n))
	return n, err
}

func (reader *TimeoutReader) Close() error {
	reader.closed.Store(true)
	return reader.r.Close()
}

func (reader *TimeoutReader) timer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		if reader.closed.Load() {
			return
		}

		readed := reader.readed.Swap(0)
		if readed == 0 {
			reader.cancel()
			return
		}
	}
}
