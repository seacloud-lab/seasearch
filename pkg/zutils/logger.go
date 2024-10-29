package zutils

import (
	"io"
)

type LogOuter struct {
	LogToStdout bool
	Out         io.Writer
}

const componentName = "[seasearch] "

func (l *LogOuter) Write(data []byte) (n int, err error) {
	if l.LogToStdout {
		buf := make([]byte, 0, len(componentName)+len(data))
		buf = append(buf, []byte(componentName)...)
		_, err := l.Out.Write(append(buf, data...))
		return len(data), err
	}
	return l.Out.Write(data)
}
