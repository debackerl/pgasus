package main

import (
	"errors"
	"io"
)

type CappedWriter struct {
	wr io.Writer
	written int64
	maxSize int64
}

func NewCappedWriter(w io.Writer, maxSize int64) *CappedWriter {
	return &CappedWriter{
		wr: w,
		maxSize: maxSize,
	}
}

func (w *CappedWriter) Write(p []byte) (n int, err error) {
	if w.written + int64(len(p)) > w.maxSize {
		return 0, errors.New("Size of CappedWriter exceeded.")
	}
	
	n, err = w.wr.Write(p)
	w.written += int64(n)
	
	return
}
