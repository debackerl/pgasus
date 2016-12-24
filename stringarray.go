// Copyright (c) 2016 Laurent Debacker
// Copyright (c) 2013 Jack Christensen
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package main

import (
	"github.com/jackc/pgx"
)

type StringArray struct {
	ElementType pgx.Oid
	Values []string
}

func (a StringArray) Encode(w *pgx.WriteBuf, oid pgx.Oid) error {
	var totalStringSize int
	for _, v := range a.Values {
		totalStringSize += len(v)
	}

	size := 20 + len(a.Values)*4 + totalStringSize
	w.WriteInt32(int32(size))

	w.WriteInt32(1)                    // number of dimensions
	w.WriteInt32(0)                    // no nulls
	w.WriteInt32(int32(a.ElementType)) // type of elements
	w.WriteInt32(int32(len(a.Values))) // number of elements
	w.WriteInt32(1)                    // index of first element

	for _, v := range a.Values {
		w.WriteInt32(int32(len(v)))
		w.WriteBytes([]byte(v))
	}

	return nil
}

func (a StringArray) FormatCode() int16 {
	return pgx.BinaryFormatCode
}
