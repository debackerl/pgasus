// Copyright 2010 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"
)

type JsonRecordSetWriter struct {
	bytes.Buffer
	MaxResponseSizeBytes int64
	stack []StateFunction
}

func NewJsonRecordSetWriter(maxResponseSizeBytes int64) *JsonRecordSetWriter {
	return &JsonRecordSetWriter {
		MaxResponseSizeBytes: maxResponseSizeBytes,
		stack: make([]StateFunction, 0, 4),
	}
}

func (w *JsonRecordSetWriter) ToBytes() []byte {
	return w.Buffer.Bytes()
}

func (w *JsonRecordSetWriter) HttpRespond(hw http.ResponseWriter) {
	hw.Header().Set("Content-Type", "application/json; charset=utf-8")
	hw.WriteHeader(http.StatusOK)
	hw.Write(w.ToBytes())
}

func (w *JsonRecordSetWriter) BeginBatch() error {
	return w.BeginArray(nil, -1)
}

func (w *JsonRecordSetWriter) EndBatch() error {
	return w.EndArray(nil)
}

func (w *JsonRecordSetWriter) BeginRecordSet(rs *RecordSet) error {
	return w.BeginArray(rs, -1)
}

func (w *JsonRecordSetWriter) EndRecordSet(rs *RecordSet) error {
	return w.EndArray(rs)
}

func (w *JsonRecordSetWriter) BeginRecord(rs *RecordSet) error {
	return w.BeginObject(rs)
}

func (w *JsonRecordSetWriter) EndRecord(rs *RecordSet) error {
	return w.EndObject(rs)
}

func (w *JsonRecordSetWriter) BeginColumn(rs *RecordSet) error {
	_, field := rs.CurrentColumn()
	return w.String(rs, field.Name)
}

func (w *JsonRecordSetWriter) EndColumn(rs *RecordSet) error {
	return nil
}

func (w *JsonRecordSetWriter) BeginArray(rs *RecordSet, size int) error {
	w.prepare()
	
	w.push(arrayInitState)
	w.WriteByte('[')
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) EndArray(rs *RecordSet) error {
	w.pop()
	w.WriteByte(']')
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) BeginObject(rs *RecordSet) error {
	w.prepare()
	
	w.push(objectInitState)
	w.WriteByte('{')
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) EndObject(rs *RecordSet) error {
	w.pop()
	w.WriteByte('}')
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) Null(rs *RecordSet) error {
	w.prepare()
	
	w.WriteString("null")
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) Bool(rs *RecordSet, v bool) error {
	w.prepare()
	
	if v {
		w.WriteString("true")
	} else {
		w.WriteString("false")
	}
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) Integer(rs *RecordSet, v int64) error {
	w.prepare()
	
	w.WriteString(strconv.FormatInt(v, 10))
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) Float(rs *RecordSet, v float64) error {
	w.prepare()
	
	w.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) Numeric(rs *RecordSet, v string) error {
	w.prepare()
	
	if v == "NaN" {
		w.WriteString(`"NaN"`)
	} else {
		w.WriteString(v)
	}
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) Date(rs *RecordSet, v time.Time) error {
	w.prepare()
	
	w.WriteByte('"')
	w.WriteString(v.Format("2006-01-02"))
	w.WriteByte('"')
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) DateTime(rs *RecordSet, v time.Time) error {
	w.prepare()
	
	w.WriteByte('"')
	w.WriteString(v.Format(time.RFC3339))
	w.WriteByte('"')
	
	return w.checkSize()
}

var hex = "0123456789abcdef"

func (w *JsonRecordSetWriter) String(rs *RecordSet, v string) error {
	w.prepare()
	
	w.WriteByte('"')
	
	start := 0
	n := len(v)
	
	for i := 0; i < n; {
		if b := v[i]; b < utf8.RuneSelf {
			if 0x20 <= b && b != '\\' && b != '"' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			if start < i {
				w.WriteString(v[start:i])
			}
			switch b {
			case '\\', '"':
				w.WriteByte('\\')
				w.WriteByte(b)
			case '\n':
				w.WriteByte('\\')
				w.WriteByte('n')
			case '\r':
				w.WriteByte('\\')
				w.WriteByte('r')
			case '\t':
				w.WriteByte('\\')
				w.WriteByte('t')
			default:
				w.WriteString(`\u00`)
				w.WriteByte(hex[b>>4])
				w.WriteByte(hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(v[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				w.WriteString(v[start:i])
			}
			w.WriteString(`\ufffd`)
			i += size
			start = i
			continue
		}
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				w.WriteString(v[start:i])
			}
			w.WriteString(`\u202`)
			w.WriteByte(hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < n {
		w.WriteString(v[start:])
	}
	w.WriteByte('"')
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) Bytes(rs *RecordSet, v []byte) error {
	w.prepare()
	
	w.WriteByte('"')
	if len(v) < 1024 {
		enc := base64.StdEncoding
		buf := make([]byte, enc.EncodedLen(len(v)))
		enc.Encode(buf, v)
		w.Write(buf)
	} else {
		enc := base64.NewEncoder(base64.StdEncoding, w)
		enc.Write(v)
		enc.Close()
	}
	w.WriteByte('"')
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) Json(rs *RecordSet, v json.RawMessage) error {
	w.prepare()
	
	w.Write(v)
	
	return w.checkSize()
}

func (w *JsonRecordSetWriter) checkSize() error {
	if int64(w.Len()) > w.MaxResponseSizeBytes {
		return errors.New("Response too long.")
	}
	return nil
}

func (w *JsonRecordSetWriter) push(state StateFunction) {
	w.stack = append(w.stack, state)
}

func (w *JsonRecordSetWriter) pop() StateFunction {
	i := len(w.stack) - 1
	if i == -1 {
		return nil
	}
	state := w.stack[i]
	w.stack = w.stack[0:i]
	return state
}

func (w *JsonRecordSetWriter) prepare() {
	s := w.pop()
	if s != nil {
		w.push(s(&w.Buffer))
	}
}

type StateFunction func(w *bytes.Buffer) StateFunction

func arrayInitState(w *bytes.Buffer) StateFunction {
	return arrayCruisingState
}

func arrayCruisingState(w *bytes.Buffer) StateFunction {
	w.WriteByte(',')
	return arrayCruisingState
}

func objectInitState(w *bytes.Buffer) StateFunction {
	return objectValueState
}

func objectKeyState(w *bytes.Buffer) StateFunction {
	w.WriteByte(',')
	return objectValueState
}

func objectValueState(w *bytes.Buffer) StateFunction {
	w.WriteByte(':')
	return objectKeyState
}
