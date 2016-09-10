package main


import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

type BinRecordSetWriter struct {
	bytes.Buffer
	MaxResponseSizeBytes int64
	ContentType string
}

func (w *BinRecordSetWriter) ToBytes() []byte {
	return w.Buffer.Bytes()
}

func (w *BinRecordSetWriter) HttpRespond(hw http.ResponseWriter) {
	hw.Header().Set("Content-Type", w.ContentType)
	hw.WriteHeader(http.StatusOK)
	hw.Write(w.ToBytes())
}

func (w *BinRecordSetWriter) BeginBatch() error {
	return errors.New("Batch mode not supported by binary format.")
}

func (w *BinRecordSetWriter) EndBatch() error {
	return nil
}

func (w *BinRecordSetWriter) BeginRecordSet(rs *RecordSet) error {
	return errors.New("Record sets not supported by binary format.")
}

func (w *BinRecordSetWriter) EndRecordSet(rs *RecordSet) error {
	return nil
}

func (w *BinRecordSetWriter) BeginRecord(rs *RecordSet) error {
	return errors.New("Record sets not supported by binary format.")
}

func (w *BinRecordSetWriter) EndRecord(rs *RecordSet) error {
	return nil
}

func (w *BinRecordSetWriter) BeginColumn(rs *RecordSet) error {
	return errors.New("Record sets not supported by binary format.")
}

func (w *BinRecordSetWriter) EndColumn(rs *RecordSet) error {
	return nil
}

func (w *BinRecordSetWriter) BeginArray(rs *RecordSet, size int) error {
	return errors.New("Arrays not supported by binary format.")
}

func (w *BinRecordSetWriter) EndArray(rs *RecordSet) error {
	return nil
}

func (w *BinRecordSetWriter) BeginObject(rs *RecordSet) error {
	return errors.New("Objects not supported by binary format.")
}

func (w *BinRecordSetWriter) EndObject(rs *RecordSet) error {
	return nil
}

func (w *BinRecordSetWriter) Null(rs *RecordSet) error {
	return nil
}

func (w *BinRecordSetWriter) Bool(rs *RecordSet, v bool) error {
	if v {
		return w.String(rs, "true")
	} else {
		return w.String(rs, "false")
	}
}

func (w *BinRecordSetWriter) Integer(rs *RecordSet, v int64) error {
	return w.String(rs, strconv.FormatInt(v, 10))
}

func (w *BinRecordSetWriter) Float(rs *RecordSet, v float64) error {
	return w.String(rs, strconv.FormatFloat(v, 'g', -1, 64))
}

func (w *BinRecordSetWriter) Numeric(rs *RecordSet, v string) error {
	return w.String(rs, v)
}

func (w *BinRecordSetWriter) Date(rs *RecordSet, v time.Time) error {
	return w.String(rs, v.Format("2006-01-02"))
}

func (w *BinRecordSetWriter) DateTime(rs *RecordSet, v time.Time) error {
	return w.String(rs, v.Format(time.RFC3339))
}

func (w *BinRecordSetWriter) String(rs *RecordSet, v string) error {
	if w.Len() > 0 {
		return errors.New("Binary format may contain only one scalar value.")
	}
	
	w.WriteString(v) // strings are UTF-8 encoded byte arrays in Go
	
	return w.checkSize()
}

func (w *BinRecordSetWriter) Bytes(rs *RecordSet, v []byte) error {
	if w.Len() > 0 {
		return errors.New("Binary format may contain only one scalar value.")
	}
	
	w.Write(v)
	
	return w.checkSize()
}

func (w *BinRecordSetWriter) Json(rs *RecordSet, v json.RawMessage) error {
	return w.String(rs, string(v))
}

func (w *BinRecordSetWriter) checkSize() error {
	if int64(w.Len()) > w.MaxResponseSizeBytes {
		return errors.New("Response too long.")
	}
	return nil
}
