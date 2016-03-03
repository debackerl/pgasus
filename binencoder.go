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
	if w.Len() > 0 {
		return errors.New("Binary format may contain only one scalar value.")
	}
	
	if v {
		w.WriteString("true")
	} else {
		w.WriteString("false")
	}
	
	return w.checkSize()
}

func (w *BinRecordSetWriter) Integer(rs *RecordSet, v int64) error {
	if w.Len() > 0 {
		return errors.New("Binary format may contain only one scalar value.")
	}
	
	w.WriteString(strconv.FormatInt(v, 10))
	
	return w.checkSize()
}

func (w *BinRecordSetWriter) Float(rs *RecordSet, v float64) error {
	if w.Len() > 0 {
		return errors.New("Binary format may contain only one scalar value.")
	}
	
	w.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	
	return w.checkSize()
}

func (w *BinRecordSetWriter) Numeric(rs *RecordSet, v string) error {
	if w.Len() > 0 {
		return errors.New("Binary format may contain only one scalar value.")
	}
	
	w.WriteString(v)
	
	return w.checkSize()
}

func (w *BinRecordSetWriter) Time(rs *RecordSet, v time.Time) error {
	if w.Len() > 0 {
		return errors.New("Binary format may contain only one scalar value.")
	}
	
	w.WriteString(v.Format(time.RFC3339))
	
	return w.checkSize()
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
	if w.Len() > 0 {
		return errors.New("Binary format may contain only one scalar value.")
	}
	
	w.String(rs, string(v))
	
	return w.checkSize()
}

func (w *BinRecordSetWriter) checkSize() error {
	if int64(w.Len()) > w.MaxResponseSizeBytes {
		return errors.New("Response too long.")
	}
	return nil
}
