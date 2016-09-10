package main


import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type CsvRecordSetWriter struct {
	bytes.Buffer
	MaxResponseSizeBytes int64
	firstColumn bool
	depth int
}

func (w *CsvRecordSetWriter) ToBytes() []byte {
	return w.Buffer.Bytes()
}

func (w *CsvRecordSetWriter) HttpRespond(hw http.ResponseWriter) {
	hw.Header().Set("Content-Type", "text/csv; charset=utf-8")
	hw.WriteHeader(http.StatusOK)
	hw.Write(w.ToBytes())
}

func (w *CsvRecordSetWriter) BeginBatch() error {
	return errors.New("Batch mode not supported by CSV format.")
}

func (w *CsvRecordSetWriter) EndBatch() error {
	return nil
}

func (w *CsvRecordSetWriter) BeginRecordSet(rs *RecordSet) error {
	return nil
}

func (w *CsvRecordSetWriter) EndRecordSet(rs *RecordSet) error {
	return nil
}

func (w *CsvRecordSetWriter) BeginRecord(rs *RecordSet) error {
	if w.Len() == 0 {
		// we don't place the following in BeginRecordSet because we still want
		// columns header for results with single row (whence BeginRecordSet is called)
		
		names := make([]string, 0, 8)
		for _, col := range rs.Columns {
			names = append(names, `"` + strings.Replace(col.Name, `"`, `""`, -1) + `"`)
		}
		
		w.WriteString(strings.Join(names, ","))
	}
	
	w.WriteString("\r\n")
	w.firstColumn = true
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) EndRecord(rs *RecordSet) error {
	return nil
}

func (w *CsvRecordSetWriter) BeginColumn(rs *RecordSet) error {
	if w.firstColumn {
		w.firstColumn = false
	} else {
		w.WriteByte(',')
	}
	w.depth = 0
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) EndColumn(rs *RecordSet) error {
	return nil
}

func (w *CsvRecordSetWriter) BeginArray(rs *RecordSet, size int) error {
	w.WriteString("array")
	w.depth++
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) EndArray(rs *RecordSet) error {
	w.depth--
	
	return nil
}

func (w *CsvRecordSetWriter) BeginObject(rs *RecordSet) error {
	w.WriteString("object")
	w.depth++
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) EndObject(rs *RecordSet) error {
	w.depth--

	return nil
}

func (w *CsvRecordSetWriter) Null(rs *RecordSet) error {
	return nil
}

func (w *CsvRecordSetWriter) Bool(rs *RecordSet, v bool) error {
	if w.depth > 0 {
		return nil
	}
	
	if v {
		w.WriteString("true")
	} else {
		w.WriteString("false")
	}
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) Integer(rs *RecordSet, v int64) error {
	if w.depth > 0 {
		return nil
	}
	
	w.WriteString(strconv.FormatInt(v, 10))
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) Float(rs *RecordSet, v float64) error {
	if w.depth > 0 {
		return nil
	}
	
	w.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) Numeric(rs *RecordSet, v string) error {
	if w.depth > 0 {
		return nil
	}
	
	w.WriteString(v)
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) Date(rs *RecordSet, v time.Time) error {
	if w.depth > 0 {
		return nil
	}
	
	w.WriteString(v.Format("2006-01-02"))
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) DateTime(rs *RecordSet, v time.Time) error {
	if w.depth > 0 {
		return nil
	}
	
	w.WriteString(v.Format(time.RFC3339))
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) String(rs *RecordSet, v string) error {
	if w.depth > 0 {
		return nil
	}
	
	w.WriteByte('"')
	w.WriteString(strings.Replace(v, `"`, `""`, -1))
	w.WriteByte('"')
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) Bytes(rs *RecordSet, v []byte) error {
	if w.depth > 0 {
		return nil
	}
	
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
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) Json(rs *RecordSet, v json.RawMessage) error {
	if w.depth > 0 {
		return nil
	}
	
	w.String(rs, string(v))
	
	return w.checkSize()
}

func (w *CsvRecordSetWriter) checkSize() error {
	if int64(w.Len()) > w.MaxResponseSizeBytes {
		return errors.New("Response too long.")
	}
	return nil
}
