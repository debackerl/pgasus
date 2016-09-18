package main


import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
	"github.com/tealeg/xlsx"
)

const XlsxMimeType string = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

type XlsxRecordSetWriter struct {
	MaxResponseSizeBytes int64
	deflatedSize int64
	depth int
	sheetCounter int
	file *xlsx.File
	sheet *xlsx.Sheet
	row *xlsx.Row
	cell *xlsx.Cell
}

func NewXlsxRecordSetWriter(maxResponseSizeBytes int64) *XlsxRecordSetWriter {
	return &XlsxRecordSetWriter{
		MaxResponseSizeBytes: maxResponseSizeBytes,
		file: xlsx.NewFile(),
	}
}

func (w *XlsxRecordSetWriter) ToBytes() (bs []byte, err error) {
	var buf bytes.Buffer
	wtr := NewCappedWriter(&buf, w.MaxResponseSizeBytes)
	if err := w.file.Write(wtr); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (w *XlsxRecordSetWriter) HttpRespond(hw http.ResponseWriter) {
	hw.Header().Set("Content-Type", XlsxMimeType)
	hw.WriteHeader(http.StatusOK)
	bytes, err := w.ToBytes()
	if err != nil {
		panic(err)
	}
	hw.Write(bytes)
}

func (w *XlsxRecordSetWriter) BeginBatch() error {
	return nil
}

func (w *XlsxRecordSetWriter) EndBatch() error {
	return nil
}

func (w *XlsxRecordSetWriter) BeginRecordSet(rs *RecordSet) error {
	return nil
}

func (w *XlsxRecordSetWriter) EndRecordSet(rs *RecordSet) error {
	w.sheet = nil
	return nil
}

func (w *XlsxRecordSetWriter) BeginRecord(rs *RecordSet) error {
	if w.sheet == nil {
		// we don't place the following in BeginRecordSet because we still want
		// columns header for results with single row (whence BeginRecordSet is called)
		
		if err := w.addSheet(); err != nil {
			return err
		}
		
		row := w.sheet.AddRow()
		for _, col := range rs.Columns {
			cell := row.AddCell()
			cell.SetString(col.Name)
		}
	}
	
	return nil
}

func (w *XlsxRecordSetWriter) EndRecord(rs *RecordSet) error {
	w.row = nil
	return nil
}

func (w *XlsxRecordSetWriter) BeginColumn(rs *RecordSet) error {
	w.depth = 0
	return nil
}

func (w *XlsxRecordSetWriter) EndColumn(rs *RecordSet) error {
	w.cell = nil
	return nil
}

func (w *XlsxRecordSetWriter) BeginArray(rs *RecordSet, size int) error {
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetString("array")
	w.depth++
	return w.check()
}

func (w *XlsxRecordSetWriter) EndArray(rs *RecordSet) error {
	w.depth--
	return nil
}

func (w *XlsxRecordSetWriter) BeginObject(rs *RecordSet) error {
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetString("object")
	w.depth++
	return w.check()
}

func (w *XlsxRecordSetWriter) EndObject(rs *RecordSet) error {
	w.depth--
	return nil
}

func (w *XlsxRecordSetWriter) Null(rs *RecordSet) error {
	if w.depth > 0 {
		return nil
	}
	
	return w.check()
}

func (w *XlsxRecordSetWriter) Bool(rs *RecordSet, v bool) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetBool(v)
	return w.check()
}

func (w *XlsxRecordSetWriter) Integer(rs *RecordSet, v int64) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetInt64(v)
	return w.check()
}

func (w *XlsxRecordSetWriter) Float(rs *RecordSet, v float64) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetFloat(v)
	return w.check()
}

func (w *XlsxRecordSetWriter) Numeric(rs *RecordSet, v string) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetString(v)
	return w.check()
}

func (w *XlsxRecordSetWriter) Date(rs *RecordSet, v time.Time) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetDate(v)
	return w.check()
}

func (w *XlsxRecordSetWriter) DateTime(rs *RecordSet, v time.Time) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetDateTime(v)
	return w.check()
}

func (w *XlsxRecordSetWriter) String(rs *RecordSet, v string) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetString(v)
	return w.check()
}

func (w *XlsxRecordSetWriter) Bytes(rs *RecordSet, v []byte) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetString(base64.StdEncoding.EncodeToString(v))
	return w.check()
}

func (w *XlsxRecordSetWriter) Json(rs *RecordSet, v json.RawMessage) error {
	if w.depth > 0 {
		return nil
	}
	
	if err := w.addCell(); err != nil {
		return err
	}
	
	w.cell.SetString(string(v))
	return w.check()
}

func (w *XlsxRecordSetWriter) addSheet() error {
	w.sheetCounter += 1
	var err error
	w.sheet, err = w.file.AddSheet(fmt.Sprintf("Sheet%d", w.sheetCounter))
	return err
}

func (w *XlsxRecordSetWriter) addCell() error {
	if w.sheet == nil {
		if err := w.addSheet(); err != nil {
			return err
		}
	}
	if w.row == nil {
		w.row = w.sheet.AddRow()
	}
	if w.cell == nil {
		w.cell = w.row.AddCell()
	}
	return nil
}

func (w *XlsxRecordSetWriter) check() error {
	w.deflatedSize += int64(len(w.cell.Value))
	if w.deflatedSize > w.MaxResponseSizeBytes {
		return errors.New("Response too long.")
	}
	
	return nil
}
