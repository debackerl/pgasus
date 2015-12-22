// Copyright (c) 2015 Laurent Debacker
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
	//"gopkg.in/jackc/pgx.v2"
	"github.com/jackc/pgx"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

type Records []map[string]interface{}

func readRecords(dst RecordSetVisitor, singleRow bool, rows *pgx.Rows) error {
	rs := RecordSet {
		Visitor: dst,
		Columns: rows.FieldDescriptions(),
		IncludeColumnNames: true,
	}
	
	count := len(rs.Columns)
	scanners := make([]interface{}, count)
	
	for i := 0; i < count; i++ {
		scanners[i] = rs.GetColumnScanner(i)
	}
	
	if !singleRow {
		if err := rs.Visitor.BeginRecordSet(&rs); err != nil {
			return err
		}
	}
	
	for rows.Next() {
		if err := rs.Visitor.BeginRecord(&rs); err != nil {
			return err
		}
		
		if err := rows.Scan(scanners...); err != nil {
			return err
		}
		
		if err := rs.Visitor.EndRecord(&rs); err != nil {
			return err
		}
	}
	
	if rows.Err() != nil {
		return rows.Err()
	}
	
	if !singleRow {
		if err := rs.Visitor.EndRecordSet(&rs); err != nil {
			return err
		}
	}
	
	return nil
}

func readScalar(dst RecordSetVisitor, rows *pgx.Rows) error {
	rs := RecordSet {
		Visitor: dst,
		Columns: rows.FieldDescriptions(),
		IncludeColumnNames: false,
	}
	
	if rows.Next() {
		if err := rows.Scan(rs.GetColumnScanner(0)); err != nil {
			return err
		}
	} else {
		return rows.Err()
	}
	
	return nil
}

type RecordSetVisitor interface {
	BeginBatch() error
	EndBatch() error
	
	BeginRecordSet(rs *RecordSet) error
	EndRecordSet(rs *RecordSet) error
	
	BeginRecord(rs *RecordSet) error
	EndRecord(rs *RecordSet) error
	
	BeginColumn(rs *RecordSet) error
	EndColumn(rs *RecordSet) error
	
	BeginArray(rs *RecordSet, size int) error
	EndArray(rs *RecordSet) error
	
	BeginObject(rs *RecordSet) error
	EndObject(rs *RecordSet) error
	
	Null(rs *RecordSet) error
	Bool(rs *RecordSet, v bool) error
	Integer(rs *RecordSet, v int64) error
	Float(rs *RecordSet, v float64) error
	Numeric(rs *RecordSet, v string) error
	Time(rs *RecordSet, v time.Time) error
	String(rs *RecordSet, v string) error
	Bytes(rs *RecordSet, v []byte) error
	Json(rs *RecordSet, v json.RawMessage) error
}

type columnScanner struct {
	RecordSet *RecordSet
	Index int
}

func (cs *columnScanner) Scan(r *pgx.ValueReader) error {
	return cs.RecordSet.scan(cs.Index, r)
}

type RecordSet struct {
	Visitor RecordSetVisitor
	Columns []pgx.FieldDescription
	IncludeColumnNames bool
	
	curCol int
}

func VisitRowsAffectedRecordSet(visitor RecordSetVisitor, rowsAffected int64) error {
	rs := RecordSet {
		Visitor: visitor,
		Columns: []pgx.FieldDescription{ pgx.FieldDescription{
			Name: "RowsAffected",
			DataType: pgx.Int8Oid,
			DataTypeSize: 8,
			DataTypeName: "int8",
		}},
	}
	
	if err := rs.Visitor.BeginRecordSet(&rs); err != nil {
		return err
	}
	if err := rs.Visitor.BeginRecord(&rs); err != nil {
		return err
	}
	if err := rs.Visitor.BeginColumn(&rs); err != nil {
		return err
	}
	if err := rs.Visitor.Integer(&rs, rowsAffected); err != nil {
		return err
	}
	if err := rs.Visitor.EndColumn(&rs); err != nil {
		return err
	}
	if err := rs.Visitor.EndRecord(&rs); err != nil {
		return err
	}
	if err := rs.Visitor.EndRecordSet(&rs); err != nil {
		return err
	}
	
	return nil
}

func (rs *RecordSet) GetColumnScanner(i int) pgx.Scanner {
	return &columnScanner {
		RecordSet: rs,
		Index: i,
	}
}

func (rs *RecordSet) CurrentColumn() (int, *pgx.FieldDescription) {
	return rs.curCol, &rs.Columns[rs.curCol]
}

func (rs *RecordSet) scan(col int, vr *pgx.ValueReader) (err error) {
	t := vr.Type()
	v := rs.Visitor
	
	rs.curCol = col
	
	if rs.IncludeColumnNames {
		if err := v.BeginColumn(rs); err != nil {
			return err
		}
	}
	
	err = nil
	
	if vr.Len() == -1 {
		v.Null(rs)
	} else {
		// more complete source of Oids: https://github.com/epgsql/epgsql/blob/master/src/epgsql_types.erl
		
		switch t.FormatCode {
		case pgx.TextFormatCode:
			s := vr.ReadString(vr.Len())
			
			switch t.DataTypeName {
			case "hstore":
				err = ParseHstore(s, rs)
			case "json", "jsonb":
				rs.Visitor.Json(rs, json.RawMessage(s))
			case "numeric":
				rs.Visitor.Numeric(rs, s)
			default:
				rs.Visitor.String(rs, s)
			}
		case pgx.BinaryFormatCode:
			switch t.DataType {
			case pgx.BoolOid:
				err = rs.decodeBool(vr)
			case pgx.ByteaOid:
				err = rs.decodeBytea(vr)
			case pgx.Int8Oid:
				err = rs.decodeInt8(vr)
			case pgx.Int2Oid:
				err = rs.decodeInt2(vr)
			case pgx.Int4Oid:
				err = rs.decodeInt4(vr)
			case pgx.Float4Oid:
				err = rs.decodeFloat4(vr)
			case pgx.Float8Oid:
				err = rs.decodeFloat8(vr)
			case pgx.TimestampOid, pgx.TimestampTzOid:
				err = rs.decodeTimestamp(vr)
			case pgx.DateOid:
				err = rs.decodeDate(vr)
			case pgx.BoolArrayOid:
				err = rs.decodeBoolArray(vr)
			case pgx.Int2ArrayOid:
				err = rs.decodeInt2Array(vr)
			case pgx.Int4ArrayOid:
				err = rs.decodeInt4Array(vr)
			case pgx.Int8ArrayOid:
				err = rs.decodeInt8Array(vr)
			case pgx.Float4ArrayOid:
				err = rs.decodeFloat4Array(vr)
			case pgx.Float8ArrayOid:
				err = rs.decodeFloat8Array(vr)
			case pgx.TextArrayOid, pgx.VarcharArrayOid:
				err = rs.decodeTextArray(vr)
			case pgx.TimestampArrayOid, pgx.TimestampTzArrayOid:
				err = rs.decodeTimestampArray(vr)
			case 1182:
				err = rs.decodeDateArray(vr)
			default:
				err = errors.New("Unknown value type for binary format.")
			}
		}
	}
	
	if err == nil && rs.IncludeColumnNames {
		return v.EndColumn(rs)
	}
	
	return err
}

func (rs *RecordSet) decodeBool(vr *pgx.ValueReader) error {
	if vr.Len() != 1 {
		return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a bool: %d", vr.Len()))
	}

	b := vr.ReadByte()
	return rs.Visitor.Bool(rs, b != 0)
}

func (rs *RecordSet) decodeInt8(vr *pgx.ValueReader) error {
	if vr.Len() != 8 {
		return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for an int8: %d", vr.Len()))
	}

	return rs.Visitor.Integer(rs, vr.ReadInt64())
}

func (rs *RecordSet) decodeInt2(vr *pgx.ValueReader) error {
	if vr.Len() != 2 {
		return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for an int2: %d", vr.Len()))
	}

	return rs.Visitor.Integer(rs, int64(vr.ReadInt16()))
}

func (rs *RecordSet) decodeInt4(vr *pgx.ValueReader) error {
	if vr.Len() != 4 {
		return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for an int4: %d", vr.Len()))
	}

	return rs.Visitor.Integer(rs, int64(vr.ReadInt32()))
}

func (rs *RecordSet) decodeFloat4(vr *pgx.ValueReader) error {
	if vr.Len() != 4 {
		return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a float4: %d", vr.Len()))
	}

	i := vr.ReadInt32()
	return rs.Visitor.Float(rs, float64(math.Float32frombits(uint32(i))))
}

func (rs *RecordSet) decodeFloat8(vr *pgx.ValueReader) error {
	if vr.Len() != 8 {
		return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a float8: %d", vr.Len()))
	}

	i := vr.ReadInt64()
	return rs.Visitor.Float(rs, math.Float64frombits(uint64(i)))
}

func (rs *RecordSet) decodeBytea(vr *pgx.ValueReader) error {
	return rs.Visitor.Bytes(rs, vr.ReadBytes(vr.Len()))
}

const microsecFromUnixEpochToY2K = 946684800 * 1000000

func (rs *RecordSet) decodeTimestamp(vr *pgx.ValueReader) error {
	if vr.Len() != 8 {
		return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a timestamp: %d", vr.Len()))
	}

	microsecSinceY2K := vr.ReadInt64()
	microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
	return rs.Visitor.Time(rs, time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000))
}

func (rs *RecordSet) decodeDate(vr *pgx.ValueReader) error {
	if vr.Len() != 4 {
		return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a date: %d", vr.Len()))
	}
	dayOffset := vr.ReadInt32()
	return rs.Visitor.Time(rs, time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.Local))
}

func (rs *RecordSet) decode1dArrayHeader(vr *pgx.ValueReader) (length int32, err error) {
	numDims := vr.ReadInt32()
	if numDims > 1 {
		return 0, pgx.ProtocolError(fmt.Sprintf("Expected array to have 0 or 1 dimension, but it had %v", numDims))
	}

	vr.ReadInt32() // 0 if no nulls / 1 if there is one or more nulls -- but we don't care
	vr.ReadInt32() // element oid

	if numDims == 0 {
		return 0, nil
	}

	length = vr.ReadInt32()

	idxFirstElem := vr.ReadInt32()
	if idxFirstElem != 1 {
		return 0, pgx.ProtocolError(fmt.Sprintf("Expected array's first element to start a index 1, but it is %d", idxFirstElem))
	}

	return length, nil
}

func (rs *RecordSet) decodeBoolArray(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 1:
			if err := rs.Visitor.Bool(rs, vr.ReadByte() == 1); err != nil {
				return err
			}
		case -1:
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		default:
			return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a bool element: %d", elSize))
		}
	}

	return rs.Visitor.EndArray(rs)
}

func (rs *RecordSet) decodeInt2Array(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 2:
			if err := rs.Visitor.Integer(rs, int64(vr.ReadInt16())); err != nil {
				return err
			}
		case -1:
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		default:
			return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for an int2 element: %d", elSize))
		}
	}

	return rs.Visitor.EndArray(rs)
}

func (rs *RecordSet) decodeInt4Array(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 4:
			if err := rs.Visitor.Integer(rs, int64(vr.ReadInt32())); err != nil {
				return err
			}
		case -1:
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		default:
			return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for an int4 element: %d", elSize))
		}
	}

	return rs.Visitor.EndArray(rs)
}

func (rs *RecordSet) decodeInt8Array(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 8:
			if err := rs.Visitor.Integer(rs, vr.ReadInt64()); err != nil {
				return err
			}
		case -1:
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		default:
			return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for an int8 element: %d", elSize))
		}
	}

	return rs.Visitor.EndArray(rs)
}

func (rs *RecordSet) decodeFloat4Array(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 4:
			n := vr.ReadInt32()
			if err := rs.Visitor.Float(rs, float64(math.Float32frombits(uint32(n)))); err != nil {
				return err
			}
		case -1:
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		default:
			return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a float4 element: %d", elSize))
		}
	}

	return rs.Visitor.EndArray(rs)
}

func (rs *RecordSet) decodeFloat8Array(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 8:
			n := vr.ReadInt64()
			if err := rs.Visitor.Float(rs, math.Float64frombits(uint64(n))); err != nil {
				return err
			}
		case -1:
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		default:
			return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a float4 element: %d", elSize))
		}
	}

	return rs.Visitor.EndArray(rs)
}

func (rs *RecordSet) decodeTextArray(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		if elSize == -1 {
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		} else {
			if err := rs.Visitor.String(rs, vr.ReadString(elSize)); err != nil {
				return err
			}
		}
	}

	return rs.Visitor.EndArray(rs)
}

func (rs *RecordSet) decodeTimestampArray(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 8:
			if err := rs.decodeTimestamp(vr); err != nil {
				return err
			}
		case -1:
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		default:
			return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a timestamp. Timestamp element: %d", elSize))
		}
	}

	return rs.Visitor.EndArray(rs)
}

func (rs *RecordSet) decodeDateArray(vr *pgx.ValueReader) error {
	numElems, err := rs.decode1dArrayHeader(vr)
	if err != nil {
		return err
	}

	if err := rs.Visitor.BeginArray(rs, int(numElems)); err != nil {
		return err
	}

	for i := int32(0); i < numElems; i++ {
		elSize := vr.ReadInt32()
		switch elSize {
		case 4:
			if err := rs.decodeDate(vr); err != nil {
				return err
			}
		case -1:
			if err := rs.Visitor.Null(rs); err != nil {
				return err
			}
		default:
			return pgx.ProtocolError(fmt.Sprintf("Received an invalid size for a date. Date element: %d", elSize))
		}
	}

	return rs.Visitor.EndArray(rs)
}
