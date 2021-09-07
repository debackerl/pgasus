package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

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
	Date(rs *RecordSet, v time.Time) error
	DateTime(rs *RecordSet, v time.Time) error
	String(rs *RecordSet, v string) error
	Bytes(rs *RecordSet, v []byte) error
	Json(rs *RecordSet, v json.RawMessage) error
}

type Field interface {
	DbValue() interface{}
	Accept(rs *RecordSet, visitor RecordSetVisitor)
}

type BoolField struct {
	pgtype.Bool
}

func (f *BoolField) DbValue() interface{} {
	return &f.Bool
}

func (f *BoolField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Bool(rs, f.Bool.Bool)
	} else {
		visitor.Null(rs)
	}
}

type BoolArrayField struct {
	pgtype.BoolArray
}

func (f *BoolArrayField) DbValue() interface{} {
	return &f.BoolArray
}

func (f *BoolArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Bool(rs, element.Bool)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type Int2Field struct {
	pgtype.Int2
}

func (f *Int2Field) DbValue() interface{} {
	return &f.Int2
}

func (f *Int2Field) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Integer(rs, int64(f.Int2.Int))
	} else {
		visitor.Null(rs)
	}
}

type Int2ArrayField struct {
	pgtype.Int2Array
}

func (f *Int2ArrayField) DbValue() interface{} {
	return &f.Int2Array
}

func (f *Int2ArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Integer(rs, int64(element.Int))
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type Int4Field struct {
	pgtype.Int4
}

func (f *Int4Field) DbValue() interface{} {
	return &f.Int4
}

func (f *Int4Field) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Integer(rs, int64(f.Int4.Int))
	} else {
		visitor.Null(rs)
	}
}

type Int4ArrayField struct {
	pgtype.Int4Array
}

func (f *Int4ArrayField) DbValue() interface{} {
	return &f.Int4Array
}

func (f *Int4ArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Integer(rs, int64(element.Int))
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type Int8Field struct {
	pgtype.Int8
}

func (f *Int8Field) DbValue() interface{} {
	return &f.Int8
}

func (f *Int8Field) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Integer(rs, f.Int8.Int)
	} else {
		visitor.Null(rs)
	}
}

type Int8ArrayField struct {
	pgtype.Int8Array
}

func (f *Int8ArrayField) DbValue() interface{} {
	return &f.Int8Array
}

func (f *Int8ArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Integer(rs, element.Int)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type Float4Field struct {
	pgtype.Float4
}

func (f *Float4Field) DbValue() interface{} {
	return &f.Float4
}

func (f *Float4Field) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Float(rs, float64(f.Float4.Float))
	} else {
		visitor.Null(rs)
	}
}

type Float4ArrayField struct {
	pgtype.Float4Array
}

func (f *Float4ArrayField) DbValue() interface{} {
	return &f.Float4Array
}

func (f *Float4ArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Float(rs, float64(element.Float))
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type Float8Field struct {
	pgtype.Float8
}

func (f *Float8Field) DbValue() interface{} {
	return &f.Float8
}

func (f *Float8Field) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Float(rs, f.Float8.Float)
	} else {
		visitor.Null(rs)
	}
}

type Float8ArrayField struct {
	pgtype.Float8Array
}

func (f *Float8ArrayField) DbValue() interface{} {
	return &f.Float8Array
}

func (f *Float8ArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Float(rs, element.Float)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

func numericToString(n *pgtype.Numeric) string {
	if n.NaN {
		return "NaN"
	}

	s := n.Int.String()
	if n.Exp == 0 {
		return s
	}

	var b strings.Builder
	if n.Exp > 0 {
		b.WriteString(s)
		for i := int32(0); i < n.Exp; i++ {
			b.WriteRune('0')
		}
	} else {
		p := len(s) + int(n.Exp)
		b.WriteString(s[:p])
		b.WriteRune('.')
		b.WriteString(s[p:])
	}
	return b.String()
}

type NumericField struct {
	pgtype.Numeric
}

func (f *NumericField) DbValue() interface{} {
	return &f.Numeric
}

func (f *NumericField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Numeric(rs, numericToString(&f.Numeric))
	} else {
		visitor.Null(rs)
	}
}

type NumericArrayField struct {
	pgtype.NumericArray
}

func (f *NumericArrayField) DbValue() interface{} {
	return &f.NumericArray
}

func (f *NumericArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Numeric(rs, numericToString(&element))
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

func inetToString(i *pgtype.Inet) string {
	ip := i.IPNet
	ones, bits := ip.Mask.Size()

	if ones == bits {
		return ip.IP.String()
	}

	return ip.String()
}

type InetField struct {
	pgtype.Inet
}

func (f *InetField) DbValue() interface{} {
	return &f.Inet
}

func (f *InetField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.String(rs, inetToString(&f.Inet))
	} else {
		visitor.Null(rs)
	}
}

type InetArrayField struct {
	pgtype.InetArray
}

func (f *InetArrayField) DbValue() interface{} {
	return &f.InetArray
}

func (f *InetArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.String(rs, inetToString(&element))
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type CIDRField struct {
	pgtype.CIDR
}

func (f *CIDRField) DbValue() interface{} {
	return &f.CIDR
}

func (f *CIDRField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.String(rs, f.CIDR.IPNet.String())
	} else {
		visitor.Null(rs)
	}
}

type CIDRArrayField struct {
	pgtype.CIDRArray
}

func (f *CIDRArrayField) DbValue() interface{} {
	return &f.CIDRArray
}

func (f *CIDRArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.String(rs, element.IPNet.String())
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type TimestampField struct {
	pgtype.Timestamp
}

func (f *TimestampField) DbValue() interface{} {
	return &f.Timestamp
}

func (f *TimestampField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.DateTime(rs, f.Timestamp.Time)
	} else {
		visitor.Null(rs)
	}
}

type TimestampArrayField struct {
	pgtype.TimestampArray
}

func (f *TimestampArrayField) DbValue() interface{} {
	return &f.TimestampArray
}

func (f *TimestampArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.DateTime(rs, element.Time)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type TimestamptzField struct {
	pgtype.Timestamptz
}

func (f *TimestamptzField) DbValue() interface{} {
	return &f.Timestamptz
}

func (f *TimestamptzField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.DateTime(rs, f.Timestamptz.Time)
	} else {
		visitor.Null(rs)
	}
}

type TimestamptzArrayField struct {
	pgtype.TimestamptzArray
}

func (f *TimestamptzArrayField) DbValue() interface{} {
	return &f.TimestamptzArray
}

func (f *TimestamptzArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.DateTime(rs, element.Time)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type DateField struct {
	pgtype.Date
}

func (f *DateField) DbValue() interface{} {
	return &f.Date
}

func (f *DateField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Date(rs, f.Date.Time)
	} else {
		visitor.Null(rs)
	}
}

type DateArrayField struct {
	pgtype.DateArray
}

func (f *DateArrayField) DbValue() interface{} {
	return &f.DateArray
}

func (f *DateArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Date(rs, element.Time)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type BPCharField struct {
	pgtype.BPChar
}

func (f *BPCharField) DbValue() interface{} {
	return &f.BPChar
}

func (f *BPCharField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.String(rs, f.BPChar.String)
	} else {
		visitor.Null(rs)
	}
}

type BPCharArrayField struct {
	pgtype.BPCharArray
}

func (f *BPCharArrayField) DbValue() interface{} {
	return &f.BPCharArray
}

func (f *BPCharArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.String(rs, element.String)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type TextField struct {
	pgtype.Text
}

func (f *TextField) DbValue() interface{} {
	return &f.Text
}

func (f *TextField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.String(rs, f.Text.String)
	} else {
		visitor.Null(rs)
	}
}

type TextArrayField struct {
	pgtype.TextArray
}

func (f *TextArrayField) DbValue() interface{} {
	return &f.TextArray
}

func (f *TextArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.String(rs, element.String)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type VarcharField struct {
	pgtype.Varchar
}

func (f *VarcharField) DbValue() interface{} {
	return &f.Varchar
}

func (f *VarcharField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.String(rs, f.Varchar.String)
	} else {
		visitor.Null(rs)
	}
}

type VarcharArrayField struct {
	pgtype.VarcharArray
}

func (f *VarcharArrayField) DbValue() interface{} {
	return &f.VarcharArray
}

func (f *VarcharArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.String(rs, element.String)
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type JSONField struct {
	pgtype.JSON
}

func (f *JSONField) DbValue() interface{} {
	return &f.JSON
}

func (f *JSONField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Json(rs, json.RawMessage(f.JSON.Bytes))
	} else {
		visitor.Null(rs)
	}
}

type JSONBField struct {
	pgtype.JSONB
}

func (f *JSONBField) DbValue() interface{} {
	return &f.JSONB
}

func (f *JSONBField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		visitor.Json(rs, json.RawMessage(f.JSONB.Bytes))
	} else {
		visitor.Null(rs)
	}
}

type JSONBArrayField struct {
	pgtype.JSONBArray
}

func (f *JSONBArrayField) DbValue() interface{} {
	return &f.JSONBArray
}

func (f *JSONBArrayField) Accept(rs *RecordSet, visitor RecordSetVisitor) {
	if f.Status == pgtype.Present {
		elements := f.Elements
		visitor.BeginArray(rs, len(elements))
		for _, element := range elements {
			if element.Status == pgtype.Present {
				visitor.Json(rs, json.RawMessage(element.Bytes))
			} else {
				visitor.Null(rs)
			}
		}
		visitor.EndArray(rs)
	} else {
		visitor.Null(rs)
	}
}

type FieldBuilder func() Field

var fieldsByOid map[uint32]FieldBuilder

func init() {
	fieldsByOid = make(map[uint32]FieldBuilder)
	fieldsByOid[pgtype.BoolOID] = func() Field { return &BoolField{} }
	fieldsByOid[pgtype.BoolArrayOID] = func() Field { return &BoolArrayField{} }
	fieldsByOid[pgtype.Int2OID] = func() Field { return &Int2Field{} }
	fieldsByOid[pgtype.Int2ArrayOID] = func() Field { return &Int2ArrayField{} }
	fieldsByOid[pgtype.Int4OID] = func() Field { return &Int4Field{} }
	fieldsByOid[pgtype.Int4ArrayOID] = func() Field { return &Int4ArrayField{} }
	fieldsByOid[pgtype.Int8OID] = func() Field { return &Int8Field{} }
	fieldsByOid[pgtype.Int8ArrayOID] = func() Field { return &Int8ArrayField{} }
	fieldsByOid[pgtype.Float4OID] = func() Field { return &Float4Field{} }
	fieldsByOid[pgtype.Float4ArrayOID] = func() Field { return &Float4ArrayField{} }
	fieldsByOid[pgtype.Float8OID] = func() Field { return &Float8Field{} }
	fieldsByOid[pgtype.Float8ArrayOID] = func() Field { return &Float8ArrayField{} }
	fieldsByOid[pgtype.NumericOID] = func() Field { return &NumericField{} }
	fieldsByOid[pgtype.NumericArrayOID] = func() Field { return &NumericArrayField{} }
	fieldsByOid[pgtype.InetOID] = func() Field { return &InetField{} }
	fieldsByOid[pgtype.InetArrayOID] = func() Field { return &InetArrayField{} }
	fieldsByOid[pgtype.CIDROID] = func() Field { return &CIDRField{} }
	fieldsByOid[pgtype.CIDRArrayOID] = func() Field { return &CIDRArrayField{} }
	fieldsByOid[pgtype.TimestampOID] = func() Field { return &TimestampField{} }
	fieldsByOid[pgtype.TimestampArrayOID] = func() Field { return &TimestampArrayField{} }
	fieldsByOid[pgtype.TimestamptzOID] = func() Field { return &TimestamptzField{} }
	fieldsByOid[pgtype.TimestamptzArrayOID] = func() Field { return &TimestamptzArrayField{} }
	fieldsByOid[pgtype.DateOID] = func() Field { return &DateField{} }
	fieldsByOid[pgtype.DateArrayOID] = func() Field { return &DateArrayField{} }
	fieldsByOid[pgtype.BPCharOID] = func() Field { return &BPCharField{} }
	fieldsByOid[pgtype.BPCharArrayOID] = func() Field { return &BPCharArrayField{} }
	fieldsByOid[pgtype.TextOID] = func() Field { return &TextField{} }
	fieldsByOid[pgtype.TextArrayOID] = func() Field { return &TextArrayField{} }
	fieldsByOid[pgtype.VarcharOID] = func() Field { return &VarcharField{} }
	fieldsByOid[pgtype.VarcharArrayOID] = func() Field { return &VarcharArrayField{} }
	fieldsByOid[pgtype.JSONOID] = func() Field { return &JSONField{} }
	fieldsByOid[pgtype.JSONBOID] = func() Field { return &JSONBField{} }
	fieldsByOid[pgtype.JSONBArrayOID] = func() Field { return &JSONBArrayField{} }
}

func readRecords(dst RecordSetVisitor, singleRow bool, rows pgx.Rows) error {
	rs := RecordSet{
		Visitor:            dst,
		Columns:            rows.FieldDescriptions(),
		IncludeColumnNames: true,
	}

	count := len(rs.Columns)
	fields := make([]Field, count)
	values := make([]interface{}, count)

	for i := 0; i < count; i++ {
		var found bool
		var builder FieldBuilder
		oid := rs.Columns[i].DataTypeOID
		// our own copy of a Field is done below
		if builder, found = fieldsByOid[oid]; !found {
			return fmt.Errorf("Unknown oid: %#v", oid)
		}

		fields[i] = builder()
		values[i] = fields[i].DbValue()
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

		if err := rows.Scan(values...); err != nil {
			return err
		}

		for i := 0; i < count; i++ {
			fields[i].Accept(&rs, dst)
		}

		if err := rs.Visitor.EndRecord(&rs); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if !singleRow {
		if err := rs.Visitor.EndRecordSet(&rs); err != nil {
			return err
		}
	}

	return nil
}

func readScalar(dst RecordSetVisitor, rows pgx.Rows) error {
	rs := RecordSet{
		Visitor:            dst,
		Columns:            rows.FieldDescriptions(),
		IncludeColumnNames: false,
	}

	oid := rs.Columns[0].DataTypeOID
	var found bool
	var builder FieldBuilder
	// our own copy of a Field is done below
	if builder, found = fieldsByOid[oid]; !found {
		return fmt.Errorf("Unknown oid: %#v", oid)
	}

	field := builder()

	if rows.Next() {
		if err := rows.Scan(field.DbValue()); err != nil {
			return err
		}

		field.Accept(&rs, dst)
	} else {
		return rows.Err()
	}

	return nil
}

type columnScanner struct {
	RecordSet *RecordSet
	Index     int
}

type RecordSet struct {
	Visitor            RecordSetVisitor
	Columns            []pgproto3.FieldDescription
	IncludeColumnNames bool

	curCol int
}

func VisitRowsAffectedRecordSet(visitor RecordSetVisitor, rowsAffected int64) error {
	rs := RecordSet{
		Visitor: visitor,
		Columns: []pgproto3.FieldDescription{pgproto3.FieldDescription{
			Name:                 []byte("RowsAffected"),
			DataTypeOID:          pgtype.Int8OID,
			DataTypeSize:         8,
			TableOID:             0,
			TableAttributeNumber: 0,
			TypeModifier:         -1,
			Format:               0,
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

func (rs *RecordSet) CurrentColumn() (int, *pgproto3.FieldDescription) {
	return rs.curCol, &rs.Columns[rs.curCol]
}
