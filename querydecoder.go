package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/antonholmquist/jason"
	"github.com/jackc/pgtype"
)

func decodeHttpBody(w http.ResponseWriter, r *http.Request, argumentsType map[string]ArgumentType, readonlyFields map[string]struct{}, maxBodySizeKbytes int64) (queries []map[string]interface{}, batch bool, err error) {
	body := http.MaxBytesReader(w, r.Body, maxBodySizeKbytes*1024)

	queries = make([]map[string]interface{}, 0, 1)

	if r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" {
		buf := new(bytes.Buffer)
		buf.ReadFrom(body)

		var values url.Values
		values, err = url.ParseQuery(buf.String())
		if err != nil {
			queries = nil
			return
		}

		var query map[string]interface{}
		query, err = prepareArgumentsFromForm(values, argumentsType, readonlyFields)
		if err != nil {
			queries = nil
			return
		}

		queries = append(queries, query)
	} else {
		var value *jason.Value

		value, err = jason.NewValueFromReader(body)
		if err != nil {
			return
		}

		if array, ok := value.Array(); ok == nil {
			for _, subValue := range array {
				if object, ok := subValue.Object(); ok == nil {
					var query map[string]interface{}
					query, err = prepareArgumentsFromObject(object, argumentsType, readonlyFields)
					if err != nil {
						queries = nil
						return
					}

					queries = append(queries, query)
				} else {
					err = errors.New("Invalid json value type, array must contain objects.")
				}
			}
			batch = true
		} else if object, ok := value.Object(); ok == nil {
			var query map[string]interface{}
			query, err = prepareArgumentsFromObject(object, argumentsType, readonlyFields)
			if err != nil {
				queries = nil
				return
			}

			queries = append(queries, query)
			batch = false
		} else {
			err = errors.New("Invalid json value type, must be array or object.")
		}
	}

	return
}

func prepareArgumentsFromForm(arguments url.Values, argumentsType map[string]ArgumentType, readonlyFields map[string]struct{}) (query map[string]interface{}, err error) {
	query = make(map[string]interface{})

	for key, value := range arguments {
		key = strings.ToLower(key)

		if readonlyFields != nil {
			if _, ok := readonlyFields[key]; ok {
				continue
			}
		}

		if _, ok := argumentsType[key]; ok {
			query[key] = value[0]
		}
	}

	return
}

func prepareArgumentsFromObject(arguments *jason.Object, argumentsType map[string]ArgumentType, readonlyFields map[string]struct{}) (query map[string]interface{}, err error) {
	query = make(map[string]interface{})

	for key, value := range arguments.Map() {
		if readonlyFields != nil {
			if _, ok := readonlyFields[key]; ok {
				continue
			}
		}

		if typ, ok := argumentsType[key]; ok {
			var arg interface{}

			arg, err = decodeArgumentFromJsonValue(value, typ)
			if err != nil {
				return
			}

			query[key] = arg
		}
	}

	return
}

func prepareArgumentsFromQueryString(rawQuery string, argumentsType map[string]ArgumentType) (query map[string]interface{}, err error) {
	var values url.Values

	values, err = url.ParseQuery(rawQuery)
	if err != nil {
		return
	}

	query = make(map[string]interface{})

	for k, vs := range values {
		if typ, ok := argumentsType[k]; ok {
			value, err := jason.NewValueFromBytes([]byte(vs[0]))
			if err != nil {
				return nil, err
			}

			arg, err := decodeArgumentFromJsonValue(value, typ)
			if err != nil {
				return nil, err
			}

			query[k] = arg
		}
	}

	return
}

func decodeArgumentFromJsonValue(value *jason.Value, typ ArgumentType) (arg interface{}, err error) {
	if null := value.Null(); null == nil {
		arg = nil
	} else {
		switch typ.Name {
		case "boolean":
			arg, err = value.Boolean()
		case "boolean[]":
			arg, err = valueToBoolArray(value)
		case "smallint", "integer", "bigint":
			arg, err = value.Int64()
		case "smallint[]":
			arg, err = valueToInt16Array(value)
		case "integer[]":
			arg, err = valueToInt32Array(value)
		case "bigint[]":
			arg, err = valueToInt64Array(value)
		case "real", "double precision":
			arg, err = value.Float64()
		case "real[]":
			arg, err = valueToFloat32Array(value)
		case "double precision[]":
			arg, err = valueToFloat64Array(value)
		case "numeric", "money":
			arg, err = valueToNumeric(value)
		case "numeric[]", "money[]":
			arg, err = valueToNumericArray(value)
		case "character", "character varying", "text", "uuid", "date", "time without time zone", "time with time zone":
			arg, err = value.String()
		case "character[]", "character varying[]", "text[]", "uuid[]", "date[]", "time without time zone[]", "time with time zone[]":
			arg, err = valueToStringArray(value)
		case "timestamp without time zone", "timestamp with time zone":
			arg, err = valueToTime(value)
		case "timestamp without time zone[]", "timestamp with time zone[]":
			arg, err = valueToTimeArray(value)
		case "bytea":
			arg, err = valueToBytea(value)
		case "bytea[]":
			arg, err = valueToByteaArray(value)
		case "hstore":
			arg, err = valueToHstore(value)
		case "hstore[]":
			arg, err = valueToHstoreArray(value)
		default:
			arg, err = valueFallback(value)
		}
	}

	return
}

func valueFallback(value *jason.Value) (res string, err error) {
	var ok error
	res, ok = value.String()
	if ok != nil {
		var raw []byte
		raw, err = value.Marshal()
		if err == nil {
			// string with JSON content
			res = string(raw)
		}
	}

	return
}

func valueToNumeric(value *jason.Value) (res pgtype.Numeric, err error) {
	if n, err := value.Number(); err == nil {
		i64, convErr := n.Int64()
		if convErr != nil {
			err = errors.New(fmt.Sprintf("Invalid JSON value for numeric data type: %s", n.String()))
		} else {
			res = pgtype.Numeric{
				Int:    big.NewInt(i64),
				Status: pgtype.Present,
			}
		}
	} else {
		var s string
		s, err = value.String()
		if err != nil {
			err = errors.New("JSON number or string expected.")
		} else {
			res.Set(s)
		}
	}

	return
}

func valueToTime(value *jason.Value) (res time.Time, err error) {
	var s string
	s, err = value.String()
	if err != nil {
		err = errors.New("JSON string expected.")
	} else {
		res, err = time.Parse(time.RFC3339, s)
	}

	return
}

func valueToBytea(value *jason.Value) (res pgtype.Bytea, err error) {
	var s string
	s, err = value.String()
	if err != nil {
		err = errors.New("JSON string expected.")
	} else {
		var bytes []byte
		bytes, err = base64.StdEncoding.DecodeString(s)
		if err == nil {
			res = pgtype.Bytea{
				Bytes:  bytes,
				Status: pgtype.Present,
			}
		}
	}

	return
}

func valueToHstore(value *jason.Value) (hstore pgtype.Hstore, err error) {
	if object, ok := value.Object(); ok == nil {
		hstore.Map = make(map[string]pgtype.Text)
		for key, value := range object.Map() {
			var ns pgtype.Text

			if value.Null() == nil {

			} else {
				ns.String, err = value.String()
				if err != nil {
					err = nil
					var raw []byte
					raw, err = value.Marshal()
					if err != nil {
						return
					}

					ns.String = string(raw)
				}

				ns.Status = pgtype.Present
			}

			hstore.Map[key] = ns
		}

		hstore.Status = pgtype.Present
	} else {
		err = errors.New("JSON object expected.")
	}

	return
}

func valueToBoolArray(value *jason.Value) (array []bool, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]bool, 0, len(values))
		for _, subValue := range values {
			var converted bool
			converted, err = subValue.Boolean()
			if err != nil {
				array = nil
				return
			}
			array = append(array, converted)
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToInt16Array(value *jason.Value) (array []int16, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]int16, 0, len(values))
		for _, subValue := range values {
			var converted int64
			converted, err = subValue.Int64()
			if err != nil {
				array = nil
				return
			}
			array = append(array, int16(converted))
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToInt32Array(value *jason.Value) (array []int32, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]int32, 0, len(values))
		for _, subValue := range values {
			var converted int64
			converted, err = subValue.Int64()
			if err != nil {
				array = nil
				return
			}
			array = append(array, int32(converted))
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToInt64Array(value *jason.Value) (array []int64, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]int64, 0, len(values))
		for _, subValue := range values {
			var converted int64
			converted, err = subValue.Int64()
			if err != nil {
				array = nil
				return
			}
			array = append(array, converted)
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToFloat32Array(value *jason.Value) (array []float32, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]float32, 0, len(values))
		for _, subValue := range values {
			var converted float64
			converted, err = subValue.Float64()
			if err != nil {
				array = nil
				return
			}
			array = append(array, float32(converted))
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToFloat64Array(value *jason.Value) (array []float64, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]float64, 0, len(values))
		for _, subValue := range values {
			var converted float64
			converted, err = subValue.Float64()
			if err != nil {
				array = nil
				return
			}
			array = append(array, converted)
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToNumericArray(value *jason.Value) (array pgtype.NumericArray, err error) {
	if values, ok := value.Array(); ok == nil {
		elements := make([]pgtype.Numeric, 0, len(values))
		for _, subValue := range values {
			var converted pgtype.Numeric
			converted, err = valueToNumeric(subValue)
			if err != nil {
				elements = nil
				return
			}
			elements = append(elements, converted)
		}

		array = pgtype.NumericArray{
			Elements:   elements,
			Dimensions: []pgtype.ArrayDimension{{Length: int32(len(elements)), LowerBound: 1}},
			Status:     pgtype.Present,
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToStringArray(value *jason.Value) (array pgtype.TextArray, err error) {
	if values, ok := value.Array(); ok == nil {
		strings := make([]pgtype.Text, 0, len(values))
		for _, subValue := range values {
			var converted string
			converted, err = subValue.String()
			if err != nil {
				return
			}
			strings = append(strings, pgtype.Text{
				String: converted,
				Status: pgtype.Present,
			})
		}

		array = pgtype.TextArray{
			Elements:   strings,
			Dimensions: []pgtype.ArrayDimension{{Length: int32(len(strings)), LowerBound: 1}},
			Status:     pgtype.Present,
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToArray(value *jason.Value, converter func(*jason.Value) (interface{}, error)) (array []interface{}, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]interface{}, 0, len(values))
		for _, subValue := range values {
			var converted interface{}
			converted, err = converter(subValue)
			if err != nil {
				array = nil
				return
			}
			array = append(array, converted)
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToTimeArray(value *jason.Value) (array []time.Time, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]time.Time, 0, len(values))
		for _, subValue := range values {
			var converted time.Time
			converted, err = valueToTime(subValue)
			if err != nil {
				array = nil
				return
			}
			array = append(array, converted)
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToByteaArray(value *jason.Value) (array pgtype.ByteaArray, err error) {
	if values, ok := value.Array(); ok == nil {
		elements := make([]pgtype.Bytea, 0, len(values))
		for _, subValue := range values {
			var converted pgtype.Bytea
			converted, err = valueToBytea(subValue)
			if err != nil {
				elements = nil
				return
			}
			elements = append(elements, converted)
		}

		array = pgtype.ByteaArray{
			Elements:   elements,
			Dimensions: []pgtype.ArrayDimension{{Length: int32(len(elements)), LowerBound: 1}},
			Status:     pgtype.Present,
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}

func valueToHstoreArray(value *jason.Value) (array pgtype.HstoreArray, err error) {
	if values, ok := value.Array(); ok == nil {
		elements := make([]pgtype.Hstore, 0, len(values))
		for _, subValue := range values {
			var converted pgtype.Hstore
			converted, err = valueToHstore(subValue)
			if err != nil {
				elements = nil
				return
			}
			elements = append(elements, converted)
		}

		array = pgtype.HstoreArray{
			Elements:   elements,
			Dimensions: []pgtype.ArrayDimension{{Length: int32(len(elements)), LowerBound: 1}},
			Status:     pgtype.Present,
		}
	} else {
		err = errors.New("JSON array expected.")
	}

	return
}
