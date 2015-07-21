package main

import (
	"github.com/antonholmquist/jason"
	"github.com/jackc/pgx"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"time"
)

func decodeHttpBody(w http.ResponseWriter, r *http.Request, argumentsType map[string]string, readonlyFields map[string]struct{}, maxBodySizeKbytes int64) (queries []map[string]interface{}, batch bool, err error) {
	//r.Header.Get("Content-Type")
	
	queries = make([]map[string]interface{}, 0, 1)
	
	var value *jason.Value
	
	value, err = jason.NewValueFromReader(http.MaxBytesReader(w, r.Body, maxBodySizeKbytes))
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
	
	return
}

func prepareArgumentsFromObject(arguments *jason.Object, argumentsType map[string]string, readonlyFields map[string]struct{}) (query map[string]interface{}, err error) {
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

func prepareArgumentsFromQueryString(rawQuery string, argumentsType map[string]string) (query map[string]interface{}, err error) {
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
				panic(err)
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

func decodeArgumentFromJsonValue(value *jason.Value, typ string) (arg interface{}, err error) {
	if null := value.Null(); null == nil {
		arg = nil
	} else {
		switch typ {
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
			arg, err = valueToNumber(value)
		case "numeric[]", "money[]":
			return nil, errors.New("numeric[] and money[] encoder not supported.")
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
			return nil, errors.New("hstore[] encoder not supported.")
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
		if err != nil {
			res = string(raw)
		}
	}
	
	return
}

func valueToNumber(value *jason.Value) (res string, err error) {
	if n, err := value.Number(); err == nil {
		res = n.String()
	} else {
		var s string
		s, err = value.String()
		if err != nil {
			err = errors.New("JSON number or string expected.")
		} else {
			res = s
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

func valueToBytea(value *jason.Value) (res []byte, err error) {
	var s string
	s, err = value.String()
	if err != nil {
		err = errors.New("JSON string expected.")
	} else {
		res, err = base64.StdEncoding.DecodeString(s)
	}
	
	return
}

func valueToHstore(value *jason.Value) (hstore pgx.NullHstore, err error) {
	if object, ok := value.Object(); ok == nil {
		hstore.Hstore = make(map[string]pgx.NullString)
		for key, value := range object.Map() {
			var ns pgx.NullString
			
			if value.Null() == nil {
				
			} else {
				var raw []byte
				raw, err = value.Marshal()
				if err != nil {
					return
				}
				
				ns.String = string(raw)
				ns.Valid = true
			}
			
			hstore.Hstore[key] = ns
		}
		
		hstore.Valid = true
	} else {
		err = errors.New("JSON object expected.")
	}
	
	return
}

func valueToBoolArray(value *jason.Value) (array []bool, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]bool, 0, 8)
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
		array = make([]int16, 0, 8)
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
		array = make([]int32, 0, 8)
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
		array = make([]int64, 0, 8)
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
		array = make([]float32, 0, 8)
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
		array = make([]float64, 0, 8)
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

func valueToStringArray(value *jason.Value) (array []string, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]string, 0, 8)
		for _, subValue := range values {
			var converted string
			converted, err = subValue.String()
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

func valueToArray(value *jason.Value, converter func(*jason.Value) (interface{}, error)) (array []interface{}, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make([]interface{}, 0, 8)
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
		array = make([]time.Time, 0, 8)
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

func valueToByteaArray(value *jason.Value) (array ByteaArray, err error) {
	if values, ok := value.Array(); ok == nil {
		array = make(ByteaArray, 0, 8)
		for _, subValue := range values {
			var converted []byte
			converted, err = valueToBytea(subValue)
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

type ByteaArray [][]byte

func (a ByteaArray) Encode(w *pgx.WriteBuf, oid pgx.Oid) error {
	if oid != 1001 {
		return errors.New("Invalid oid expected for bytea array.")
	}
	
	n := len(a)
	
	total := 0
	for _, v := range a {
		total += 4 + len(v)
	}
	
	writeBinaryArrayHeader(w, total, n, pgx.ByteaOid)
	
	for _, v := range a {
		w.WriteInt32(int32(len(v)))
		w.WriteBytes(v)
	}
	
	return nil
}

func (a ByteaArray) FormatCode() int16 {
	return pgx.BinaryFormatCode
}

func writeBinaryArrayHeader(w *pgx.WriteBuf, totalSize int, numElements int, elementOid pgx.Oid) {
	w.WriteInt32(int32(20 + totalSize))
	w.WriteInt32(1)
	w.WriteInt32(0)
	w.WriteInt32(int32(elementOid))
	w.WriteInt32(int32(numElements))
	w.WriteInt32(1)
}