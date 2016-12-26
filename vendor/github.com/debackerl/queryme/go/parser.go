package queryme

import (
	"fmt"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

/*
predicates    = predicate *("," predicate)
predicate     = (not / and / or / eq / lt / le / gt / ge)
not           = "not" "(" predicate ")"
and           = "and" "(" predicates ")"
or            = "or" "(" predicates ")"
eq            = "eq" "(" field "," values ")"
lt            = "lt" "(" field "," value ")"
le            = "le" "(" field "," value ")"
gt            = "gt" "(" field "," value ")"
ge            = "ge" "(" field "," value ")"
fts           = "fts" "(" field "," string ")"

values        = value *("," value)
value         = (null / boolean / number / string / date)
null          = "null"
boolean       = "true" / "false"
number        = 1*(DIGIT / "." / "e" / "E" / "+" / "-")
string        = "$" *(unreserved / pct-encoded)
date          = 4DIGIT "-" 2DIGIT "-" 2DIGIT *1("T" 2DIGIT ":" 2DIGIT ":" 2DIGIT *1("." 3DIGIT) "Z")

fieldorders   = *1(fieldorder *("," fieldorder))
fieldorder    = *1"!" field
field         = *(unreserved / pct-encoded)

unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
pct-encoded   = "%" HEXDIG HEXDIG
sub-delims    = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
query         = *( pchar / "/" / "?" )
*/

var (
	SortOrderSeparatorExpected error = errors.New("Expected seperator ',' after sorted order.")
	IdentifierExpected error = errors.New("Expected identifier.")
	ValueExpected error = errors.New("Expected value.")
	EndOfStringExpected error = errors.New("Expected end of string.")
	StringExpected error = errors.New("Expected string.")
	OperatorExpected error = errors.New("Expected operator.")
	UnexpectedEndOfPredicate error = errors.New("Unexpected end of predicate.")
	UnexpectedEndOfSortOrders error = errors.New("Unexpected end of sort orders.")

	characters []byte
)

func init() {
	characters = make([]byte, 128)

	characters[int('=')] = 1
	characters[int('&')] = 1

	characters[int('!')] = 2
	characters[int('\'')] = 2
	characters[int('(')] = 2
	characters[int(')')] = 2
	characters[int('*')] = 2
	characters[int(',')] = 2
	characters[int(';')] = 2
	characters[int('/')] = 2
	characters[int('?')] = 2
	characters[int('@')] = 2

	characters[int('$')] = 3
	characters[int('+')] = 3
	characters[int(':')] = 3

	// 'pct-encoded' characters
	characters[int('%')] = 4

	// 'unreserved' characters
	characters[int('-')] = 5
	characters[int('.')] = 5
	characters[int('_')] = 5
	characters[int('~')] = 5

	for i := int('0'); i <= int('9'); i++ {
		characters[i] = 5
	}

	for i := int('a'); i <= int('z'); i++ {
		characters[i] = 5
	}

	for i := int('A'); i <= int('Z'); i++ {
		characters[i] = 5
	}
}

func firstCharClass(s string) byte {
	r, _ := utf8.DecodeRuneInString(s)
	if r > 127 {
		return 0
	} else {
		return characters[r]
	}
}

func charClassDetector(min byte, max byte) func(r rune) bool {
	return func(r rune) bool {
		i := int(r)
		if i > 127 {
			return false
		}
		c := characters[i]
		return c >= min && c <= max
	}
}

// QueryString is a parsed query part of a URL.
type QueryString struct {
	fields map[string]string
}

// NewFromRawQuery creates a new QueryString from a raw query string.
func NewFromRawQuery(rawQuery string) *QueryString {
	qs := new(QueryString)
	qs.fields = make(map[string]string)

	for {
		i := strings.IndexRune(rawQuery, '=')
		if i == -1 {
			break
		}
		name := rawQuery[:i]
		rawQuery = rawQuery[i+1:]

		i = strings.IndexFunc(rawQuery, charClassDetector(1, 1))
		var value string
		if i == -1 {
			value = rawQuery
		} else {
			value = rawQuery[:i]
			rawQuery = rawQuery[i+1:]
		}

		qs.fields[name] = value

		if i == -1 {
			break
		}
	}

	return qs
}

// NewFromRawQuery creates a new QueryString from an existing URL object.
func NewFromURL(url *url.URL) *QueryString {
	return NewFromRawQuery(url.RawQuery)
}

// Tests if specified name has been found in query string.
func (q *QueryString) Contains(name string) bool {
	_, ok := q.fields[name]
	return ok
}

// Returns raw query string value.
func (q *QueryString) Raw(name string) (string, bool) {
	v, ok := q.fields[name]
	return v, ok
}

// Predicate parses the given component of the query as a predicate, then returns it.
func (q *QueryString) Predicate(name string) (p Predicate, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = rec.(error)
		}
	}()

	raw, ok := q.fields[name]
	if !ok {
		return nil, fmt.Errorf("field not found: %q", name)
	}

	p, raw = parsePredicate(raw)
	if len(raw) != 0 {
		p = nil
		err = UnexpectedEndOfPredicate
	}

	return
}

// Predicate parses the given component of the query as a sort order, then returns it.
func (q *QueryString) SortOrder(name string) (os []*SortOrder, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = rec.(error)
		}
	}()

	raw, ok := q.fields[name]
	if !ok {
		return nil, fmt.Errorf("field not found: %q", name)
	}

	os, raw = parseSortOrders(raw)
	if len(raw) != 0 {
		os = nil
		err = UnexpectedEndOfSortOrders
	}

	return
}

func parsePredicate(s string) (p Predicate, n string) {
	if len(s) == 0 {
		panic(OperatorExpected)
	}

	var op string
	op, n = parseIdentifier(s)

	n = parseLiteral(n, "(")

	var f string
	var ps []Predicate
	var vs []Value
	var v Value

	switch op {
		case "not":
			var operand Predicate
			operand, n = parsePredicate(n)
			p = Not{operand}
		case "and":
			ps, n = parsePredicates(n)
			p = And(ps)
		case "or":
			ps, n = parsePredicates(n)
			p = Or(ps)
		case "eq":
			f, n = parseIdentifier(n)
			n = parseLiteral(n, ",")
			vs, n = parseValues(n)
			p = Eq{Field(f), vs}
		case "gt":
			f, n = parseIdentifier(n)
			n = parseLiteral(n, ",")
			v, n = parseValue(n)
			p = Gt{Field(f), v}
		case "ge":
			f, n = parseIdentifier(n)
			n = parseLiteral(n, ",")
			v, n = parseValue(n)
			p = Ge{Field(f), v}
		case "lt":
			f, n = parseIdentifier(n)
			n = parseLiteral(n, ",")
			v, n = parseValue(n)
			p = Lt{Field(f), v}
		case "le":
			f, n = parseIdentifier(n)
			n = parseLiteral(n, ",")
			v, n = parseValue(n)
			p = Le{Field(f), v}
		case "fts":
			f, n = parseIdentifier(n)
			n = parseLiteral(n, ",")
			s, n = parseString(n)
			p = Fts{Field(f), s}
		default:
			panic(fmt.Errorf("Invalid operator: %q", op))
	}

	n = parseLiteral(n, ")")

	return
}

func parsePredicates(s string) (ps []Predicate, n string) {
	ps = make([]Predicate, 0, 4)

	if len(s) > 0 && firstCharClass(s) > 2 {
		n = s
		for {
			var operand Predicate
			operand, n = parsePredicate(n)
			ps = append(ps, operand)

			if len(n) > 0 && n[0] == ',' {
				n = n[1:]
			} else {
				break
			}
		}
	}

	return
}

func ParseValues(s string) ([]Value, error) {
	vs, n := parseValues(s)
	if n != "" {
		return vs, EndOfStringExpected
	}
	return vs, nil
}

func parseValues(s string) (vs []Value, n string) {
	vs = make([]Value, 0, 4)

	if len(s) > 0 && firstCharClass(s) > 2 {
		n = s
		for {
			var operand interface{}
			operand, n = parseValue(n)
			vs = append(vs, operand)

			if len(n) > 0 && n[0] == ',' {
				n = n[1:]
			} else {
				break
			}
		}
	}

	return
}

func parseString(s string) (v string, n string) {
	if len(s) == 0 || s[0] != '$' {
		panic(StringExpected)
	}

	s = s[1:]

	l := strings.IndexFunc(s, charClassDetector(1, 2))

	if l == -1 {
		l = len(s)
	}

	var err error
	if v, err = url.QueryUnescape(s[:l]); err != nil {
		panic(err)
	}

	n = s[l:]
	return
}

func ParseValue(s string) (Value, error) {
	v, n := parseValue(s)
	if n != "" {
		return v, EndOfStringExpected
	}
	return v, nil
}

func parseValue(s string) (v Value, n string) {
	if len(s) == 0 {
		panic(ValueExpected)
	}

	r, l := utf8.DecodeRuneInString(s)

	switch(r) {
		case 'n':
			n = parseLiteral(s, "null")
			v = nil
		case 't':
			n = parseLiteral(s, "true")
			v = true
		case 'f':
			n = parseLiteral(s, "false")
			v = false
		case '$':
			v, n = parseString(s)
		default:
			if l = strings.IndexFunc(s, charClassDetector(1, 2)); l == -1 {
				l = len(s)
			}

			if (l == 10 || ((l == 20 || (l == 24 && s[19] == '.')) && s[10] == 'T' && s[13] == ':' && s[16] == ':' && s[l-1] == 'Z')) && s[4] == '-' && s[7] == '-' {
				var err error
				var yr, mo, dy, hr, mn, sc, ms int64 = 0, 0, 0, 0, 0, 0, 0

				if yr, err = strconv.ParseInt(s[0:4], 10, 32); err != nil {
					panic(err)
				}
				if mo, err = strconv.ParseInt(s[5:7], 10, 32); err != nil {
					panic(err)
				}
				if dy, err = strconv.ParseInt(s[8:10], 10, 32); err != nil {
					panic(err)
				}

				if l >= 20 {
					if hr, err = strconv.ParseInt(s[11:13], 10, 32); err != nil {
						panic(err)
					}
					if mn, err = strconv.ParseInt(s[14:16], 10, 32); err != nil {
						panic(err)
					}
					if sc, err = strconv.ParseInt(s[17:19], 10, 32); err != nil {
						panic(err)
					}

					if l == 24 {
						if ms, err = strconv.ParseInt(s[20:23], 10, 32); err != nil {
							panic(err)
						}
					}
				}

				v = time.Date(int(yr), time.Month(mo), int(dy), int(hr), int(mn), int(sc), int(ms) * 1000000, time.UTC)
			} else {
				if f, err := strconv.ParseFloat(s[:l], 64); err != nil {
					panic(err)
				} else {
					v = f
				}
			}

			n = s[l:]
	}

	return
}

func parseLiteral(s string, expected string) (n string) {
	if len(s) < len(expected) || s[:len(expected)] != expected {
		panic(fmt.Errorf("expected: %q", expected))
	}

	return s[len(expected):]
}

func parseSortOrders(s string) (os []*SortOrder, n string) {
	os = make([]*SortOrder, 0, 4)

	if len(s) > 0 {
		for {
			var o *SortOrder
			o, s = parseSortOrder(s)
			os = append(os, o)

			if len(s) == 0 {
				break
			}

			if r, l := utf8.DecodeRuneInString(s); r != ',' {
				panic(SortOrderSeparatorExpected)
			} else {
				s = s[l:]
			}
		}
	}

	n = s
	return
}

func parseSortOrder(s string) (o *SortOrder, n string) {
	o = new(SortOrder)

	if r, _ := utf8.DecodeRuneInString(s); r == '!' {
		s = s[1:]
	} else {
		o.Ascending = true
	}

	f, n := parseIdentifier(s)
	o.Field = Field(f)
	return
}

func ParseIdentifier(s string) (Value, error) {
	v, n := parseIdentifier(s)
	if n != "" {
		return v, EndOfStringExpected
	}
	return v, nil
}

func parseIdentifier(s string) (id string, n string) {
	if len(s) == 0 {
		panic(IdentifierExpected)
	}

	i := strings.IndexFunc(s, charClassDetector(1, 3))

	if i == 0 {
		panic(IdentifierExpected)
	}

	if i == -1 {
		n = ""
	} else {
		n = s[i:]
		s = s[:i]
	}

	var err error
	if id, err = url.QueryUnescape(s); err != nil {
		panic(err)
	}

	return
}
