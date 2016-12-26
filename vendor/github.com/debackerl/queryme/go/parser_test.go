package queryme

import (
	"fmt"
	"github.com/bmizerany/assert"
	"testing"
	"time"
	"net/url"
)

func TestPredicate(t *testing.T) {
	qs := NewFromRawQuery("f=ge(age,18)&o=name")

	p, err := qs.Predicate("f")
	assert.Equal(t, nil, err)
	assert.Equal(t, Ge{"age", 18.0}, p)

	p, err = qs.Predicate("a")
	assert.Equal(t, nil, p)
	assert.Equal(t, fmt.Errorf("field not found: %q", "a"), err)
}

func TestSortOrder(t *testing.T) {
	url, _ := url.Parse("?f=ge(age,18)&o=name")
	qs := NewFromURL(url)

	so, err := qs.SortOrder("o")
	assert.Equal(t, nil, err)
	assert.Equal(t, []*SortOrder{&SortOrder{"name", true}}, so)

	so, err = qs.SortOrder("a")
	assert.Equal(t, []*SortOrder(nil), so)
	assert.Equal(t, fmt.Errorf("field not found: %q", "a"), err)
}

func TestParsePredicate(t *testing.T) {
	p, n := parsePredicate("ge(age,18)")
	assert.Equal(t, Ge{"age", 18.0}, p)
	assert.Equal(t, "", n)

	p, n = parsePredicate("fts(title,$belgian chocolate),")
	assert.Equal(t, Fts{"title", "belgian chocolate"}, p)
	assert.Equal(t, ",", n)

	p, n = parsePredicate("eq(type,4,5),")
	assert.Equal(t, Eq{"type", []Value{4.0, 5.0}}, p)
	assert.Equal(t, ",", n)

	p, n = parsePredicate("not(fts(title,$belgian chocolate))")
	assert.Equal(t, Not{Fts{"title", "belgian chocolate"}}, p)
	assert.Equal(t, "", n)

	p, n = parsePredicate("and(ge(age,18),not(fts(title,$belgian chocolate)))")
	assert.Equal(t, And{Ge{"age", 18.0},Not{Fts{"title", "belgian chocolate"}}}, p)
	assert.Equal(t, "", n)

	p, n = parsePredicate("or(lt(date,2000-01-02))")
	assert.Equal(t, Or{Lt{"date", time.Date(2000, time.Month(1), 2, 0, 0, 0, 0, time.UTC)}}, p)
	assert.Equal(t, "", n)
}

func TestParseString(t *testing.T) {
	v, n := parseString("$,")
	assert.Equal(t, "", v)
	assert.Equal(t, ",", n)

	v, n = parseString("$ok")
	assert.Equal(t, "ok", v)
	assert.Equal(t, "", n)

	v, n = parseString("$%C3%A0")
	assert.Equal(t, "à", v)
	assert.Equal(t, "", n)
}

func TestParseValue(t *testing.T) {
	v, n := parseValue("null")
	assert.Equal(t, nil, v)
	assert.Equal(t, "", n)

	v, n = parseValue("null,")
	assert.Equal(t, nil, v)
	assert.Equal(t, ",", n)

	v, n = parseValue("true")
	assert.Equal(t, true, v)
	assert.Equal(t, "", n)

	v, n = parseValue("true,")
	assert.Equal(t, true, v)
	assert.Equal(t, ",", n)

	v, n = parseValue("false")
	assert.Equal(t, false, v)
	assert.Equal(t, "", n)

	v, n = parseValue("false,")
	assert.Equal(t, false, v)
	assert.Equal(t, ",", n)

	v, n = parseValue("$,")
	assert.Equal(t, "", v)
	assert.Equal(t, ",", n)

	v, n = parseValue("$ok")
	assert.Equal(t, "ok", v)
	assert.Equal(t, "", n)

	v, n = parseValue("$%C3%A0")
	assert.Equal(t, "à", v)
	assert.Equal(t, "", n)

	v, n = parseValue("2000-01-02")
	assert.Equal(t, time.Date(2000, time.Month(1), 2, 0, 0, 0, 0, time.UTC), v)
	assert.Equal(t, "", n)

	v, n = parseValue("2000-01-02T12:34:56Z,")
	assert.Equal(t, time.Date(2000, time.Month(1), 2, 12, 34, 56, 0, time.UTC), v)
	assert.Equal(t, ",", n)

	v, n = parseValue("2000-01-02T12:34:56.789Z,")
	assert.Equal(t, time.Date(2000, time.Month(1), 2, 12, 34, 56, 789000000, time.UTC), v)
	assert.Equal(t, ",", n)

	v, n = parseValue("45")
	assert.Equal(t, 45.0, v)
	assert.Equal(t, "", n)

	v, n = parseValue("4.5")
	assert.Equal(t, 4.5, v)
	assert.Equal(t, "", n)

	v, n = parseValue("-45e6,")
	assert.Equal(t, -45.0e6, v)
	assert.Equal(t, ",", n)
}

func TestParseSortOrders(t *testing.T) {
	os, n := parseSortOrders("")
	assert.Equal(t, []*SortOrder{}, os)
	assert.Equal(t, "", n)

	os, n = parseSortOrders("brol")
	assert.Equal(t, []*SortOrder{&SortOrder{"brol", true}}, os)
	assert.Equal(t, "", n)

	os, n = parseSortOrders("brol,!zorg")
	assert.Equal(t, []*SortOrder{&SortOrder{"brol", true},&SortOrder{"zorg", false}}, os)
	assert.Equal(t, "", n)

	assert.Panic(t, IdentifierExpected, func() {
		os, n = parseSortOrders("brol,!zorg,")
	})
}

func TestParseSortOrder(t *testing.T) {
	o, n := parseSortOrder("brol")
	assert.Equal(t, SortOrder{"brol", true}, *o)
	assert.Equal(t, "", n)

	o, n = parseSortOrder("!zorg,")
	assert.Equal(t, SortOrder{"zorg", false}, *o)
	assert.Equal(t, ",", n)

	assert.Panic(t, IdentifierExpected, func() {
		parseSortOrder("!")
	})

	assert.Panic(t, IdentifierExpected, func() {
		parseSortOrder(",")
	})
}

func TestParseIdentifier(t *testing.T) {
	id, n := parseIdentifier("brol")
	assert.Equal(t, "brol", id)
	assert.Equal(t, "", n)

	id, n = parseIdentifier("zorg,")
	assert.Equal(t, "zorg", id)
	assert.Equal(t, ",", n)

	assert.Panic(t, IdentifierExpected, func() {
		parseIdentifier("")
	})

	assert.Panic(t, IdentifierExpected, func() {
		parseIdentifier(",")
	})
}
