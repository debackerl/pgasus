package main

import (
	"bytes"
	"strconv"
	"strings"
)

type SqlBuilder struct {
	query bytes.Buffer
	values []interface{}
}

func NewSqlBuilder() *SqlBuilder {
	return &SqlBuilder {
		values: make([]interface{}, 0, 4),
	}
}

func (b *SqlBuilder) Sql() string {
	return b.query.String()
}

func (b *SqlBuilder) Values() []interface{} {
	return b.values
}

func (b *SqlBuilder) WriteSql(sql string) {
	b.query.WriteString(sql)
}

func (b *SqlBuilder) WriteId(id string) {
	b.query.WriteString(quoteIdentifier(id))
}

func (b *SqlBuilder) WriteValue(value interface{}) {
	b.values = append(b.values, value)
	b.query.WriteString("$" + strconv.Itoa(len(b.values)))
}

func quoteIdentifier(name string) string {
	if end := strings.IndexRune(name, 0); end > -1 {
		name = name[:end]
	}
	
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}
