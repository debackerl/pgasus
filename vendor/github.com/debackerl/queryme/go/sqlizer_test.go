package queryme

import (
	"fmt"
	"testing"
	"github.com/bmizerany/assert"
)

func TestPredicateToSql(t *testing.T) {
	p := And{
		Not{Or{
			Lt{"foo", "bob"},
			Eq{"bar", []Value{true}}}},
		Fts{"foo", "go library"}}

	sql, values := PredicateToSql(p, []Field{"foo", "bar"})
	assert.Equal(t, "((NOT (`foo`<? OR `bar`=?)) AND MATCH (`foo`) AGAINST (?))", sql)
	assert.Equal(t, []interface{}{"bob", true, "go library"}, values)

	assert.Panic(t, fmt.Errorf("unauthorized field accessed: %q", "bar"), func() {
		PredicateToSql(p, []Field{"foo"})
	})
}

func TestSortOrderToSql(t *testing.T) {
	o := []*SortOrder{&SortOrder{"name",true},&SortOrder{"subscriptionDate",false}}

	sql := SortOrderToSql(o, []Field{"name", "subscriptionDate"})
	assert.Equal(t, "`name`,`subscriptionDate` DESC", sql)

	assert.Panic(t, fmt.Errorf("unauthorized field accessed: %q", "subscriptionDate"), func() {
		SortOrderToSql(o, []Field{"name"})
	})
}
