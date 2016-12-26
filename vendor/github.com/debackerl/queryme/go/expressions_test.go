package queryme

import (
	"testing"
	"github.com/bmizerany/assert"
)

func TestFields(t *testing.T) {
	p := And{
		Not{Or{
			Lt{"foo", "bob"},
			Eq{"bar", []Value{true}}}},
		Fts{"foo", "go library"}}

	fields := Fields(p)

	assert.Equal(t, []Field{"foo", "bar"}, fields)
}
