package main

import (
	"github.com/debackerl/queryme/go"
	"strconv"
	"strings"
)

// PredicateToSql converts a Predicate to its equivalent SQL form and extracts constant to a seperate array. Each referenced field is matched against a list of allowed fields.
func PredicateToPostgreSql(sql *SqlBuilder, ftsFunction string, argumentsType map[string]string, predicate queryme.Predicate) {
	visitor := predicateSqlizer { Sql: sql, FtsFunction: ftsFunction, ArgumentsType: argumentsType }

	predicate.Accept(&visitor)
}

// SortOrderToSql converts an array of SortOrder to its equivalent SQL form. Each referenced field is matched against a list of allowed fields.
func SortOrderToPostgreSql(sql *SqlBuilder, sortOrder []*queryme.SortOrder) {
	for i, so := range sortOrder {
		if i > 0 {
			sql.WriteSql(",")
		}

		sql.WriteId(string(so.Field))

		if !so.Ascending {
			sql.WriteSql(" DESC")
		}
	}
}

type predicateSqlizer struct {
	Sql *SqlBuilder
	FtsFunction string
	ArgumentsType map[string]string
}

func (s *predicateSqlizer) isFieldArray(field string) bool {
	if typ, ok := s.ArgumentsType[field]; ok {
		return strings.HasSuffix(typ, "[]")
	}
	return false
}

func (s *predicateSqlizer) AppendSql(sql string) {
	s.Sql.WriteSql(sql)
}

func (s *predicateSqlizer) AppendId(id string) {
	s.Sql.WriteId(id)
}

func (s *predicateSqlizer) AppendValue(field string, value interface{}) {
	if typ, ok := s.ArgumentsType[field]; ok {
		switch typ {
		case "smallint", "integer", "bigint", "smallint[]", "integer[]", "bigint[]":
			if f, ok := value.(float64); ok {
				value = int64(f)
			}
		case "numeric", "money", "numeric[]", "money[]":
			if f, ok := value.(float64); ok {
				value = strconv.FormatFloat(f, 'g', -1, 64)
			}
		}
	}
	
	s.Sql.WriteValue(value)
}

func (s *predicateSqlizer) VisitNot(operand queryme.Predicate) {
	s.AppendSql("(NOT ")
	operand.Accept(s)
	s.AppendSql(")")
}

func (s *predicateSqlizer) VisitPredicates(sqlOperator string, defaultValue string, operands []queryme.Predicate) {
	s.AppendSql("(")
	if len(operands) > 0 {
		for i, p := range operands {
			if i > 0 {
				s.AppendSql(" ")
				s.AppendSql(sqlOperator)
				s.AppendSql(" ")
			}
			p.Accept(s)
		}
	} else {
		s.AppendSql(defaultValue)
	}
	s.AppendSql(")")
}

func (s *predicateSqlizer) VisitAnd(operands []queryme.Predicate) {
	s.VisitPredicates("AND", "true", operands)
}

func (s *predicateSqlizer) VisitOr(operands []queryme.Predicate) {
	s.VisitPredicates("OR", "false", operands)
}

func (s *predicateSqlizer) VisitEq(field queryme.Field, operands []queryme.Value) {
	f := string(field)
	
	switch len(operands) {
	case 0:
		s.AppendSql("false")
	case 1:
		s.AppendId(f)
		
		if s.isFieldArray(f) {
			s.AppendSql("@>ARRAY[")
			s.AppendValue(f, operands[0])
			s.AppendSql("]::")
			typ, _ := s.ArgumentsType[f]
			s.AppendSql(typ)
		} else if operands[0] == nil {
			s.AppendSql(" IS NULL")
		} else {
			s.AppendSql("=")
			s.AppendValue(f, operands[0])
		}
	default:
		s.AppendId(f)
		if s.isFieldArray(f) {
			s.AppendSql("&&ARRAY[")
			for i, op := range operands {
				if i > 0 {
					s.AppendSql(",")
				}
				s.AppendValue(f, op)
			}
			s.AppendSql("]::")
			typ, _ := s.ArgumentsType[f]
			s.AppendSql(typ)
		} else {
			seen := 0
			test_null := false
			
			for _, op := range operands {
				if op == nil {
					test_null = true
				} else {
					if seen == 0 {
						s.AppendSql(" IN (")
					} else {
						s.AppendSql(",")
					}
					s.AppendValue(f, op)
					seen++
				}
			}
			if seen > 0 {
				s.AppendSql(")")
			}
			
			if test_null {
				s.AppendSql(" OR ")
				s.AppendId(f)
				s.AppendSql(" IS NULL")
			}
		}
	}
}

func (s *predicateSqlizer) VisitLt(field queryme.Field, operand queryme.Value) {
	f := string(field)
	s.AppendId(f)
	s.AppendSql("<")
	s.AppendValue(f, operand)
}

func (s *predicateSqlizer) VisitLe(field queryme.Field, operand queryme.Value) {
	f := string(field)
	s.AppendId(f)
	s.AppendSql("<=")
	s.AppendValue(f, operand)
}

func (s *predicateSqlizer) VisitGt(field queryme.Field, operand queryme.Value) {
	f := string(field)
	s.AppendId(f)
	s.AppendSql(">")
	s.AppendValue(f, operand)
}

func (s *predicateSqlizer) VisitGe(field queryme.Field, operand queryme.Value) {
	f := string(field)
	s.AppendId(f)
	s.AppendSql(">=")
	s.AppendValue(f, operand)
}

func (s *predicateSqlizer) VisitFts(field queryme.Field, query string) {
	f := string(field)
	s.AppendId(f)
	s.AppendSql(" @@ ")
	s.AppendId(s.FtsFunction)
	s.AppendSql("(")
	s.AppendValue(f, query)
	s.AppendSql(")")
}
