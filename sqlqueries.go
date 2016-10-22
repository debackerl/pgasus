package main

import (
	"errors"
	"github.com/debackerl/queryme/go"
)

func buildSelectSqlQuery(sql *SqlBuilder, ftsFunction string, argumentsType map[string]string, columns string, relation string, filter queryme.Predicate, order []*queryme.SortOrder, limit int64) error {
	sql.WriteSql("SELECT ")
	sql.WriteSql(columns)
	sql.WriteSql(" FROM ")
	sql.WriteId(relation)
	
	if filter != nil {
		sql.WriteSql(" WHERE ")
		PredicateToPostgreSql(sql, ftsFunction, argumentsType, filter)
	}
	
	if len(order) > 0 {
		sql.WriteSql(" ORDER BY ")
		SortOrderToPostgreSql(sql, order)
	}
	
	if limit > 0 {
		sql.WriteSql(" LIMIT ")
		sql.WriteValue(limit)
	}
	
	return nil
}

func buildInsertSqlQuery(sql *SqlBuilder, ftsFunction string, argumentsType map[string]string, columns string, relation string, query map[string]interface{}) error {
	sql.WriteSql("INSERT INTO ")
	sql.WriteId(relation)
	
	sql.WriteSql(" (")
	
	i := 0
	
	for name := range query {
		if i > 0 {
			sql.WriteSql(",")
		}
		
		sql.WriteId(name)
		
		i++
	}
	
	sql.WriteSql(") VALUES (")
	
	i = 0
	
	for _, value := range query {
		if i > 0 {
			sql.WriteSql(",")
		}
		
		sql.WriteValue(value)
		
		i++
	}
	
	sql.WriteSql(" RETURNING ")
	sql.WriteSql(columns)
	
	return nil
}

func buildUpdateSqlQuery(sql *SqlBuilder, ftsFunction string, argumentsType map[string]string, relation string, filter queryme.Predicate, query map[string]interface{}) error {
	sql.WriteSql("UPDATE ")
	sql.WriteId(relation)
	
	sql.WriteSql(" SET ")
	
	i := 0
	
	for name, value := range query {
		if i > 0 {
			sql.WriteSql(",")
		}
		
		sql.WriteId(name)
		
		sql.WriteSql(" = ")
		
		sql.WriteValue(value)
		
		i++
	}
	
	if filter != nil {
		sql.WriteSql(" WHERE ")
		PredicateToPostgreSql(sql, ftsFunction, argumentsType, filter)
	}
	
	return nil
}

func buildDeleteSqlQuery(sql *SqlBuilder, ftsFunction string, argumentsType map[string]string, relation string, filter queryme.Predicate) error {
	sql.WriteSql("DELETE FROM ")
	sql.WriteId(relation)
	
	if filter != nil {
		sql.WriteSql(" WHERE ")
		PredicateToPostgreSql(sql, ftsFunction, argumentsType, filter)
	}
	
	return nil
}

func buildProcedureSqlQuery(sql *SqlBuilder, procedure string, proretset bool, jsonize bool, query map[string]interface{}) error {
	if proretset {
		if jsonize {
			return errors.New("No need to jsonize a result set.")
		} else {
			sql.WriteSql("SELECT * FROM ")
		}
	} else if jsonize {
		sql.WriteSql("SELECT row_to_json(")
	} else {
		sql.WriteSql("SELECT ")
	}
	
	sql.WriteId(procedure)
	sql.WriteSql("(")
	
	i := 0
	
	for name, value := range query {
		if i > 0 {
			sql.WriteSql(",")
		}
		
		sql.WriteId(name)
		
		sql.WriteSql(" := ")
		
		sql.WriteValue(value)
		
		i++
	}
	
	sql.WriteSql(")")
	
	if jsonize {
		sql.WriteSql(")")
	}
	
	return nil
}
