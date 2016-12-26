Structured language to embed complex queries in the query part of a URL.

**This API is still unstable**, but many unit tests have been written already.

You can read the documention of the Go library online: http://godoc.org/github.com/debackerl/queryme/go

Continuous integration: [![Build Status](https://drone.io/github.com/debackerl/queryme/status.png)](https://drone.io/github.com/debackerl/queryme/latest)

## Example

The following example JavaScript code:

```JavaScript
var filter = QM.And(QM.Not(QM.Eq("type",[QM.String("image"),QM.String("video")])),QM.Fts("content","open source"));
var sort = QM.Sort(QM.Order("relevance"),QM.Order("date",false));
window.location.search = "?f=" + filter + "&s=" + sort;
```

will generate the this query string

```
?f=and(not(eq(type,$image,$video)),fts(content,$open source))&s=relevance,!date
```

Once received by the server, the go library's ToSql function can check for any disallowed fields, and generate SQL and extract constants:

```go
qs := NewFromURL(url)
allowed_fields := []Field{"type","content","id","author"}
sqlWhere, values := PredicateToSql(qs.Predicate("f"), allowed_fields)
sqlOrderBy := SortOrderToSql(qs.SortOrder("s"), allowed_fields)
fmt.Println(sqlWhere)
fmt.Println(values)
fmt.Println(sqlOrderBy)
```

will print the following

```
((NOT `type` IN (?,?)) AND MATCH (`content`) AGAINST (?))
[]interface{}{"image", "video", "open source"}
relevance, date DESC
```

## Data Types

The query language support all JSON data types with the addition of dates:

* null value
* booleans
* numbers (double-precision and exact integers with up to 15 digits)
* strings
* dates (up to millisecond precision)

## Predicates

*queryme* supports all the basic predicates:

Name | Description
---- | --------------------------------------
not  | negation
and  | conjunction
or   | disjunction
eq   | equality check with one or more values
gt   | stricly greater
ge   | greater or equal
lt   | stricly less
le   | less or equal
fts  | full-text search

## Formal specification

```
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
```
