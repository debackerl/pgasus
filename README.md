# pgasus

## PostgreSQL API Server for Universal Stack

This software is of beta quality at the moment. Expect production quality by end of 2015.

### Introduction

pgasus offers RESTful interface for PostgreSQL.

Design rules:
* One URL per resource.
* Public information can be cached by server in front of pgasus to lower load on the database server.
* Supports thousands of client HTTP(S) connections.
* Use of connection pool to PostgreSQL to keep parsed functions and plans in sessions.
* Graceful shutdown waiting for requests to complete.
* Strict restrictions on HTTP requests/responses size, and timeout on database requests: pgasus lives in the wild!
* Offers access to relations (table, views) and procedures.

### Routes

A table stored in the database stores all routes made available by pgasus. The reason to store this in the database is to be able to synchronize deployment of new tables and functions, while updating routes in a single transaction. Here are the fields:
* HTTP method, get, post, put, delete
* URL, like /enterprises/:entref/pos, containing variables
* Object name, name of relation or procedure
* Object type, relation, procedure
* TTL, used for cache-control in HTTP response
* Public/Private, used for cache-control in HTTP response
* Hidden fields, fields hidden from result sets
* Read-Only fields, fields that can be returned but not saved via inserts/updates
* Constants, constant values set in middleware's context
* Context-mapped headers, HTTP header values set in middleware's context
* Context-mapped variables, route's parameters and cookie values set in middleware's context

When the routes table is updated, a trigger sends a notification to pgasus which reload routes automatically. If you change columns of a relation, or arguments of a procedure, you may want to reload routes as well.

### Relations

Four HTTP methods are available:
* GET: select, supports filters as "where" clause, and ordering as "order by" clause
* POST: insert
* PUT: update, supports filters as "where" clause
* DELETE: delete, supports filters as "where" clause

Definition of columns loaded from database for automatic conversion.

Use views when SQL joins are required or to transform output.

Response from pgasus depends on HTTP method:
* GET: result set as an array
* POST: all fields of new record, including auto-increments
* PUT: number of affected records
* DELETE: number of affected records

### Procedures

Four HTTP methods are supported for procedures:
* GET if database state is not modified, procedure must be immutable or stable.
* POST if database state will be altered.
* PUT if repeating calls will equal parameters results in the same database state.
* DELETE if resource has to be permanently erased.

Supports neither filters nor ordering.

Definition of arguments loaded from database for automatic conversion

Response depends on the result type of procedure:
* Array if procedure returns a result set
* Single value otherwise

### Making a request

#### Composing requests for relations

The where clause is composed of values found in:
* URL route variables, those are used as equality operators.
* URL route constants, those are also used as equality operators.
* URL query string, accordingly to the format specified by [queryme](https://github.com/debackerl/queryme)

For insers, and updates, the new values of columns must be specified in the HTTP body. See section below.

A simple URL may look like this:

``GET /customers.json?f=eq(city,456)&s=street,!streetnr&l=10``

/customers identifies the resource being accessed.

* *.json* is the output format, json or csv.
* *eq(city,456)* is the filter keeping customers living in city 456.
* *street,!streetnr* defines the ordering of result, by street name first, then by decreasing street number.
* *10* is the limit on number of records to be returned

#### Composing requests to procedures

The using a procedure, the order of parameter is not important. Also, optional parameters remains optional.

Values passed as parameters are found in:
* URL route variables.
* URL route constants.
* URL query string, where key is argument's name, and value is formatted in JSON.
* HTTP body. See section below.

#### HTTP Body

Not everything fits in a URL. A URL is used to identify and filter only.

HTTP bodies are encoded in JSON format.

The HTTP body is used by client side to send (large amount of) data. This can happen in three cases:

* POST and PUT to procedure: Body is a JSON object, which fields are arguments to be provided to procedure. If URL defines variables of equal names, URL variables have priority.
* POST on relation: Body is a JSON object, which fields are values of columns of new record being inserted.
* PUT on relation: Body is a JSON object, which fields are values of columns of records being updated.

#### Context

pgasus uses the notion of context when executing requests on the database. PostgreSQL's set_config function is used to this end.

All context variables will be put in the same namespace as specified in the configuration file to avoid conflicts with other parameters.

Four sources of information can be used to assign session variables:
* HTTP header values
* HTTP cookies
* URL route variables
* Constants as degined in URL route

#### Batch mode

POST on procedures and relations supports batch mode.

Batch mode is activated by sending a JSON array.

Each element of the array is a JSON object for procedure execution or record insertion.

CSV output format does not support batch mode as it is not recursive.

### Security

A CA certificate can be configured to validate client application certificate for TLS mutual authentication. In this case, client's common name is used as database user. This mean that accesses to database objects can be restricted on a per application basis.

Also, because pgasus has the notion of context, a session id could be passed in the HTTP header, and stored as a PostgreSQL configuration in the database session. That way, you can check session id against a table of active sessions, and verify permissions when accessing data (e.g. using row-level policies in PostgreSQL 9.5+).

### Self-defense

pgasus offers restriction on:
* HTTP header size
* HTTP body size
* HTTP response size
* Total connection count
* Excessive reads and writes durations on TCP sockets
* Excessive execution time of SQL requests
