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
* URL, like /enterprises/:entref/pos, containing variables. See [denco](https://github.com/naoina/denco) for format.
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

#### Composing requests for relations (tables and views)

The following values are used to compose the where clause of the generated SQL query:
* URL route constants, those are also used as equality operators.
* URL route variables, those are used as equality operators. Overrides constants.
* URL query string, accordingly to the format specified by [queryme](https://github.com/debackerl/queryme)

For inserts and updates, the new values of columns must be specified in the HTTP body. See section below.

URL must satisfy the following format:

`/ROUTE.FORMAT?f=FILTER&s=SORT&l=LIMIT`

* `ROUTE` is a path matching one of the URL route.
* `FORMAT` is the requested format for result.
* `FILTER` (optional) is the condition used for the where condition in the resulting SQL query, see [queryme](https://github.com/debackerl/queryme).
* `SORT` (optional) is the sorting order to be used in the SQL query, see [queryme](https://github.com/debackerl/queryme).
* `LIMIT` (optional) is the maximum number of records to read from the database.

A simple URL may look like this:

`/customers.json?f=eq(city,456)&s=street,!streetnr&l=10`

`/customers` identifies the resource being accessed.

* `.json` is the output format.
* `eq(city,456)` keeps customers living in city 456.
* `street,!streetnr` sorts by street name first, then by decreasing street number.
* `10` limits the result to 10 records.

#### Formats

Three build-in data formats can be used to generate the content of the HTTP response:
* `json` is the only format able to serialize any kind of result from the database.
* `csv` is UTF-8 encoded, and will not serialize arrays and other composite data types.
* `bin` is used to return result of a procedure as is. Text is UTF-8 encoded. Only scalar data types are supported.

In addition, the configuration file may define several `binary_formats` sections. Those are used when format isn't one of the build-in formats. Each section must define two fields:
* `extension` is the format as specified in the requested URL.
* `mime_type` is the corresponding MIME type to be specified in the HTTP response's header.

#### Composing requests to procedures

When calling a procedure, the order of parameter is not important. Also, optional parameters remains optional.

Values loaded for each parameter are loaded in the following order:
* URL route constants.
* URL route variables. Overrides constants.
* URL query string for GET and DELETE methods. Keys found in query string are argument names, and values are formatted in JSON.
* HTTP body for POST and PUT methods. See section below.

A simple URL may look like this:

`/tickets/create.json?kind="incident"&level=10&title="fire!"`

#### HTTP Body

Not everything fits in a URL. A URL is used to identify and filter only.

The HTTP body is used by client side to send (a large amount of) data. Data can be encoded in JSON (default), or using Postgres literals when the Content-Type of the request is set to `application/x-www-form-urlencoded`.

HTTP bodies are used in three cases:

* POST and PUT to procedure: fields sent are arguments to be provided to procedure. If URL defines variables of equal names, URL variables have priority.
* POST on relation: fields are values of columns of new record being inserted.
* PUT on relation: fields are values of columns of records being updated.

#### Context

pgasus uses the notion of context when executing requests on the database. PostgreSQL's set_config function is used to this end.

All context variables will be put in the same namespace as specified in the configuration file to avoid conflicts with other parameters.

The context is built in the following order:
* Map HTTP header values accordingly to route's `context_mapped_headers` setting. A special header, X-Accept-Extension, is initialized by pgasus with file extension as specified in requested URL.
* Load variables defined in route's `context_mapped_variables` setting. Looking first in route's variables if found, otherwise in cookies. Overrides header.

#### Batch mode

POST on procedures and relations supports batch mode.

Batch mode is activated by sending a JSON array.

Each element of the array is a JSON object for procedure execution or record insertion.

CSV output format does not support batch mode as it is not recursive.

### Security

A CA certificate can be configured to validate client application certificate for TLS mutual authentication. In this case, client's common name is used as database user. This mean that accesses to database objects can be restricted on a per application basis.

If HTTP client doesn't support mutual authentication, basic HTTP authentication can also be used in which case the provided password is matched against encrypted password stored in Postgres. This mode can only be used over a TLS connection to keep credential confidential.

Also, because pgasus has the notion of context, a session id could be passed in the HTTP header, and stored as a PostgreSQL configuration in the database session. That way, you can check session id against a table of active sessions, and verify permissions when accessing data (e.g. using row-level policies in PostgreSQL 9.5+).

### Self-defense

pgasus offers restriction on:
* HTTP header size
* HTTP body size
* HTTP response size
* Total connection count
* Excessive reads and writes durations on TCP sockets
* Excessive execution time of SQL requests

### Database design tips

* Use triggers to validate changes made to tables when using routes to relations.
* For multi-language web sites, put the language desired in the route. Then load this parameter in the database context using the `context_mapped_variables` setting of the routes table. Then you can create views on table to localise the data, or use this context variable in your procedure.
* If you use AJAX to connect to pgasus, load the user session id from a cookie by configuring the proper cookie name in the routes table. Then to show only records relevent to a user, create a view where a filter is applied using the session id stored in the database context. It is also possible to use row-level policies to restrict accesses based on the session id with PostgreSQL 9.5+.

### Installation

pgasus is a go program. You will need the go compiler to build the project.

On debian, one clean way to install go is to use [godeb](https://github.com/niemeyer/godeb).

go will want its own directory to download source code, build, and install binaries. One nonintrusive way is the following, if you are using bash:

```
mkdir ~/gocode
echo "export GOPATH=~/gocode" >> ~/.bash_profile
```

It may also be wise to update your $PATH to include *"~/gocode/bin/"*.

You are now ready to download pgasus:

```
go get github.com/debackerl/pgasus
```

and install it:

```
go install github.com/debackerl/pgasus
```

You can now type *pgasus* to start the program.

### Configuration

The program must be started with the path to its configuration path like this:

```
pgasus --config pgasus.conf
```

Please have a look at the sample pgasus.conf file which is written in [TOML](https://github.com/toml-lang/toml) format.
