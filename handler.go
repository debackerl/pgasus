package main

import (
	gorilla "github.com/gorilla/handlers"
	"github.com/antonholmquist/jason"
	"github.com/debackerl/queryme/go"
	"github.com/naoina/denco"
	//"gopkg.in/jackc/pgx.v2"
	"github.com/jackc/pgx"
	//"github.com/kr/pretty"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type RecordSetHttpResponder interface {
	RecordSetVisitor
	
	HttpRespond(hw http.ResponseWriter)
}

type route struct {
	Method string
	UrlPath string
	ObjectName string
	ObjectType string
	TTL int
	IsPublic bool
	ContextHeaders pgx.NullHstore
	ContextVariables []string
	ParametersTypes map[string]string
	RawConstants []byte
	Constants map[string]interface{}
	MaxLimit int64 // select on relations only
	HiddenFields map[string]struct{}
	ReadOnlyFields map[string]struct{} // insert and update on relations only
	Proretset bool // procedures only, true if it returns a set, false otherwise
	Provolatile string // procedures only, "i" for immutable, "s" for stable, "v" for volatile
	Prorettyptype string // procedures only
	Columns string // relations only
}

type RequestHandler struct {
	Verbose bool
	Socket string
	Database string
	SearchPath string
	MaxOpenConnections int
	ContextParameterName string
	RoutesTableName string
	FtsFunctionName string
	DefaultCn string
	UpdateForwardedForHeader bool
	MaxBodySizeKbytes int64
	MaxResponseSizeKbytes int64
	FilterQueryName string
	SortQueryName string
	LimitQueryName string
	
	db *pgx.ConnPool
	handler http.Handler
	reqLogFile *os.File
}

func (h *RequestHandler) OpenRequestsLogFile(path string) error {
	var err error
	if path == "-" {
		path = "/dev/stdout"
	}
	h.reqLogFile, err = os.OpenFile(path, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
	return err
}

func (h *RequestHandler) CloseRequestsLogFile() {
	h.reqLogFile.Close()
	h.reqLogFile = nil
}

func (h *RequestHandler) Load() error {
	var err error
	h.db, err = pgx.NewConnPool(pgx.ConnPoolConfig {
		ConnConfig: pgx.ConnConfig {
			Host: h.Socket,
			Database: h.Database,
		},
		MaxConnections: h.MaxOpenConnections,
	})
	
	if err != nil {
		return err
	}
	
	if err := h.createHandlers(); err != nil {
		return err
	}
	
	return nil
}

func lastIndexRune(s string, r rune) int {
	for i := len(s); i > 0; {
		rune, size := utf8.DecodeLastRuneInString(s[0:i])
		i -= size
		if rune == r {
			return i
		}
	}
	return -1
}

func (h *RequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Error while processing request:", r)
			
			w.WriteHeader(http.StatusInternalServerError)
			
			if err, ok := r.(error); ok {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.Write([]byte(err.Error()))
			}
		}
	}()
	
	path := r.URL.Path
	sepIdx := lastIndexRune(path, '.')
	
	if sepIdx == -1 || sepIdx == len(path) - 1 {
		w.WriteHeader(422)
		w.Write([]byte("Extension in path expected in URL."))
	} else {
		ext := path[sepIdx+1:]
		r.URL.Path = path[0:sepIdx]
		r.Header.Set("X-Accept", ext)
		
		if h.UpdateForwardedForHeader {
			if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
				original := r.Header.Get("X-Forwarded-For")
				if len(original) > 0 {
					original = original + ","
				}
				original = original + ip
				r.Header.Set("X-Forwarded-For", original)
			}
		}
		
		h.handler.ServeHTTP(w, r)
	}
}

/*func (h *RequestHandler) listen() {
	var channelName = ""
	
	for {
		conn, err := h.db.Acquire()
		
		err = conn.Listen(channelName)
		
		notification, err := conn.WaitForNotification(time.Minute)
		
		if err == nil {
			
		} else if err != pgx.ErrNotificationTimeout {
			conn.Close()
			h.db.Release(conn)
		}
	}
}*/

func (h *RequestHandler) createHandlers() error {
	mux := denco.NewMux()
	
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	if h.SearchPath != "" {
		if strings.Index(h.SearchPath, ";") >= 0 {
			return errors.New("Invalid search path: " + h.SearchPath)
		}
		
		if _, err := tx.Exec(`SET LOCAL search_path = ` + h.SearchPath); err != nil {
			return err
		}
	}
	
	routes, err := loadRoutes(tx, h.RoutesTableName)
	if err != nil {
		return err
	}
	
	handlers := make([]denco.Handler, 0, len(routes))
	for _, r := range routes {
		loadParametersTypes(tx, r)
		
		if r.ObjectType == "procedure" {
			loadProc(tx, r)
		}
		
		var jsonConstants *jason.Object
		jsonConstants, err = jason.NewObjectFromBytes(r.RawConstants)
		if err != nil {
			return err
		}
		r.RawConstants = nil
		
		r.Constants, err = prepareArgumentsFromObject(jsonConstants, r.ParametersTypes, nil)
		if err != nil {
			return err
		}
		
		if h.Verbose {
			log.Printf("Loading route, method: %s, url: %s, target: %s type: %s", r.Method, r.UrlPath, r.ObjectName, r.ObjectType)
		}
		
		var routeHandler denco.HandlerFunc = nil
		
		switch r.ObjectType {
		case "relation":
			switch r.Method {
			case "get", "delete":
				routeHandler = h.makeNonBatchRouteHandler(r)
			case "post", "put":
				routeHandler = h.makeBatchRouteHandler(r)
			default:
				return errors.New("Unknown HTTP method " + r.Method)
			}
		case "procedure":
			if r.Method == "get" && (r.Provolatile != "i" && r.Provolatile != "s") {
				return errors.New("Invalid provolatile value for GET route on procedure: " + r.Provolatile)
			}
			routeHandler = h.makeProcedureRouteHandler(r)
		}
		
		handlers = append(handlers, mux.Handler(strings.ToUpper(r.Method), r.UrlPath, routeHandler))
	}
	
	handler, err := mux.Build(handlers)
	if err != nil {
		return err
	}
	
	if h.reqLogFile != nil {
		handler = gorilla.LoggingHandler(h.reqLogFile, handler)
	}
	
	h.handler = handler
	
	return nil
}

func loadRoutes(tx *pgx.Tx, routesTableName string) ([]*route, error) {
	rows, err := tx.Query(`SELECT method,url_path,object_name,object_type,ttl,is_public,hidden_fields,readonly_fields,context_mapped_headers,context_mapped_variables,constants,max_limit FROM ` + quoteIdentifier(routesTableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	routes := make([]*route, 0, 16)
	for rows.Next() {
		r := new(route)
		
		var ttl int32
		var maxLimit int32
		var hiddenFields []string
		var readonlyFields []string
		if err := rows.Scan(&r.Method, &r.UrlPath, &r.ObjectName, &r.ObjectType, &ttl, &r.IsPublic, &hiddenFields, &readonlyFields, &r.ContextHeaders, &r.ContextVariables, &r.RawConstants, &maxLimit); err != nil {
			return nil, err
		}
		
		r.TTL = int(ttl)
		r.MaxLimit = int64(maxLimit)
		
		r.HiddenFields = make(map[string]struct{})
		for _, hiddenField := range hiddenFields {
			r.HiddenFields[hiddenField] = struct{}{}
		}
		
		r.ReadOnlyFields = make(map[string]struct{})
		for _, readonlyField := range readonlyFields {
			r.ReadOnlyFields[readonlyField] = struct{}{}
		}
		
		routes = append(routes, r)
	}
	
	return routes, nil
}

func loadParametersTypes(tx *pgx.Tx, route *route) error {
	// for base types, use attribute's type, for domains, use underlying type, otherwise use text
	var sql string
	switch route.ObjectType {
	case "relation":
		sql = `SELECT att.attname, (CASE typ.typtype WHEN 'b' THEN att.atttypid WHEN 'd' THEN typ.typbasetype ELSE 25 END)::regtype FROM pg_attribute att INNER JOIN pg_type typ ON att.atttypid = typ.oid WHERE att.attrelid = $1::regclass::oid AND att.attisdropped = false AND att.attnum > 0`
	case "procedure":
		sql = `SELECT args.name, (CASE typ.typtype WHEN 'b' THEN args.type WHEN 'd' THEN typ.typbasetype ELSE 25 END)::regtype FROM (SELECT unnest.* FROM pg_proc, unnest(pg_proc.proargnames, pg_proc.proargtypes::int[]) WHERE pg_proc.proname = $1) AS args(name, type) INNER JOIN pg_type typ ON args.type = typ.oid`
	default:
		return errors.New("Unknown object type: " + route.ObjectType)
	}
	
	rows, err := tx.Query(sql, route.ObjectName)
	if err != nil {
		return err
	}
	defer rows.Close()
	
	route.ParametersTypes = make(map[string]string)
	fieldsLeft := make([]string, 0, 16)
	
	for rows.Next() {
		var name, typ pgx.NullString
		if err := rows.Scan(&name, &typ); err != nil {
			return err
		}
		
		if name.Valid && typ.Valid {
			route.ParametersTypes[name.String] = typ.String
			
			if route.ObjectType == "relation" {
				if _, ok := route.HiddenFields[name.String]; !ok {
					fieldsLeft = append(fieldsLeft, quoteIdentifier(name.String))
				}
			}
		}
	}
	
	route.Columns = strings.Join(fieldsLeft, ",")
	
	return nil
}

func loadProc(tx *pgx.Tx, route *route) error {
	rows, err := tx.Query(`SELECT pro.proretset, pro.provolatile, typ.typtype FROM pg_proc pro INNER JOIN pg_type typ ON pro.prorettype = typ.oid WHERE pro.proname = $1`, route.ObjectName)
	if err != nil {
		return err
	}
	defer rows.Close()
	
	if rows.Next() {
		if err := rows.Scan(&route.Proretset, &route.Provolatile, &route.Prorettyptype); err != nil {
			return err
		}
	} else {
		return errors.New("Procedure could not be found: " + route.ObjectName)
	}
	
	return nil
}

func (h *RequestHandler) makeNonBatchRouteHandler(route *route) denco.HandlerFunc {
	return func (w http.ResponseWriter, r *http.Request, params denco.Params) {
		globalQuery := initGlobalQuery(route)
		paramsDecoder(globalQuery, params, route.ParametersTypes)
		
		filter, order, limit, err := parseQueryString(r, globalQuery, h.FilterQueryName, h.SortQueryName, h.LimitQueryName, route.MaxLimit)
		if err != nil {
			panic(err)
		}
		
		responder, err := getResponder(r, h.MaxResponseSizeKbytes)
		if err != nil {
			panic(err)
		}
		
		tx, err := h.db.Begin()
		if err != nil {
			panic(err)
		}
		defer tx.Rollback()
		
		clientCn := getClientCn(r, h.DefaultCn)
		context := makeContext(r, route.ContextHeaders, params, route.ContextVariables)
		if err := setTxContext(tx, clientCn, h.ContextParameterName, context); err != nil {
			panic(err)
		}
		
		sql := NewSqlBuilder()
		
		switch route.Method {
		case "get":
			if err := buildSelectSqlQuery(&sql, h.FtsFunctionName, route.ParametersTypes, route.Columns, route.ObjectName, filter, order, limit); err != nil {
				panic(err)
			}
			
			rows, err := tx.Query(sql.Sql(), sql.Values()...)
			if err != nil {
				panic(err)
			}
			defer rows.Close()
			
			if err := readRecords(responder, false, rows); err != nil {
				panic(err)
			}
		case "delete":
			if err := buildDeleteSqlQuery(&sql, h.FtsFunctionName, route.ParametersTypes, route.ObjectName, filter); err != nil {
				panic(err)
			}
			
			cmdTag, err := tx.Exec(sql.Sql(), sql.Values()...)
			if err != nil {
				panic(err)
			}
			
			if err := VisitRowsAffectedRecordSet(responder, cmdTag.RowsAffected()); err != nil {
				panic(err)
			}
		default:
			panic(errors.New("Unknown HTTP method: " + route.Method))
		}
		
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		
		setCacheControl(w, route.TTL, route.IsPublic)
		responder.HttpRespond(w)
	}
}

func (h *RequestHandler) makeBatchRouteHandler(route *route) denco.HandlerFunc {
	return func (w http.ResponseWriter, r *http.Request, params denco.Params) {
		globalQuery := initGlobalQuery(route)
		paramsDecoder(globalQuery, params, route.ParametersTypes)
		
		queries, batch, err := decodeHttpBody(w, r, route.ParametersTypes, route.ReadOnlyFields, h.MaxBodySizeKbytes)
		if err != nil {
			panic(err)
		}
		
		filter, _, _, err := parseQueryString(r, globalQuery, h.FilterQueryName, h.SortQueryName, h.LimitQueryName, route.MaxLimit)
		if err != nil {
			panic(err)
		}
		
		responder, err := getResponder(r, h.MaxResponseSizeKbytes)
		if err != nil {
			panic(err)
		}
		
		tx, err := h.db.Begin()
		if err != nil {
			panic(err)
		}
		defer tx.Rollback()
		
		clientCn := getClientCn(r, h.DefaultCn)
		context := makeContext(r, route.ContextHeaders, params, route.ContextVariables)
		if err := setTxContext(tx, clientCn, h.ContextParameterName, context); err != nil {
			panic(err)
		}
		
		if batch {
			responder.BeginBatch()
		}
		
		switch route.Method {
		case "post":
			for _, query := range queries {
				processBatchPost(h, route, tx, responder, query, filter)
			}
		case "put":
			if batch {
				panic(errors.New("put requests do not support batch mode."))
			} else {
				query := queries[0]
				
				sql := NewSqlBuilder()
				
				if err := buildUpdateSqlQuery(&sql, h.FtsFunctionName, route.ParametersTypes, route.ObjectName, filter, query); err != nil {
					panic(err)
				}
				
				cmdTag, err := tx.Exec(sql.Sql(), sql.Values()...)
				if err != nil {
					panic(err)
				}
				
				if err := VisitRowsAffectedRecordSet(responder, cmdTag.RowsAffected()); err != nil {
					panic(err)
				}
			}
		default:
			panic(errors.New("Unknown HTTP method: " + route.Method))
		}
		
		if batch {
			responder.EndBatch()
		}
		
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		
		setCacheControl(w, route.TTL, route.IsPublic)
		responder.HttpRespond(w)
	}
}

func processBatchPost(h *RequestHandler, route *route, tx *pgx.Tx, responder RecordSetHttpResponder, query map[string]interface{}, filter queryme.Predicate) {
	sql := NewSqlBuilder()
	
	if err := buildInsertSqlQuery(&sql, h.FtsFunctionName, route.ParametersTypes, route.ObjectName, filter, query); err != nil {
		panic(err)
	}
	
	rows, err := tx.Query(sql.Sql(), sql.Values()...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	
	if err := readRecords(responder, false, rows); err != nil {
		panic(err)
	}
}

func (h *RequestHandler) makeProcedureRouteHandler(route *route) denco.HandlerFunc {
	return func (w http.ResponseWriter, r *http.Request, params denco.Params) {
		globalQuery := initGlobalQuery(route)
		paramsDecoder(globalQuery, params, route.ParametersTypes)
		
		var queries []map[string]interface{}
		var batch bool
		
		if r.Method == "GET" || r.Method == "DELETE" {
			batch = false
			queries = make([]map[string]interface{}, 0, 1)
			
			query, err := prepareArgumentsFromQueryString(r.URL.RawQuery, route.ParametersTypes)
			if err != nil {
				panic(err)
			}
			
			queries = append(queries, query)
		} else {
			var err error
			queries, batch, err = decodeHttpBody(w, r, route.ParametersTypes, nil, h.MaxBodySizeKbytes)
			if err != nil {
				panic(err)
			}
		}
		
		responder, err := getResponder(r, h.MaxResponseSizeKbytes)
		if err != nil {
			panic(err)
		}
		
		tx, err := h.db.Begin()
		if err != nil {
			panic(err)
		}
		defer tx.Rollback()
		
		clientCn := getClientCn(r, h.DefaultCn)
		context := makeContext(r, route.ContextHeaders, params, route.ContextVariables)
		if err := setTxContext(tx, clientCn, h.ContextParameterName, context); err != nil {
			panic(err)
		}
		
		if batch {
			responder.BeginBatch()
		}
		
		for _, query := range queries {
			for k, v := range globalQuery {
				query[k] = v
			}
			
			processProcedureQuery(route, tx, responder, query)
		}
		
		if batch {
			responder.EndBatch()
		}
		
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		
		setCacheControl(w, route.TTL, route.IsPublic)
		responder.HttpRespond(w)
	}
}

func processProcedureQuery(route *route, tx *pgx.Tx, responder RecordSetHttpResponder, query map[string]interface{}) {
	sql := NewSqlBuilder()
	// if returned type is a composite type, then we also send a SELECT * FROM
	if err := buildProcedureSqlQuery(&sql, route.ObjectName, route.Proretset || route.Prorettyptype == "c", query); err != nil {
		panic(err)
	}
	
	rows, err := tx.Query(sql.Sql(), sql.Values()...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	
	if rows.Err() != nil {
		panic(rows.Err())
	} else {
		if route.Proretset {
			if err := readRecords(responder, false, rows); err != nil {
				panic(err)
			}
		} else if route.Prorettyptype == "c" { // composite type as return
			if err := readRecords(responder, true, rows); err != nil {
				panic(err)
			}
		} else {
			if err := readScalar(responder, rows); err != nil {
				panic(err)
			}
		}
	}
}

func initGlobalQuery(route *route) map[string]interface{} {
	query := make(map[string]interface{})
	
	for k, v := range route.Constants {
		query[k] = v
	}
	
	return query
}

func paramsDecoder(query map[string]interface{}, params denco.Params, argumentsType map[string]string) (err error) {
	for _, p := range params {
		if typ, ok := argumentsType[p.Name]; ok {
			var arg interface{} = nil
			val := p.Value
			
			switch typ {
			case "boolean":
				switch val {
				case "":
					arg = nil
				case "t", "true":
					arg = true
				case "f", "false":
					arg = false
				default:
					err = errors.New("Invalid boolean value: " + val)
				}
			case "smallint", "integer", "bigint":
				if val != "" {
					arg, err = strconv.ParseInt(val, 10, 64)
				}
			case "real", "double precision":
				if val != "" {
					arg, err = strconv.ParseFloat(val, 64)
				}
			case "timestamp without time zone", "timestamp with time zone":
				if val != "" {
					arg, err = time.Parse(time.RFC3339, val)
				}
			case "bytea":
				arg, err = base64.URLEncoding.DecodeString(val)
			default: // including "numeric", "money", "date", "time without time zone", "time with time zone", "character", "text", and "character varying"
				arg, err = url.QueryUnescape(val)
			}
			
			if err != nil {
				return
			}
			
			query[p.Name] = arg
		}
	}
	
	return
}

func getResponder(r *http.Request, maxResponseSizeKbytes int64) (RecordSetHttpResponder, error) {
	// the following header is provided by this program just before routing
	switch r.Header.Get("X-Accept") {
	case "json":
		return NewJsonRecordSetWriter(maxResponseSizeKbytes), nil
	case "csv":
		return &CsvRecordSetWriter{MaxResponseSizeKbytes: maxResponseSizeKbytes}, nil
	default:
		return nil, errors.New("Requested format unsupported.")
	}
}

func getClientCn(r *http.Request, defaultCn string) string {
	if r.TLS != nil && r.TLS.PeerCertificates != nil && len(r.TLS.PeerCertificates) > 0 {
		return r.TLS.PeerCertificates[0].Subject.CommonName
	}
	return defaultCn
}

func parseQueryString(r *http.Request, globalQuery map[string]interface{}, filterQueryName string, sortQueryName string, limitQueryName string, maxLimit int64) (queryme.Predicate, []*queryme.SortOrder, int64, error) {
	queryString := queryme.NewFromRawQuery(r.URL.RawQuery)
	
	conjunctionTerms := make([]queryme.Predicate, 0, 8)
	
	if queryString.Contains(filterQueryName) {
		filter, err := queryString.Predicate(filterQueryName)
		if err != nil {
			return nil, nil, -1, err
		}
		
		conjunctionTerms = append(conjunctionTerms, filter)
	}
	
	if globalQuery != nil {
		for k, v := range globalQuery {
			equalityTerm := queryme.Eq{queryme.Field(k), []queryme.Value{v}}
			conjunctionTerms = append(conjunctionTerms, equalityTerm)
		}
	}
	
	var filter queryme.Predicate = nil
	if len(conjunctionTerms) > 0 {
		filter = queryme.And(conjunctionTerms)
	}
	
	order := []*queryme.SortOrder{}
	if queryString.Contains(sortQueryName) {
		var err error
		order, err = queryString.SortOrder(sortQueryName)
		if err != nil {
			return nil, nil, -1, err
		}
	}
	
	limit := int64(-1)
	if raw, ok := queryString.Raw(limitQueryName); ok {
		var err error
		limit, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, nil, -1, err
		}
	}
	
	if maxLimit > 0 && (maxLimit < limit || limit <= 0) {
		limit = maxLimit
	}
	
	return filter, order, limit, nil
}

func setCacheControl(w http.ResponseWriter, ttl int, public bool) {
	// http://www.mobify.com/blog/beginners-guide-to-http-cache-headers/
	
	cacheControl := "private"
	if public {
		cacheControl = "public"
	}
	
	if ttl > 0 {
		cacheControl = fmt.Sprintf("%s, max-age=%d", cacheControl, ttl)
	} else {
		cacheControl = fmt.Sprintf("%s, no-store", cacheControl)
	}
	
	w.Header().Set("Cache-Control", cacheControl)
}

func makeContext(r *http.Request, contextHeaders pgx.NullHstore, params denco.Params, contextVariables []string) map[string]string {
	context := make(map[string]string)
	
	for from, to := range contextHeaders.Hstore {
		if values, ok := r.Header[from]; ok {
			name := from
			if to.Valid {
				name = to.String
			}
			context[name] = values[0]
		}
	}
	
	var cookies map[string]*http.Cookie
	
	for _, name := range contextVariables {
		v := params.Get(name)
		
		if v == "" {
			if cookies == nil {
				now := time.Now()
				cookies = make(map[string]*http.Cookie)
				for _, c := range r.Cookies() {
					if c.MaxAge >= 0 && (c.RawExpires == "" || c.Expires.After(now)) {
						cookies[c.Name] = c
					}
				}
			}
			
			if c, ok := cookies[name]; ok {
				v = c.Value
			}
		}
		
		context[name] = v
	}
	
	return context
}

func setTxContext(tx *pgx.Tx, role string, sessionParameter string, context map[string]string) error {
	builder := NewSqlBuilder()
	
	builder.WriteSql("SET LOCAL ROLE ")
	if role == "" {
		builder.WriteSql("NONE")
	} else {
		builder.WriteSql("E")
		builder.WriteSql(quoteWith(role, '\'', true))
	}
	
	if _, err := tx.Exec(builder.Sql(), builder.Values()...); err != nil {
		return err
	}
	
	// use current_setting(setting_name) to get context variables
	
	for k, v := range context {
		builder = NewSqlBuilder()
		
		builder.WriteSql("SELECT set_config(")
		builder.WriteValue(sessionParameter + "." + k)
		builder.WriteSql(",")
		builder.WriteValue(v)
		builder.WriteSql(",true)")
		
		if _, err := tx.Exec(builder.Sql(), builder.Values()...); err != nil {
			return err
		}
	}
	
	return nil
}
