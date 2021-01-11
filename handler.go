package main

import (
	gorilla "github.com/gorilla/handlers"
	"github.com/antonholmquist/jason"
	"github.com/debackerl/queryme/go"
	"github.com/naoina/denco"
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
	"sync/atomic"
	"time"
	"unicode/utf8"
	"unsafe"
)

type RecordSetHttpResponder interface {
	RecordSetVisitor
	
	HttpRespond(hw http.ResponseWriter)
}

type RequestHandler struct {
	handler unsafe.Pointer // placed first to be 64-bit aligned
	stop int32

	DbConnConfig pgx.ConnConfig
	Verbose bool
	UrlPrefix string
	UpdatesChannelName string
	SearchPath string
	MaxOpenConnections int
	ContextParameterName string
	FtsFunctionName string
	StatementTimeoutSecs int
	DefaultCn string
	UpdateForwardedForHeader bool
	MaxBodySizeKbytes int64
	MaxResponseSizeKbytes int64
	FilterQueryName string
	SortQueryName string
	LimitQueryName string
	DefaultContext map[string]string
	BinaryFormats map[string]string
	
	Schema Schema
	
	db *pgx.ConnPool
	reqLogFile *os.File
}

func (h *RequestHandler) OpenRequestsLogFile(path string) error {
	var err error
	if path == "-" {
		// TODO: this is UNIX only
		path = "/dev/stdout"
	}
	h.reqLogFile, err = os.OpenFile(path, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
	return err
}

func (h *RequestHandler) CloseRequestsLogFile() {
	h.reqLogFile.Close()
	h.reqLogFile = nil
}

// connects to PostgreSQL and loads all required data from there
func (h *RequestHandler) Load() error {
	var err error
	h.db, err = pgx.NewConnPool(pgx.ConnPoolConfig {
		ConnConfig: h.DbConnConfig,
		MaxConnections: h.MaxOpenConnections,
	})
	
	if err != nil {
		return err
	}
	
	if h.UpdatesChannelName != "" {
		h.listen()
	}
	
	if err := h.createHandlers(); err != nil {
		return err
	}
	
	return nil
}

func (h *RequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	prefix := h.UrlPrefix
	if !strings.HasPrefix(path, prefix) {
		w.WriteHeader(400)
		w.Write([]byte("No routes in this path."))
		return
	}

	path = path[len(prefix):]

	sepIdx := lastIndexRune(path, '.')
	
	if sepIdx == -1 || sepIdx == len(path) - 1 {
		w.WriteHeader(422)
		w.Write([]byte("Extension in path expected in URL."))
	} else {
		ext := path[sepIdx+1:]
		r.URL.Path = path[0:sepIdx]
		r.Header.Set("X-Accept-Extension", ext)
		
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
		
		(*(*http.Handler)(atomic.LoadPointer(&h.handler))).ServeHTTP(w, r)
	}
}

// stop listening on the routes table
func (h *RequestHandler) StopReloads() {
	atomic.StoreInt32(&h.stop, 1)
}

// listens to updates on the routes table for auto-reload
func (h *RequestHandler) listen() {
	go func() {
		log.Println("Listening to routes updates...")
		for atomic.LoadInt32(&h.stop) == 0 {
			conn, err := h.db.Acquire()
			if err != nil {
				log.Fatalln(err)
			}
			
			if err := conn.Listen(h.UpdatesChannelName); err != nil {
				log.Println(err)
				conn.Close()
				h.db.Release(conn)
			} else {
				for {
					notification, err := conn.WaitForNotification(time.Second)
					if err != nil && err != pgx.ErrNotificationTimeout {
						log.Println(err)
						conn.Close()
						h.db.Release(conn)
						break
					}
					if notification != nil && notification.Channel == h.UpdatesChannelName {
						log.Println("Routes reload requested.")
						if err := h.createHandlers(); err != nil {
							log.Println(err)
						}
					}
				}
			}
		}
	}()
}

// loads all routes from PostgreSQL and creates corresponding HTTP handlers, thread-safe
func (h *RequestHandler) createHandlers() error {
	mux := denco.NewMux()
	
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	
	routes, err := h.Schema.LoadRoutes(tx, h.SearchPath)
	if err != nil {
		return err
	}
	
	handlers := make([]denco.Handler, 0, len(routes))
	
	for _, r := range routes {
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
				return errors.New("Invalid provolatile value '" + r.Provolatile + "' for GET route on procedure '" + r.ObjectName + "'")
			}
			routeHandler = h.makeProcedureRouteHandler(r)
		}
		
		handlers = append(handlers, mux.Handler(strings.ToUpper(r.Method), r.UrlPath, routeHandler))
	}
	
	handler, err := mux.Build(handlers)
	if err != nil {
		return err
	}
	
	handler = CatchingHandler(handler)
	
	if h.reqLogFile != nil {
		handler = gorilla.LoggingHandler(h.reqLogFile, handler)
	}
	
	atomic.StorePointer(&h.handler, unsafe.Pointer(&handler))
	
	return nil
}

// makes a request handler for non-batch routes on a relation (GETs and DELETEs)
func (h *RequestHandler) makeNonBatchRouteHandler(route *Route) denco.HandlerFunc {
	return func (w http.ResponseWriter, r *http.Request, params denco.Params) {
		globalQuery := initGlobalQuery(route)
		paramsDecoder(globalQuery, params, route.ParametersTypes)
		
		filter, order, limit, err := parseQueryString(r, globalQuery, h.FilterQueryName, h.SortQueryName, h.LimitQueryName, route.MaxLimit)
		if err != nil {
			panic(err)
		}
		
		responder, err := h.getResponder(r, h.MaxResponseSizeKbytes, route)
		if err != nil {
			panic(err)
		}
		
		tx, err := h.db.Begin()
		if err != nil {
			panic(err)
		}
		defer tx.Rollback()
		
		clientCn, err := getClientRole(tx, r, h.DefaultCn)
		if err != nil {
			panic(err)
		}
		
		context := makeContext(r, h.DefaultContext, params, route.ContextInputCookies, route.ContextParameters, route.ContextHeaders)
		if err := setTxContext(tx, h.StatementTimeoutSecs, clientCn, h.ContextParameterName, context); err != nil {
			panic(err)
		}
		
		sql := NewSqlBuilder()
		
		switch route.Method {
		case "get":
			if err := buildSelectSqlQuery(&sql, h.FtsFunctionName, route.ParametersTypes, route.SelectedColumns, route.ObjectName, filter, order, limit); err != nil {
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
		
		if err := setCookies(w, tx, h.ContextParameterName, route.ContextOutputCookies); err != nil {
			panic(err)
		}
		
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		
		setCacheControl(w, route.TTL, route.IsPublic)
		responder.HttpRespond(w)
	}
}

// makes a request handler for batch routes on a relation (POSTs and PUTs)
func (h *RequestHandler) makeBatchRouteHandler(route *Route) denco.HandlerFunc {
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
		
		responder, err := h.getResponder(r, h.MaxResponseSizeKbytes, route)
		if err != nil {
			panic(err)
		}
		
		tx, err := h.db.Begin()
		if err != nil {
			panic(err)
		}
		defer tx.Rollback()
		
		clientCn, err := getClientRole(tx, r, h.DefaultCn)
		if err != nil {
			panic(err)
		}
		
		context := makeContext(r, h.DefaultContext, params, route.ContextInputCookies, route.ContextParameters, route.ContextHeaders)
		if err := setTxContext(tx, h.StatementTimeoutSecs, clientCn, h.ContextParameterName, context); err != nil {
			panic(err)
		}
		
		if batch {
			responder.BeginBatch()
		}
		
		switch route.Method {
		case "post":
			if filter != nil {
				panic(errors.New("post requests on relations do not support filters."))
			}
			
			for _, query := range queries {
				processPostQuery(h, route, tx, responder, query)
			}
		case "put":
			if batch {
				panic(errors.New("put requests on relations do not support batch mode."))
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
		
		if err := setCookies(w, tx, h.ContextParameterName, route.ContextOutputCookies); err != nil {
			panic(err)
		}
		
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		
		setCacheControl(w, route.TTL, route.IsPublic)
		responder.HttpRespond(w)
	}
}

// makes one SQL insert based on a POST request
func processPostQuery(h *RequestHandler, route *Route, tx *pgx.Tx, responder RecordSetHttpResponder, query map[string]interface{}) {
	sql := NewSqlBuilder()
	
	if err := buildInsertSqlQuery(&sql, h.FtsFunctionName, route.ParametersTypes, route.SelectedColumns, route.ObjectName, query); err != nil {
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

// makes a request handler for a route to a procedure
func (h *RequestHandler) makeProcedureRouteHandler(route *Route) denco.HandlerFunc {
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
		
		responder, err := h.getResponder(r, h.MaxResponseSizeKbytes, route)
		if err != nil {
			panic(err)
		}
		
		tx, err := h.db.Begin()
		if err != nil {
			panic(err)
		}
		defer tx.Rollback()
		
		clientCn, err := getClientRole(tx, r, h.DefaultCn)
		if err != nil {
			panic(err)
		}
		
		context := makeContext(r, h.DefaultContext, params, route.ContextInputCookies, route.ContextParameters, route.ContextHeaders)
		if err := setTxContext(tx, h.StatementTimeoutSecs, clientCn, h.ContextParameterName, context); err != nil {
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
		
		if err := setCookies(w, tx, h.ContextParameterName, route.ContextOutputCookies); err != nil {
			panic(err)
		}
		
		if err := tx.Commit(); err != nil {
			panic(err)
		}
		
		setCacheControl(w, route.TTL, route.IsPublic)
		responder.HttpRespond(w)
	}
}

// makes one procedure call
func processProcedureQuery(route *Route, tx *pgx.Tx, responder RecordSetHttpResponder, query map[string]interface{}) {
	sql := NewSqlBuilder()
	
	// if returned type is a composite type or a setof, then we also send a SELECT * FROM
	// if returned type is 'record', then we jsonize using row_to_json
	// setof record not supported because 'ERROR: a column definition list is required for functions returning "record"'
	
	if route.Proretset && route.Proretoid == pgx.RecordOid {
		panic(errors.New("Functions returning setof record not supported."))
	}
	
	if err := buildProcedureSqlQuery(&sql, route.ObjectName, route.Prorettyptype == "c" || route.Proretset, route.Proretoid == pgx.RecordOid, query); err != nil {
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

// initializes query parameters based on constants defined in route
func initGlobalQuery(route *Route) map[string]interface{} {
	query := make(map[string]interface{})
	
	for k, v := range route.Constants {
		query[k] = v
	}
	
	return query
}

// updates query parameters based on route parameters and types of columns or arguments expected by PostgreSQL
func paramsDecoder(query map[string]interface{}, params denco.Params, argumentsType map[string]ArgumentType) (err error) {
	for _, p := range params {
		if typ, ok := argumentsType[p.Name]; ok {
			var arg interface{} = nil
			val := p.Value
			
			switch typ.Name {
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

// get proper HTTP response writer based on requested extension
func (h *RequestHandler) getResponder(r *http.Request, maxResponseSizeKbytes int64, route *Route) (RecordSetHttpResponder, error) {
	// the following header is provided by this program just before routing
	accept := r.Header.Get("X-Accept-Extension")
	
	mimeType := ""
	
	switch accept {
	case "json":
		return NewJsonRecordSetWriter(maxResponseSizeKbytes << 10), nil
	case "xlsx":
		if route.Proretoid == pgx.ByteaOid && !route.Proretset {
			mimeType = XlsxMimeType
		} else {
			return NewXlsxRecordSetWriter(maxResponseSizeKbytes << 10), nil
		}
	case "csv":
		if (route.Proretoid == pgx.TextOid || route.Proretoid == pgx.VarcharOid) && !route.Proretset {
			mimeType = CsvMimeType
		} else {
			return &CsvRecordSetWriter{MaxResponseSizeBytes: maxResponseSizeKbytes << 10}, nil
		}
	case "bin":
		mimeType = "application/octet-stream"
	default:
		var ok bool
		mimeType, ok = h.BinaryFormats[accept]
		if !ok {
			return nil, errors.New("Requested format unsupported.")
		}
	}

	return &BinRecordSetWriter{MaxResponseSizeBytes: maxResponseSizeKbytes << 10, ContentType: mimeType}, nil
}

// checks TLS common name against configured CA or HTTP Basic authentication as a database user
func getClientRole(tx *pgx.Tx, r *http.Request, defaultCn string) (string, error) {
	if defaultCn == "" {
		// if defaultCn is not specified, we don't active impersonalisation
		return "", nil
	}

	if r.TLS != nil && r.TLS.PeerCertificates != nil && len(r.TLS.PeerCertificates) > 0 {
		return r.TLS.PeerCertificates[0].Subject.CommonName, nil
	}
	
	if h, ok := r.Header["Authorization"]; ok {
		if r.TLS == nil {
			return "", errors.New("Authorization denied over unencrypted connections.")
		}
		
		parts := strings.SplitN(h[0], " ", 2)
		if len(parts) == 2 && parts[0] == "Basic" {
			if usrpwd, err := base64.StdEncoding.DecodeString(parts[1]); err == nil {
				parts = strings.Split(string(usrpwd), ":")
				if len(parts) == 2 {
					usr := parts[0]
					pwd := parts[1]
					
					if err := checkDbRole(tx, usr, pwd); err != nil {
						return "", err
					}
					
					return usr, nil
				}
			}
		}
	}
	
	return defaultCn, nil
}

// checks username/passward against PostgreSQL
func checkDbRole(tx *pgx.Tx, role string, password string) error {
	builder := NewSqlBuilder()
	
	builder.WriteSql("SELECT true FROM pg_authid WHERE (rolvaliduntil > now() OR rolvaliduntil IS NULL) AND rolname=")
	builder.WriteValue(role)
	builder.WriteSql(" AND CASE WHEN substr(rolpassword, 1, 3) = 'md5' THEN rolpassword = 'md5' || encode(digest(")
	builder.WriteValue(password)
	builder.WriteSql(" || ")
	builder.WriteValue(role)
	builder.WriteSql(", 'md5'), 'hex') ELSE FALSE END")
	
	if tag, err := tx.Exec(builder.Sql(), builder.Values()...); err != nil {
		return err
	} else if tag.RowsAffected() == 0 {
		return errors.New("Incorrect credentials.")
	}
	
	return nil
}

// build filters on relations based on query string
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

// compute variables of context based on HTTP request
func makeContext(r *http.Request, defaultContext map[string]string, params denco.Params, contextInputCookies map[string]*CookieConfig, contextParameters []string, contextHeaders pgx.NullHstore) map[string]string {
	context := make(map[string]string)
	
	for k, v := range defaultContext {
		context[k] = v
	}
	
	if len(contextInputCookies) > 0 {
		now := time.Now()
		for _, cookie := range r.Cookies() {
			if config, ok := contextInputCookies[cookie.Name]; ok {
				if cookie.MaxAge >= 0 && (cookie.RawExpires == "" || cookie.Expires.After(now)) && (!config.HttpOnly || cookie.HttpOnly) {
					context[config.ContextVariable.String] = cookie.Value
				}
			}
		}
	}
	
	for _, name := range contextParameters {
		context[name] = params.Get(name)
	}
	
	for from, to := range contextHeaders.Hstore {
		if values, ok := r.Header[from]; ok {
			name := from
			if to.Valid {
				name = to.String
			}
			context[name] = values[0]
		}
	}
	
	return context
}

// set context in PostgreSQL transaction
func setTxContext(tx *pgx.Tx, statementTimeout int, role string, sessionParameter string, context map[string]string) error {
	var builder SqlBuilder

	if role != "" {
		builder = NewSqlBuilder()

		builder.WriteSql("SET LOCAL ROLE ")
		builder.WriteSql("E")
		builder.WriteSql(quoteWith(role, '\'', true))

		if _, err := tx.Exec(builder.Sql(), builder.Values()...); err != nil {
			return err
		}
	}
	
	// use current_setting(setting_name) to get context variables
	
	builder = NewSqlBuilder()
	builder.WriteSql("SELECT set_config(k,v,true) FROM (VALUES ")
	i := 0
	for k, v := range context {
		if i > 0 {
			builder.WriteSql(",")
		}
		builder.WriteSql("(")
		builder.WriteValue(sessionParameter + "." + k)
		builder.WriteSql(",")
		builder.WriteValue(v)
		builder.WriteSql(")")
		i += 1
	}
	builder.WriteSql(") xs(k,v)")
	
	if i > 0 {
		if _, err := tx.Exec(builder.Sql(), builder.Values()...); err != nil {
			return err
		}
	}
	
	// we set statement_timeout last so previous set_config can't overwrite it
	
	if _, err := tx.Exec("SET statement_timeout = " + strconv.Itoa(statementTimeout * 1000)); err != nil {
		return err
	}
	
	return nil
}

// set cookies in HTTP response
func setCookies(w http.ResponseWriter, tx *pgx.Tx, sessionParameter string, contextOutputCookies []*CookieConfig) error {
	sessionParameterLen := len(sessionParameter) + 1
	
	if len(contextOutputCookies) == 0 {
		return nil
	}
	
	builder := NewSqlBuilder()
	builder.WriteSql("SELECT name,current_setting(name) FROM (VALUES ")
	for i, config := range contextOutputCookies {
		if i > 0 {
			builder.WriteSql(",")
		}
		builder.WriteSql("(")
		builder.WriteValue(sessionParameter + "." + config.ContextVariable.String)
		builder.WriteSql(")")
	}
	builder.WriteSql(") xs(name)")
	
	rows, err := tx.Query(builder.Sql(), builder.Values()...)
	if err != nil {
		return err
	}
	defer rows.Close()
	
	if rows.Err() != nil {
		return rows.Err()
	}
	
	values := make(map[string]string)
	
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			return err
		}
		
		values[name[sessionParameterLen:]] = setting
	}
	
	for _, config := range contextOutputCookies {
		if value, found := values[config.ContextVariable.String]; found {
			var cookie http.Cookie
			
			cookie.Name = config.Name
			cookie.Value = value
			if config.Path.Valid {
				cookie.Path = config.Path.String
			}
			if config.SubDomain.Valid {
				cookie.Domain = config.SubDomain.String
			}
			cookie.MaxAge = config.MaxAge
			cookie.Secure = config.Secure
			cookie.HttpOnly = config.HttpOnly
			
			http.SetCookie(w, &cookie)
		}
	}
	
	return nil
}

// set cache-control header in HTTP response
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

// returns position of last given rune in a string
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
