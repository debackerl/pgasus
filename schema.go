package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type Schema struct {
	CookiesDomain        string
	CookiesPath          string
	CookiesDisableSecure bool
	RoutesTableName      string
}

type ArgumentType struct {
	Name       string
	ElementOid pgtype.OID
}

type Route struct {
	Method               string
	UrlPath              string
	ObjectName           string
	ObjectType           string
	TTL                  int
	IsPublic             bool
	ContextHeaders       pgtype.Hstore
	ContextParameters    []string
	ParametersTypes      map[string]ArgumentType // arguments of procedure, or columns of relation
	RawConstants         []byte
	Constants            map[string]interface{}
	MaxLimit             int64 // select on relations only
	HiddenFields         map[string]struct{}
	ReadOnlyFields       map[string]struct{}      // insert and update on relations only
	Proretset            bool                     // procedures only, true if it returns a set, false otherwise
	Provolatile          rune                     // procedures only, "i" for immutable, "s" for stable, "v" for volatile
	Prorettyptype        rune                     // procedures only
	Proretoid            pgtype.OID               // procedures only
	SelectedColumns      string                   // relations only
	ContextInputCookies  map[string]*CookieConfig // cookies to get from HTTP requests
	ContextOutputCookies []*CookieConfig          // cookies to set in HTTP responses
	// for documentation generator:
	RouteID             int
	AllCookies          []CookieConfig
	ParametersDeclTypes map[string]string
	OptionalArguments   []string // procedures only
	Prorettypname       string   // procedures only
	Description         string
}

type CookieConfig struct {
	ContextVariable NullString `json:"contextVariable"` // name of context variable to read from PostgreSQL's session
	Name            string     `json:"name"`            // cookie's name
	MaxAge          int        `json:"maxAge"`          // cookie expires after this many seconds, set to 0 to disable expiration
	SubDomain       NullString `json:"subDomain"`       // the subdomain is prepended to the domain specified in the configuration file, null values disable this option
	Path            NullString `json:"path"`            // the path is appended to the path specified in the configuration file, null values disable this option
	Secure          bool       `json:"secure"`          // transmitted over TLS connections only
	HttpOnly        bool       `json:"httpOnly"`        // transmitted over HTTP(S) connections only, inaccessible via JavaScript
	Read            bool       `json:"read"`
	Write           bool       `json:"write"`
}

type NullString struct {
	String string
	Valid  bool // Valid is true if String is not NULL
}

func (s *NullString) UnmarshalJSON(raw []byte) error {
	s.Valid = string(raw) != "null"

	if s.Valid {
		if err := json.Unmarshal(raw, &s.String); err != nil {
			return err
		}
	}

	return nil
}

// loads all routes defined in PostgreSQL
func (s *Schema) LoadRoutes(ctx context.Context, tx pgx.Tx, searchPath string) ([]*Route, error) {
	log.Println("Loading routes...")

	if searchPath != "" {
		if strings.Index(searchPath, ";") >= 0 {
			return nil, errors.New("Invalid search path: " + searchPath)
		}

		if _, err := tx.Exec(ctx, `SET LOCAL search_path = `+searchPath); err != nil {
			return nil, err
		}
	}

	rows, err := tx.Query(ctx, `SELECT route_id,method,url_path,object_name,object_type,ttl,is_public,hidden_fields,readonly_fields,context_mapped_headers,context_mapped_variables,constants,max_limit,context_mapped_cookies FROM `+quoteIdentifier(s.RoutesTableName)+`ORDER BY url_path, CASE method WHEN 'get' THEN 0 WHEN 'post' THEN 1 WHEN 'put' THEN 2 ELSE 9 END`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	routes := make([]*Route, 0, 16)
	for rows.Next() {
		r := new(Route)

		var ttl int32
		var maxLimit int32
		var hiddenFields []string
		var readonlyFields []string
		var rawCookiesJson []byte
		if err := rows.Scan(&r.RouteID, &r.Method, &r.UrlPath, &r.ObjectName, &r.ObjectType, &ttl, &r.IsPublic, &hiddenFields, &readonlyFields, &r.ContextHeaders, &r.ContextParameters, &r.RawConstants, &maxLimit, &rawCookiesJson); err != nil {
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

		if rawCookiesJson != nil {
			if err := json.Unmarshal(rawCookiesJson, &r.AllCookies); err != nil {
				return nil, errors.New(fmt.Sprintf("Could not parse cookies configuration for %v %v, error: %v", r.Method, r.UrlPath, err.Error()))
			}

			r.ContextInputCookies = make(map[string]*CookieConfig)
			r.ContextOutputCookies = make([]*CookieConfig, 0, 4)

			for i := range r.AllCookies {
				cookie := &r.AllCookies[i]

				if cookie.Read {
					r.ContextInputCookies[cookie.Name] = cookie
				}

				if cookie.Write {
					r.ContextOutputCookies = append(r.ContextOutputCookies, cookie)
				}

				if !cookie.ContextVariable.Valid || cookie.ContextVariable.String == "" {
					cookie.ContextVariable.Valid = true
					cookie.ContextVariable.String = cookie.Name
				}

				if cookie.SubDomain.Valid {
					cookie.SubDomain.String = strings.Join([]string{cookie.SubDomain.String, s.CookiesDomain}, ".")
				} else if s.CookiesDomain != "" {
					cookie.SubDomain.String = s.CookiesDomain
					cookie.SubDomain.Valid = true
				}

				if cookie.Path.Valid {
					cookie.Path.String = path.Join(s.CookiesPath, path.Clean(cookie.Path.String))
				} else if s.CookiesPath != "" {
					cookie.Path.String = s.CookiesPath
					cookie.Path.Valid = true
				}

				if s.CookiesDisableSecure {
					cookie.Secure = false
				}
			}
		}

		routes = append(routes, r)
	}

	for _, r := range routes {
		if err := loadObject(ctx, tx, r); err != nil {
			return nil, err
		}

		if r.ObjectType == "procedure" {
			if err := loadProc(ctx, tx, r); err != nil {
				return nil, err
			}
		}
	}

	log.Println("Routes loaded.")

	return routes, nil
}

// loads types of relation columns or procedure arguments from PostgreSQL for given route
func loadObject(ctx context.Context, tx pgx.Tx, route *Route) error {
	// for base types, use attribute's type, for domains, use underlying type, otherwise use text
	var rows pgx.Rows
	var oid pgtype.OID
	var err error
	switch route.ObjectType {
	case "relation":
		oid, err = getRelationOid(ctx, tx, route.ObjectName)
		if err != nil {
			return err
		}
		sql := `SELECT att.attname, coalesce(etyp.oid, 0), (CASE coalesce(etyp.typtype, typ.typtype) WHEN 'b' THEN att.atttypid::regtype::text WHEN 'd' THEN coalesce(etyp.typbasetype::regtype::text || '[]', typ.typbasetype::regtype::text) ELSE (CASE WHEN typ.typcategory = 'A' THEN 25::regtype::text || '[]' ELSE 25::regtype::text END) END), typ.oid::regtype, att.atthasdef OR NOT att.attnotnull FROM pg_attribute att INNER JOIN pg_type typ ON att.atttypid = typ.oid LEFT JOIN pg_type etyp ON typ.typelem = etyp.oid AND typ.typcategory = 'A' WHERE att.attrelid = $1 AND att.attisdropped = false AND att.attnum > 0`
		rows, err = tx.Query(ctx, sql, oid)
	case "procedure":
		oid, err = getProcedureOid(ctx, tx, route.ObjectName)
		if err != nil {
			return err
		}
		sql := `SELECT args.name, coalesce(etyp.oid, 0), (CASE coalesce(etyp.typtype, typ.typtype) WHEN 'b' THEN args.type::regtype::text WHEN 'd' THEN coalesce(etyp.typbasetype::regtype::text || '[]', typ.typbasetype::regtype::text) ELSE (CASE WHEN typ.typcategory = 'A' THEN 25::regtype::text || '[]' ELSE 25::regtype::text END) END), typ.oid::regtype, isoptional FROM (SELECT (row_number() OVER ()) BETWEEN (pg_proc.pronargs-pg_proc.pronargdefaults+1) AND pg_proc.pronargs, unnest.* FROM pg_proc, unnest(pg_proc.proargnames, pg_proc.proargtypes::int[]) WHERE pg_proc.oid = $1) AS args(isoptional, name, type) INNER JOIN pg_type typ ON args.type = typ.oid LEFT JOIN pg_type etyp ON typ.typelem = etyp.oid AND typ.typcategory = 'A'`
		rows, err = tx.Query(ctx, sql, oid)
	default:
		return errors.New("Unknown object type: " + route.ObjectType)
	}

	if err != nil {
		return err
	}
	defer rows.Close()

	route.ParametersTypes = make(map[string]ArgumentType)
	route.ParametersDeclTypes = make(map[string]string)
	fieldsLeft := make([]string, 0, 16)
	optionalArguments := make([]string, 0, 16)

	for rows.Next() {
		var name, typ, declTyp pgtype.Text
		var eoid pgtype.OID
		var isoptional bool
		if err := rows.Scan(name, &eoid, &typ, &declTyp, &isoptional); err != nil {
			return err
		}

		if name.Status == pgtype.Present && typ.Status == pgtype.Present {
			route.ParametersTypes[name.String] = ArgumentType{Name: typ.String, ElementOid: eoid}

			if route.ObjectType == "relation" {
				if _, ok := route.HiddenFields[name.String]; !ok {
					fieldsLeft = append(fieldsLeft, quoteIdentifier(name.String))
				}

				if route.Method != "post" {
					isoptional = true
				}
			}

			if isoptional {
				optionalArguments = append(optionalArguments, name.String)
			}

			if declTyp.Status == pgtype.Present {
				route.ParametersDeclTypes[name.String] = declTyp.String
			} else {
				route.ParametersDeclTypes[name.String] = typ.String
			}
		}
	}

	route.SelectedColumns = strings.Join(fieldsLeft, ",")
	route.OptionalArguments = optionalArguments

	if route.Description, err = getDescription(ctx, tx, oid); err != nil {
		return err
	}

	return nil
}

// loads details of a procedure from PostgreSQL for given route
func loadProc(ctx context.Context, tx pgx.Tx, route *Route) error {
	rows, err := tx.Query(ctx, `SELECT pro.proretset, pro.provolatile, typ.typtype, typ.oid::regtype, typ.oid FROM pg_proc pro INNER JOIN pg_type typ ON pro.prorettype = typ.oid WHERE pro.proname = $1`, route.ObjectName)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&route.Proretset, &route.Provolatile, &route.Prorettyptype, &route.Prorettypname, &route.Proretoid); err != nil {
			return err
		}
	} else {
		return errors.New("Could not find procedure " + route.ObjectName)
	}

	return nil
}

func getRelationOid(ctx context.Context, tx pgx.Tx, id string) (pgtype.OID, error) {
	rows, err := tx.Query(ctx, `SELECT $1::regclass::oid`, id)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, errors.New("Could not find relation " + id)
	}

	var oid pgtype.OID
	if err = rows.Scan(&oid); err != nil {
		return 0, err
	}

	return oid, nil
}

func getProcedureOid(ctx context.Context, tx pgx.Tx, id string) (pgtype.OID, error) {
	rows, err := tx.Query(ctx, `SELECT $1::regproc::oid`, id)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, errors.New("Could not find procedure " + id)
	}

	var oid pgtype.OID
	if err = rows.Scan(&oid); err != nil {
		return 0, err
	}

	return oid, nil
}

func getDescription(ctx context.Context, tx pgx.Tx, oid pgtype.OID) (string, error) {
	rows, err := tx.Query(ctx, `SELECT obj_description($1)`, oid)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var s pgtype.Text
	if rows.Next() {
		if err := rows.Scan(&s); err != nil {
			return "", err
		}
	}

	return s.String, nil
}
