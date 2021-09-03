package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/olekukonko/tablewriter"
)

type DocumentationGenerator struct {
	DbConnConfig    *pgx.ConnConfig
	Schema          Schema
	SearchPath      string
	FilterQueryName string
	SortQueryName   string
	LimitQueryName  string
}

func (g *DocumentationGenerator) GenerateDocumentation(outputPath string) {
	var err error

	ctx := context.Background()
	routeParser := regexp.MustCompile("/[:*][^/]+")
	tblBorders := tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false}
	typeDescriptions := make(map[string]string)

	var db *pgx.Conn
	db, err = pgx.ConnectConfig(ctx, g.DbConnConfig)
	if err != nil {
		log.Fatalln("Could not connect to database:", err)
	}

	var tx pgx.Tx
	tx, err = db.Begin(ctx)
	if err != nil {
		log.Fatalln("Could not begin transaction:", err)
	}
	defer tx.Rollback(ctx)

	var routes []*Route
	routes, err = g.Schema.LoadRoutes(ctx, tx, g.SearchPath)
	if err != nil {
		log.Fatalln("Could not load routes:", err)
	}

	var f *os.File
	f, err = os.Create(outputPath)
	if err != nil {
		log.Fatalln("Could not create output file:", err)
	}
	defer f.Close()

	wtr := bufio.NewWriter(f)
	defer wtr.Flush()

	wtr.WriteString("# API Specification\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("## Protocol\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("This web service implements a RESTful compatible interface.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("### Requests\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("A resource is identified by its URL. Those can contain several arguments, for which two formats are available:\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("- `/:arg_name`, argument `arg_name` will match a single path segment\r\n")
	wtr.WriteString("- `/*arg_name`, placed at the end of a route, will match the end of the path\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("When sending a request, the expected result format is specified by appending an extension `.ext` at the end of the URL. This is compulsory. Several formats are available:\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("- `.json`\r\n")
	wtr.WriteString("- `.xlsx`\r\n")
	wtr.WriteString("- `.csv`\r\n")
	wtr.WriteString("- `.bin`\r\n")
	wtr.WriteString("- Other formats may be available depending on route.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("Arguments which are not expected in the URL must be provided in the query string for *get* and *delete* requests, or as the content of the HTTP requests for *post* and *put*.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("The format of arguments in the querystring is the same used for literals in PostgreSQL.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("The recommended content type of *post* and *put* requests is `application/json`. In this case, the content of the HTTP request is either a JSON object with parameters, or an array of sub objects for batch requests. When using batch mode, several result sets will be returned. If no content type is specified, `application/x-www-form-urlencoded` is assumed.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("In addition, *get*, *delete*, and *put* requests on *relations* may accept a filter condition, sort order (except *put*), and limit (except *put*) in the query string. Those follow the [queryme](https://github.com/debackerl/queryme) format:\r\n")
	wtr.WriteString("- `")
	wtr.WriteString(g.FilterQueryName)
	wtr.WriteString("`, filter condition\r\n")
	wtr.WriteString("- `")
	wtr.WriteString(g.SortQueryName)
	wtr.WriteString("`, sort order\r\n")
	wtr.WriteString("- `")
	wtr.WriteString(g.LimitQueryName)
	wtr.WriteString("`, limit\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("### Responses\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("The cache-control and max-age settings are route specific, and will be set accordingly to the specification of routes below.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("*json format*\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("Result sets are encoded as arrays of objects.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("Batches are encoded as arrays.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("*xslx format*\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("Each result set in a batch is saved as a new sheet.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("*csv format*\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("A single result set must be returned. Batch mode is not supported.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("Fields are command separated, and text fields are double-quoted.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("*bin format*\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("Some routes may return binary data.\r\n")
	wtr.WriteString("\r\n")
	wtr.WriteString("## Resources\r\n\r\n")

	routesByUrlPath := make(map[string]*RoutesGroup)
	routesGroups := make([]*RoutesGroup, 0, 8)
	for _, route := range routes {
		var group *RoutesGroup

		if match, ok := routesByUrlPath[route.UrlPath]; ok {
			group = match
		} else {
			group = &RoutesGroup{UrlPath: route.UrlPath}
			routesByUrlPath[route.UrlPath] = group
			routesGroups = append(routesGroups, group)
		}

		switch route.Method {
		case "get":
			group.Get = true
		case "post":
			group.Post = true
		case "put":
			group.Put = true
		case "delete":
			group.Delete = true
		}
	}

	sort.Sort(RoutesByUrlPath(routesGroups))

	for _, group := range routesGroups {
		wtr.WriteString("- `")
		wtr.WriteString(group.UrlPath)
		wtr.WriteString("`")
		if group.Get {
			wtr.WriteString(" [get](#")
			wtr.WriteString(anchorName("get " + group.UrlPath))
			wtr.WriteString(")")
		}
		if group.Post {
			wtr.WriteString(" [post](#")
			wtr.WriteString(anchorName("post " + group.UrlPath))
			wtr.WriteString(")")
		}
		if group.Put {
			wtr.WriteString(" [put](#")
			wtr.WriteString(anchorName("put " + group.UrlPath))
			wtr.WriteString(")")
		}
		if group.Delete {
			wtr.WriteString(" [delete](#")
			wtr.WriteString(anchorName("delete " + group.UrlPath))
			wtr.WriteString(")")
		}
		wtr.WriteString("\r\n")
	}

	wtr.WriteString("\r\n## Routes\r\n\r\n")

	for route_i, route := range routes {
		optionals := make(map[string]struct{})
		for _, name := range route.OptionalArguments {
			optionals[name] = struct{}{}
		}

		routeArguments := make(map[string]string)
		for _, name := range routeParser.FindAllString(route.UrlPath, -1) {
			name = name[2:]

			typ := "text"
			if t, ok := route.ParametersDeclTypes[name]; ok {
				typ = t
			}

			typ = strings.TrimSuffix(typ, "[]")

			routeArguments[name] = typ
		}

		cacheControl := "private"
		maxAge := ""
		if route.IsPublic {
			cacheControl = "public"
			maxAge = fmt.Sprintf("%d sec", route.TTL)
		}

		if route_i > 0 {
			wtr.WriteString("---\r\n\r\n")
		}

		wtr.WriteString("### ")
		wtr.WriteString(route.Method)
		wtr.WriteString(" `")
		wtr.WriteString(route.UrlPath)
		wtr.WriteString("`\r\n\r\n")

		if route.Description != "" {
			wtr.WriteString(route.Description)
			wtr.WriteString("\r\n\r\n")
		}

		table := tablewriter.NewWriter(wtr)
		table.SetAutoWrapText(false)
		table.SetBorders(tblBorders)
		table.SetCenterSeparator("|")
		table.SetHeader([]string{"ID", "Kind", "Cache-Control", "Max-Age"})
		table.Append([]string{fmt.Sprintf("%d", route.RouteID), route.ObjectType, cacheControl, maxAge})
		table.Render()

		wtr.WriteString("\r\n**Arguments**\r\n\r\n")

		table = tablewriter.NewWriter(wtr)
		table.SetAutoWrapText(false)
		table.SetBorders(tblBorders)
		table.SetCenterSeparator("|")
		table.SetHeader([]string{"Name", "Type", "Optional"})
		rows := make(Rows, 0, 8)
		for name, typ := range routeArguments {
			link, err := describeType(ctx, tx, typ, typeDescriptions)
			if err != nil {
				log.Fatalln("Error: ", err)
			}

			rows.Append([]string{"`" + name + "`", link, "false"})
		}
		if route.ObjectType == "procedure" || route.Method == "post" || route.Method == "put" {
			for name, typ := range route.ParametersDeclTypes {
				isoptional := IsStringInMap(name, optionals)
				isro := IsStringInMap(name, route.ReadOnlyFields)
				_, isroutearg := routeArguments[name]

				if isro {
					if route.Method == "post" && !isoptional && !isroutearg {
						log.Fatalf("Route has a read-only field without default value, route_id %d, field %s\n", route.RouteID, name)
					}
				} else if !isroutearg {
					link, err := describeType(ctx, tx, typ, typeDescriptions)
					if err != nil {
						log.Fatalln("Error: ", err)
					}

					rows.Append([]string{"`" + name + "`", link, fmt.Sprintf("%t", isoptional)})
				}
			}
		}
		sort.Sort(rows)
		table.AppendBulk(rows)
		table.Render()

		wtr.WriteString("\r\n**Result**\r\n\r\n")

		switch route.ObjectType {
		case "relation":
			wtr.WriteString("result set:\r\n\r\n")

			table = tablewriter.NewWriter(wtr)
			table.SetAutoWrapText(false)
			table.SetBorders(tblBorders)
			table.SetCenterSeparator("|")
			table.SetHeader([]string{"Name", "Type"})
			rows = make(Rows, 0, 8)
			for name, typ := range route.ParametersDeclTypes {
				if _, ok := route.HiddenFields[name]; !ok {
					link, err := describeType(ctx, tx, typ, typeDescriptions)
					if err != nil {
						log.Fatalln("Error: ", err)
					}

					rows.Append([]string{"`" + name + "`", link})
				}
			}
			sort.Sort(rows)
			table.AppendBulk(rows)
			table.Render()

		case "procedure":
			if route.Proretset {
				wtr.WriteString("set of ")
			}

			link, err := describeType(ctx, tx, route.Prorettypname, typeDescriptions)
			if err != nil {
				log.Fatalln("Error: ", err)
			}

			wtr.WriteString(link)
			wtr.WriteString("\r\n")
		}

		if route.ContextHeaders.Status == pgtype.Present && len(route.ContextHeaders.Map) > 0 {
			wtr.WriteString("\r\n**HTTP Request Headers**\r\n\r\n")

			for httpHeaderName := range route.ContextHeaders.Map {
				wtr.WriteString("- `")
				wtr.WriteString(httpHeaderName)
				wtr.WriteString("`\r\n")
			}
		}

		if len(route.AllCookies) > 0 {
			wtr.WriteString("\r\n**Cookies**\r\n\r\n")

			table = tablewriter.NewWriter(wtr)
			table.SetAutoWrapText(false)
			table.SetBorders(tblBorders)
			table.SetCenterSeparator("|")
			table.SetHeader([]string{"Name", "Read/Write", "Domain", "Path", "HTTP Only", "Secure", "Max Age"})
			rows = make(Rows, 0, 8)
			for _, cookie := range route.AllCookies {
				rw := ""
				if cookie.Read {
					rw += "R"
				}
				if cookie.Write {
					rw += "W"
				}

				httpOnly := ""
				secure := ""
				maxAge := ""

				if cookie.Write {
					httpOnly = fmt.Sprintf("%t", cookie.HttpOnly)
					secure = fmt.Sprintf("%t", cookie.Secure)
					maxAge = fmt.Sprintf("%d sec", cookie.MaxAge)
				}

				rows.Append([]string{cookie.Name, rw, cookie.SubDomain.String, cookie.Path.String, httpOnly, secure, maxAge})
			}
			sort.Sort(rows)
			table.AppendBulk(rows)
			table.Render()
		}

		wtr.WriteString("\r\n")
	}

	wtr.WriteString("\r\n## Types\r\n\r\n")

	describedTypes := make([]string, 0, len(typeDescriptions))
	for typname, description := range typeDescriptions {
		if description != "" {
			describedTypes = append(describedTypes, typname)
		}
	}

	sort.Strings(describedTypes)

	for _, typname := range describedTypes {
		wtr.WriteString(typeDescriptions[typname])
		wtr.WriteString("\r\n")
	}
}

func describeType(ctx context.Context, tx pgx.Tx, typname string, descriptions map[string]string) (string, error) {
	isarray := strings.HasSuffix(typname, "[]")
	if isarray {
		typname = typname[:len(typname)-2]
	}

	switch typname {
	case "timestamp without time zone", "timestamp with time zone":
		typname = "timestamp"
	case "jsonb":
		typname = "json"
	case "serial":
		typname = "integer"
	case "bigserial":
		typname = "bigint"
	case "character":
		typname = "text"
	case "character varying":
		typname = "text"
	}

	description, ok := descriptions[typname]

	if !ok {
		buf := new(bytes.Buffer)
		buf.WriteString("### type `")
		buf.WriteString(typname)
		buf.WriteString("`\r\n\r\n")

		// https://www.postgresql.org/docs/9.6/static/datatype.html

		switch typname {
		case "boolean":
			description = "Boolean value"
		case "smallint":
			description = "Signed 16-bit integer number"
		case "integer":
			description = "Signed 32-bit integer number"
		case "bigint":
			description = "Signed 64-bit integer number"
		case "real":
			description = "32-bit floating point number"
		case "double precision":
			description = "64-bit floating point number"
		case "numeric", "money":
			description = "Decimal number. Encoded as a string in JSON to avoid base-2 floating point approximation error."
		case "text":
			description = "String value"
		case "date":
			description = "Date without time. Encoded in yyyy-mm-dd format."
		case "timestamp":
			description = "Date and time. Encoded in RFC3339 format."
		case "hstore":
			description = "Object with string keys and string values."
		case "json":
			description = "JSON value"
		case "bytea":
			description = "Byte array. Encoded in base64 except for binary HTTP responses."
		case "uuid":
			description = "Universally unique identifier"
		case "inet":
			description = "IPv4 or IPv6 host address"
		case "cidr":
			description = "IPv4 or IPv6 network address"
		}

		if description != "" {
			buf.WriteString(description)
			buf.WriteString("\r\n")

			description = buf.String()
		} else {
			descriptions[typname] = ""

			typtype, typrelid, typbasetype, pgdesc, err := loadTypeBasics(ctx, tx, typname)
			if err != nil {
				return "", err
			}

			if pgdesc.Status == pgtype.Present {
				buf.WriteString(pgdesc.String)
				buf.WriteString("\r\n\r\n")
			}

			switch typtype {
			case "c":
				names, types, err := loadCompositeType(ctx, tx, typname, typrelid)
				if err != nil {
					return "", err
				}

				table := tablewriter.NewWriter(buf)
				table.SetAutoWrapText(false)
				table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
				table.SetCenterSeparator("|")
				table.SetHeader([]string{"Name", "Type"})

				for i, name := range names {
					typ := types[i]

					link, err := describeType(ctx, tx, typ, descriptions)
					if err != nil {
						return "", err
					}

					table.Append([]string{name, link})
				}

				table.Render()

				description = buf.String()

			case "e":
				values, err := loadEnumType(ctx, tx, typname)
				if err != nil {
					return "", err
				}

				buf.WriteString("Possible values:\r\n\r\n")

				for _, value := range values {
					buf.WriteString("- `\"")
					buf.WriteString(value)
					buf.WriteString("\"`\r\n")
				}

				description = buf.String()

			case "d":
				if typbasetype.Status == pgtype.Present {
					link, err := describeType(ctx, tx, typbasetype.String, descriptions)
					if err != nil {
						return "", err
					}

					buf.WriteString("Base type: ")
					buf.WriteString(link)
					buf.WriteString("\r\n")

					description = buf.String()
				}
			}
		}

		descriptions[typname] = description
	}

	suffix := ""
	if isarray {
		suffix = "[]"
	}

	if description != "" {
		return fmt.Sprintf("[%s](#%s)%s", typname, anchorName("type "+typname), suffix), nil
	} else {
		return typname + suffix, nil
	}
}

func loadCompositeType(ctx context.Context, tx pgx.Tx, typename string, typrelid pgtype.OID) (names []string, types []string, err error) {
	rows, err := tx.Query(ctx, `SELECT a.attname, t.typname FROM pg_attribute a INNER JOIN pg_type t ON t.oid = a.atttypid WHERE attrelid = $1 ORDER BY a.attnum`, typrelid)
	if err != nil {
		return
	}
	defer rows.Close()

	names = make([]string, 0, 4)
	types = make([]string, 0, 4)

	for rows.Next() {
		var name, typ string
		if err = rows.Scan(&name, &typ); err != nil {
			return
		}

		names = append(names, name)
		types = append(types, typ)
	}

	return
}

func loadEnumType(ctx context.Context, tx pgx.Tx, typename string) (values []string, err error) {
	rows, err := tx.Query(ctx, `SELECT array_agg(enumlabel ORDER BY enumsortorder)::text[] FROM pg_enum WHERE enumtypid = $1::regtype::oid`, typename)
	if err != nil {
		return
	}
	defer rows.Close()

	values = make([]string, 0, 4)

	if rows.Next() {
		if err = rows.Scan(&values); err != nil {
			return
		}
	} else {
		values = []string{}
	}

	return
}

func loadTypeBasics(ctx context.Context, tx pgx.Tx, typname string) (typtype string, typrelid pgtype.OID, typbasetype pgtype.Text, description pgtype.Text, err error) {
	rows, err := tx.Query(ctx, `SELECT typtype, typrelid, typbasetype::regtype, obj_description(oid) FROM pg_type WHERE typname = $1`, typname)
	if err != nil {
		return
	}
	defer rows.Close()

	if !rows.Next() {
		return
	}

	if err = rows.Scan(&typtype, &typrelid, &typbasetype, &description); err != nil {
		return
	}

	return
}

func IsStringInMap(x string, xs map[string]struct{}) bool {
	_, ok := xs[x]
	return ok
}

func anchorName(s string) string {
	s = strings.ToLower(s)
	s = strings.Replace(s, " ", "-", -1)
	return regexp.MustCompile("[^0-9a-z\\-_]").ReplaceAllString(s, "")
}

type RoutesGroup struct {
	UrlPath string
	Get     bool
	Post    bool
	Put     bool
	Delete  bool
}

type RoutesByUrlPath []*RoutesGroup

func (rs RoutesByUrlPath) Len() int {
	return len(rs)
}

func (rs RoutesByUrlPath) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func (rs RoutesByUrlPath) Less(i, j int) bool {
	return rs[i].UrlPath < rs[j].UrlPath
}

type Rows [][]string

func (rs *Rows) Append(fields []string) {
	*rs = append(*rs, fields)
}

func (rs Rows) Len() int {
	return len(rs)
}

func (rs Rows) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func (rs Rows) Less(i, j int) bool {
	return rs[i][0] < rs[j][0]
}
