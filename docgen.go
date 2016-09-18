package main

import (
	"github.com/jackc/pgx"
	"github.com/olekukonko/tablewriter"
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
)

type DocumentationGenerator struct {
	DbConnConfig pgx.ConnConfig
	Schema Schema
	SearchPath string
}

func (g *DocumentationGenerator) GenerateDocumentation(outputPath string) {
	var err error
	
	routeParser := regexp.MustCompile("/[:*][^/]+")
	tblBorders := tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false}
	
	var db *pgx.Conn
	db, err = pgx.Connect(g.DbConnConfig)
	if err != nil {
		log.Fatalln("Could not connect to database:", err)
	}
	
	var tx *pgx.Tx
	tx, err = db.Begin()
	if err != nil {
		log.Fatalln("Could not begin transaction:", err)
	}
	defer tx.Rollback()
	
	var routes []*Route
	routes, err = g.Schema.LoadRoutes(tx, g.SearchPath)
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
	
	wtr.WriteString("# API Specification\r\n\r\n")
	
	wtr.WriteString("## Routes\r\n\r\n")
	
	for _, route := range routes {
		optionals := make(map[string]struct{})
		for _, name := range route.OptionalArguments {
			optionals[name] = struct{}{}
		}
		
		routeArguments := make(map[string]string)
		for _, name := range routeParser.FindAllString(route.UrlPath, -1) {
			name = name[2:]
			
			typ := "text"
			if t, ok := route.ParametersTypes[name]; ok {
				typ = t
			}
			
			routeArguments[name] = strings.TrimSuffix(typ, "[]")
		}
		
		wtr.WriteString("### ")
		wtr.WriteString(route.Method)
		wtr.WriteString(" `")
		wtr.WriteString(route.UrlPath)
		wtr.WriteString("`\r\n\r\n")
		
		if route.Description != "" {
			wtr.WriteString(route.Description)
			wtr.WriteString("\r\n\r\n===\r\n\r\n")
		}
		
		table := tablewriter.NewWriter(wtr)
		table.SetBorders(tblBorders)
		table.SetCenterSeparator("|")
		table.SetHeader([]string{"ID", "Kind", "Public", "TTL"})
		table.Append([]string{fmt.Sprintf("%d", route.RouteID), route.ObjectType, fmt.Sprintf("%t", route.IsPublic), fmt.Sprintf("%d sec", route.TTL)})
		table.Render()
		
		wtr.WriteString("\r\n**Arguments**\r\n\r\n")
		
		table = tablewriter.NewWriter(wtr)
		table.SetBorders(tblBorders)
		table.SetCenterSeparator("|")
		table.SetHeader([]string{"Name", "Type", "Optional"})
		rows := make(Rows, 0, 8)
		for name, typ := range routeArguments {
			rows.Append([]string{"`" + name + "`", "`" + typ + "`", "false"})
		}
		if route.ObjectType == "procedure" || route.Method == "post" || route.Method == "put" {
			for name, typ := range route.ParametersTypes {
				isoptional := IsStringInMap(name, optionals)
				isro := IsStringInMap(name, route.ReadOnlyFields)
				_, isroutearg := routeArguments[name]
				
				if isro {
					if route.Method == "post" && !isoptional && !isroutearg {
						log.Fatalf("Route has read-only field without default value, route_id %d, field %s\n", route.RouteID, name)
					}
				} else if !isroutearg {
					rows.Append([]string{"`" + name + "`", "`" + typ + "`", fmt.Sprintf("%t", isoptional)})
				}
			}
		}
		sort.Sort(rows)
		table.AppendBulk(rows)
		table.Render()
		
		wtr.WriteString("\r\n**Result**\r\n\r\n")
		
		switch route.ObjectType {
		case "relation":
			table = tablewriter.NewWriter(wtr)
			table.SetBorders(tblBorders)
			table.SetCenterSeparator("|")
			table.SetHeader([]string{"Name", "Type"})
			rows = make(Rows, 0, 8)
			for name, typ := range route.ParametersTypes {
				if _, ok := route.HiddenFields[name]; !ok {
					rows.Append([]string{"`" + name + "`", "`" + typ + "`"})
				}
			}
			sort.Sort(rows)
			table.AppendBulk(rows)
			table.Render()
			
		case "procedure":
			if route.Proretset {
				wtr.WriteString("set of ")
			}
			
			wtr.WriteString("`")
			wtr.WriteString(route.Prorettypname)
			wtr.WriteString("`\r\n")
		}
		
		if route.ContextHeaders.Valid && len(route.ContextHeaders.Hstore) > 0 {
			wtr.WriteString("\r\n**HTTP Request Headers**\r\n\r\n")
			
			for httpHeaderName := range route.ContextHeaders.Hstore {
				wtr.WriteString("- `")
				wtr.WriteString(httpHeaderName)
				wtr.WriteString("`\r\n")
			}
		}
		
		if len(route.AllCookies) > 0 {
			wtr.WriteString("\r\n**Cookies**\r\n\r\n")
			
			table = tablewriter.NewWriter(wtr)
			table.SetBorders(tblBorders)
			table.SetCenterSeparator("|")
			table.SetHeader([]string{"Name", "Read/Write", "Domain", "Path", "HTTP Only", "Secure", "Max Age"})
			rows = make(Rows, 0, 8)
			for _, cookie := range route.AllCookies {
				rw := ""
				if cookie.Read { rw += "R" }
				if cookie.Write { rw += "W" }
				
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
}

func IsStringInMap(x string, xs map[string]struct{}) bool {
	_, ok := xs[x]
	return ok
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
