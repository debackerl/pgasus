package main

import (
	"gopkg.in/alecthomas/kingpin.v1"
	"github.com/naoina/toml"
	"gopkg.in/tylerb/graceful.v1"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"log"
	"os"
	"io/ioutil"
	"runtime"
	"time"
)

var (
	appCmdLine = kingpin.New("goml", "PostgreSQL-fueled middle layer.")
	configPathArg = appCmdLine.Flag("config", "Path to configuration file.").Required().String()
)

var config struct {
	System struct {
		Maxprocs int
	}

	Http struct {
		Address string
		MaxOpenConnections int
		Key string
		Cert string
		RequestsLogFile string
		ClientCa string
		DefaultClientCn string
		UpdateForwardedForHeader bool
		MaxHeaderSizeKbytes int
		MaxBodySizeKbytes int64
		MaxResponseSizeKbytes int64
		ReadTimeoutSecs int
		WriteTimeoutSecs int
		ShutdownTimeoutSecs int
	}
	
	Postgres struct {
		Socket string
		Database string
		SearchPath string
		MaxOpenConnections int
		ContextParameterName string
		RoutesTableName string
		FtsFunctionName string
	}
	
	Protocol struct {
		FilterQueryName string
		SortQueryName string
		LimitQueryName string
	}
}

func loadConfig(path string) {
	// default values
	config.Http.Address = ":https"
	config.Http.ReadTimeoutSecs = 10
	config.Http.WriteTimeoutSecs = 10
	config.Http.ShutdownTimeoutSecs = 60
	config.Postgres.ContextParameterName = "goml"
	config.Postgres.RoutesTableName = "routes"
	
	f, err := os.Open(path)
	if err != nil {
		log.Fatalln("Cannot open configuration file:", err)
	}
	defer f.Close()
	
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("Cannot read configuration file:", err)
	}
	
	if err := toml.Unmarshal(buf, &config); err != nil {
		log.Fatalln("Cannot decode configuration file:", err)
	}
}

func main() {
	kingpin.MustParse(appCmdLine.Parse(os.Args[1:]))
	
	// On SIGHUP, parent process reforks, and config is reloaded,
	// however original port is still in use.
	
	loadConfig(*configPathArg)
	
	runtime.GOMAXPROCS(config.System.Maxprocs)
	
	var handler RequestHandler
	handler.Socket = config.Postgres.Socket
	handler.Database = config.Postgres.Database
	handler.SearchPath = config.Postgres.SearchPath
	handler.MaxOpenConnections = config.Postgres.MaxOpenConnections
	handler.ContextParameterName = config.Postgres.ContextParameterName
	handler.RoutesTableName = config.Postgres.RoutesTableName
	handler.FtsFunctionName = config.Postgres.FtsFunctionName
	handler.DefaultCn = config.Http.DefaultClientCn
	handler.UpdateForwardedForHeader = config.Http.UpdateForwardedForHeader
	handler.MaxBodySizeKbytes = config.Http.MaxBodySizeKbytes
	handler.MaxResponseSizeKbytes = config.Http.MaxResponseSizeKbytes
	handler.FilterQueryName = config.Protocol.FilterQueryName
	handler.SortQueryName = config.Protocol.SortQueryName
	handler.LimitQueryName = config.Protocol.LimitQueryName
	
	if config.Http.RequestsLogFile != "" {
		if err := handler.OpenRequestsLogFile(config.Http.RequestsLogFile); err != nil {
			log.Fatalln(err)
		}
	}
	
	if err := handler.Load(); err != nil {
		log.Fatalln(err)
	}
	
	certPool := x509.NewCertPool()
	if config.Http.ClientCa != "" {
		rootCAs, err := ioutil.ReadFile(config.Http.ClientCa)
		if err != nil {
			log.Fatalln("Cannot open root CAs file:", err)
		}
		
		if !certPool.AppendCertsFromPEM(rootCAs) {
			log.Fatalln("Could not load client root CAs.")
		}
	}
	
	svr := 	&graceful.Server {
		Timeout: time.Duration(config.Http.ShutdownTimeoutSecs) * time.Second,
		ListenLimit: config.Http.MaxOpenConnections,
		ShutdownInitiated: func() { log.Println("Goml shutdown requested.") },
		Server: &http.Server {
			Addr:           config.Http.Address,
			Handler:        &handler,
			ReadTimeout:    time.Duration(config.Http.ReadTimeoutSecs) * time.Second,
			WriteTimeout:   time.Duration(config.Http.WriteTimeoutSecs) * time.Second,
			MaxHeaderBytes: config.Http.MaxHeaderSizeKbytes << 10,
			TLSConfig:      &tls.Config {
				ClientCAs:  certPool,
			},
		},
	}
	
	log.Println("Goml started.")
	
	if config.Http.Key != "" && config.Http.Cert != "" {
		if err := svr.ListenAndServeTLS(config.Http.Cert, config.Http.Key); err != nil {
			log.Fatalln(err)
		}
	} else {
		if err := svr.ListenAndServe(); err != nil {
			log.Fatalln(err)
		}
	}
	
	handler.CloseRequestsLogFile()
}
