package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/naoina/toml"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	appCmdLine        = kingpin.New("pgasus", "PostgreSQL API Server for Universal Stack.")
	configPathArg     = appCmdLine.Flag("config", "Path to configuration file.").Required().String()
	serveCmd          = appCmdLine.Command("serve", "Start server.")
	genDocCmd         = appCmdLine.Command("gendoc", "Generate documentation.")
	docOutputPathArg  = genDocCmd.Arg("outputPath", "Destination file.").Required().String()
	serverCertificate *x509.Certificate
)

var config struct {
	System struct {
		Maxprocs int
		Verbose  bool
	}

	Http struct {
		Address   string
		UrlPrefix string
		//MaxOpenConnections int
		Key                      string
		Cert                     string
		RequestsLogFile          string
		ClientCa                 string
		DefaultClientCn          string
		UpdateForwardedForHeader bool
		MaxHeaderSizeKbytes      int
		MaxBodySizeKbytes        int64
		MaxResponseSizeKbytes    int64
		ReadTimeoutSecs          int
		WriteTimeoutSecs         int
		//ShutdownTimeoutSecs int
		CookiesDomain        string
		CookiesPath          string
		CookiesDisableSecure bool
	}

	Postgres struct {
		Socket               string
		Port                 uint16
		ServerCertificate    string
		CaCertificates       string
		Database             string
		UpdatesChannelName   string
		SearchPath           string
		MaxOpenConnections   int32
		ContextParameterName string
		RoutesTableName      string
		FtsFunctionName      string
		StatementTimeoutSecs int
	}

	Protocol struct {
		FilterQueryName string
		SortQueryName   string
		LimitQueryName  string
	}

	DefaultContext map[string]string

	BinaryFormats []struct {
		Extension string
		MimeType  string
	}
}

func loadConfig(path string) {
	// default values
	config.Http.Address = ":https"
	config.Http.ReadTimeoutSecs = 10
	config.Http.WriteTimeoutSecs = 10
	//config.Http.ShutdownTimeoutSecs = 60
	config.Postgres.ContextParameterName = "context"
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

func checkServerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if len(rawCerts) != 1 {
		return fmt.Errorf("One server certificate expected, %d received", len(rawCerts))
	}

	cert, err := x509.ParseCertificate(rawCerts[0])
	if err == nil {
		if cert.Equal(serverCertificate) {
			return nil
		}
	}

	return errors.New("Invalid server certificate.")
}

func loadServerCertificate(path string) {
	certPem, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalln("Cannot read server certificate:", err)
	}

	for len(certPem) > 0 {
		var block *pem.Block
		block, certPem = pem.Decode(certPem)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}

		serverCertificate = cert
		certPem = []byte{}
	}
}

func loadX509Pool(caPath string) *x509.CertPool {
	certPool := x509.NewCertPool()

	if caPath != "" {
		rootCAs, err := ioutil.ReadFile(caPath)
		if err != nil {
			log.Fatalln("Cannot open root CAs file:", err)
		}

		if !certPool.AppendCertsFromPEM(rootCAs) {
			log.Fatalln("Could not load client root CAs.")
		}
	}

	return certPool
}

func main() {
	cmd := kingpin.MustParse(appCmdLine.Parse(os.Args[1:]))

	loadConfig(*configPathArg)

	runtime.GOMAXPROCS(config.System.Maxprocs)

	var tlsConfig *tls.Config
	if config.Postgres.Socket[0] != '/' {
		// when not using Unix sockets, then TLS is required

		insecureSkipVerify := false
		var verifyPeerCertificate func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error
		serverName := config.Postgres.Socket
		var rootCAs *x509.CertPool

		if config.Postgres.ServerCertificate != "" {
			loadServerCertificate(config.Postgres.ServerCertificate)
			serverName = ""
			verifyPeerCertificate = checkServerCertificate
			insecureSkipVerify = true
		} else if config.Postgres.CaCertificates != "" {
			rootCAs = loadX509Pool(config.Postgres.CaCertificates)
		}

		tlsConfig = &tls.Config{
			InsecureSkipVerify:    insecureSkipVerify,
			VerifyPeerCertificate: verifyPeerCertificate,
			ServerName:            serverName,
			RootCAs:               rootCAs,
		}
	}

	var handler RequestHandler
	// as recommended by config, _ := pgxpool.ParseConfig("")
	handler.DbConnConfig, _ = pgx.ParseConfig("")
	handler.DbConnConfig.Host = config.Postgres.Socket
	handler.DbConnConfig.Port = config.Postgres.Port
	handler.DbConnConfig.User = os.Getenv("PG_USER")
	handler.DbConnConfig.Password = os.Getenv("PG_PASSWORD")
	handler.DbConnConfig.Database = config.Postgres.Database
	handler.DbConnConfig.TLSConfig = tlsConfig

	handler.Schema = Schema{
		CookiesDomain:        config.Http.CookiesDomain,
		CookiesPath:          config.Http.CookiesPath,
		CookiesDisableSecure: config.Http.CookiesDisableSecure,
		RoutesTableName:      config.Postgres.RoutesTableName,
	}
	handler.Verbose = config.System.Verbose
	handler.UrlPrefix = config.Http.UrlPrefix
	handler.UpdatesChannelName = config.Postgres.UpdatesChannelName
	handler.SearchPath = config.Postgres.SearchPath
	handler.MaxOpenConnections = config.Postgres.MaxOpenConnections
	handler.ContextParameterName = config.Postgres.ContextParameterName
	handler.FtsFunctionName = config.Postgres.FtsFunctionName
	handler.StatementTimeoutSecs = config.Postgres.StatementTimeoutSecs
	handler.DefaultCn = config.Http.DefaultClientCn
	handler.UpdateForwardedForHeader = config.Http.UpdateForwardedForHeader
	handler.MaxBodySizeKbytes = config.Http.MaxBodySizeKbytes
	handler.MaxResponseSizeKbytes = config.Http.MaxResponseSizeKbytes
	handler.FilterQueryName = config.Protocol.FilterQueryName
	handler.SortQueryName = config.Protocol.SortQueryName
	handler.LimitQueryName = config.Protocol.LimitQueryName
	handler.DefaultContext = config.DefaultContext

	handler.BinaryFormats = make(map[string]string)
	for _, x := range config.BinaryFormats {
		handler.BinaryFormats[x.Extension] = x.MimeType
	}

	switch cmd {
	case serveCmd.FullCommand():
		startServer(handler)
	case genDocCmd.FullCommand():
		generateDocumentation(handler)
	default:
		log.Fatalln("No command provided.")
	}
}

func startServer(handler RequestHandler) {
	if config.Http.RequestsLogFile != "" {
		if err := handler.OpenRequestsLogFile(config.Http.RequestsLogFile); err != nil {
			log.Fatalln(err)
		}
	}

	if err := handler.Load(); err != nil {
		log.Fatalln(err)
	}

	certPool := loadX509Pool(config.Http.ClientCa)

	svr := http.Server{
		Addr:           config.Http.Address,
		Handler:        &handler,
		ReadTimeout:    time.Duration(config.Http.ReadTimeoutSecs) * time.Second,
		WriteTimeout:   time.Duration(config.Http.WriteTimeoutSecs) * time.Second,
		MaxHeaderBytes: config.Http.MaxHeaderSizeKbytes << 10,
		// disable HTTP/2 (issue with Kubernetes probe when HTTPS scheme is enabled)
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)),
		TLSConfig: &tls.Config{
			ClientCAs: certPool,
		},
	}

	svr.RegisterOnShutdown(func() {
		if config.System.Verbose {
			log.Println("pgasus shutdown requested.")
		}
		handler.StopReloads()
	})

	idleConnsClosed := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint

		if err := svr.Shutdown(context.Background()); err != nil {
			log.Printf("Error while shutting down: %v", err)
		}

		close(idleConnsClosed)
	}()

	if config.System.Verbose {
		log.Println("pgasus started.")
	}

	if config.Http.Key != "" && config.Http.Cert != "" {
		if err := svr.ListenAndServeTLS(config.Http.Cert, config.Http.Key); err != http.ErrServerClosed {
			log.Fatalln(err)
		}
	} else {
		if err := svr.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalln(err)
		}
	}

	<-idleConnsClosed // wait for last connections to close
	handler.CloseRequestsLogFile()
}

func generateDocumentation(handler RequestHandler) {
	docGen := DocumentationGenerator{
		DbConnConfig:    handler.DbConnConfig,
		Schema:          handler.Schema,
		SearchPath:      config.Postgres.SearchPath,
		FilterQueryName: config.Protocol.FilterQueryName,
		SortQueryName:   config.Protocol.SortQueryName,
		LimitQueryName:  config.Protocol.LimitQueryName,
	}

	docGen.GenerateDocumentation(*docOutputPathArg)
}
