package tcpclient

import (
	"bufio"
	"net"
	"sync"
	"time"
)

// TLSConfig holds TLS configuration parameters
type TLSConfig struct {
	Enabled    bool
	CertFile   string
	KeyFile    string
	CAFile     string
	ServerName string
	SkipVerify bool
	KeyAlias   string
}

// ConnectionBean contains connection configuration
type ConnectionBean struct {
	IPAddress               string
	Port                    int
	ConnectionName          string
	TimeoutPeriod           int
	TLSConfig               TLSConfig
	Persistent              bool
	KeepAliveFlag           bool
	ReadMode                ReadMode
	LengthType              string
	LengthOfLength          int
	LengthFormat            LengthFormat
	Queue                   chan []byte
	ConnectionEvent         ConnectionEventHandler
	KeepAliveThread         *time.Ticker
	SocketReconnectInterval int
}

// ClientConnectionHandler manages TCP connections
type ClientConnectionHandler struct {
	connBean         *ConnectionBean
	socket           net.Conn
	bis              *bufio.Reader
	bos              *bufio.Writer
	lastHitInMillis  int64
	connectionStatus ConnectionStatus
	stopReconnection bool
	stopKeepAlive    bool
	retryRequired    bool
	reader           *AsyncRead
	readerWg         sync.WaitGroup
	keepAliveWg      sync.WaitGroup
	mu               sync.Mutex
}

// LengthHandler handles custom length formats
type LengthHandler interface {
	ExtractLength(header []byte, lengthType string) int
	ConstructWithLength(message []byte, lengthOfLength int, lengthType string) []byte
}

// AsyncRead handles asynchronous reading
type AsyncRead struct {
	connBean *ConnectionBean
	handler  *ClientConnectionHandler
	stopChan chan struct{}
}
