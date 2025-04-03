package tcpcommon

import (
	"bufio"
	"net"
	"sync"
	"time"
)

// TLSConfig holds TLS configuration parameters.
type TLSConfig struct {
	Enabled    bool
	CertFile   string
	KeyFile    string
	CAFile     string
	ServerName string
	SkipVerify bool
}

// // ConnectionBean contains connection configuration for a TCP connection.
type ConnectionBean struct {
	IPAddress               string
	Port                    int
	ConnectionName          string
	TimeoutPeriod           int          // In seconds
	TLSConfig               TLSConfig    // TLS settings
	Persistent              bool         // Persistent or one shot connection
	KeepAliveFlag           bool         // Enable TCP keep-alive
	ReadMode                ReadMode     // Sync or async reading
	LengthType              string       // e.g., "LL", "LLLL", "NO_LENGTH"
	LengthOfLength          int          // Size of length header in bytes
	LengthFormat            LengthFormat // Format of length header
	Queue                   chan []byte  // Queue to hold async messages
	ConnectionEvent         ConnectionEventHandler
	KeepAliveThread         *time.Ticker // For keep-alive pings
	SocketReconnectInterval int          // Reconnect interval in seconds
}

// ClientConnectionHandler manages a TCP connections.
type ClientConnectionHandler struct {
	connBean      *ConnectionBean
	socket        net.Conn
	reader        *bufio.Reader
	writer        *bufio.Writer
	lastActivity  int64            // Last activity timestamp (Unix millis)
	status        ConnectionStatus // Current connection state
	stopReconnect bool             // Stop reconnection attempts
	stopKeepAlive bool             // Stop keep-alive goroutine
	retryRequired bool             // Retry connection on failure
	asyncReader   *AsyncRead       // Async reader instance
	readerWg      sync.WaitGroup   // Wait group for async reader
	keepAliveWg   sync.WaitGroup   // Wait group for keep-alive
	mu            sync.Mutex       // to protect connection state
	lengthHandler LengthHandler    // Custom length handler
}

// LengthHandler defines methods for custom length encoding/decoding.
type LengthHandler interface {
	ExtractLength(header []byte, lengthType string) int
	ConstructWithLength(message []byte, lengthOfLength int, lengthType string) []byte
}

// // AsyncRead handles asynchronous reading from the connection.
type AsyncRead struct {
	connBean *ConnectionBean
	handler  *ClientConnectionHandler
	stopChan chan struct{}
}
