package tcpclient

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// ConnectionEventHandler defines callbacks for connection events
type ConnectionEventHandler interface {
	ConnectionClosed(handler *ClientConnectionHandler)
	AfterEveryConnection(handler *ClientConnectionHandler)
	OnWriteFailure(handler *ClientConnectionHandler) bool
	DoSignOn(handler *ClientConnectionHandler)
	DoSignOff(handler *ClientConnectionHandler)
	DoDKEProcess(handler *ClientConnectionHandler)
	DoEchoTest(handler *ClientConnectionHandler)
	DoCutOver(handler *ClientConnectionHandler)
}

// NewClientConnectionHandler creates a new client connection handler
func NewClientConnectionHandler(connBean *ConnectionBean, socket net.Conn) (*ClientConnectionHandler, error) {
	if socket == nil {
		return nil, errors.New("socket cannot be nil")
	}

	handler := &ClientConnectionHandler{
		connBean:      connBean,
		socket:        socket,
		retryRequired: true,
	}

	remoteAddr := socket.RemoteAddr().(*net.TCPAddr)
	handler.connBean.IPAddress = remoteAddr.IP.String()
	handler.connBean.Port = remoteAddr.Port
	handler.connBean.ConnectionName = handler.getIPAddressAndPort()
	if err := handler.initializeConnections(); err != nil {
		return nil, err
	}

	return handler, nil
}

// NewClientConnection creates a client connection without an existing socket
func NewClientConnection(connBean *ConnectionBean) *ClientConnectionHandler {
	return &ClientConnectionHandler{
		connBean:      connBean,
		retryRequired: true,
	}
}

// Connect establishes a connection to the server
func (c *ClientConnectionHandler) Connect() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connectionStatus == ConnectionEstablished {
		return true, nil
	}

	var conn net.Conn
	var err error

	address := fmt.Sprintf("%s:%d", c.connBean.IPAddress, c.connBean.Port)
	log.Printf("Connecting to %s", address)

	// Establish TCP connection
	dialer := &net.Dialer{
		Timeout: time.Duration(c.connBean.TimeoutPeriod) * time.Second,
	}

	if c.connBean.TLSConfig.Enabled {
		// TLS configuration
		tlsConfig := &tls.Config{
			ServerName:         c.connBean.TLSConfig.ServerName,
			InsecureSkipVerify: c.connBean.TLSConfig.SkipVerify,
		}

		// Load client certificate if specified
		if c.connBean.TLSConfig.CertFile != "" && c.connBean.TLSConfig.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.connBean.TLSConfig.CertFile, c.connBean.TLSConfig.KeyFile)
			if err != nil {
				return false, fmt.Errorf("failed to load client certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		conn, err = tls.DialWithDialer(dialer, "tcp", address, tlsConfig)
	} else {
		conn, err = dialer.Dial("tcp", address)
	}

	if err != nil {
		c.connectionStatus = ConnectionDisconnected
		return false, fmt.Errorf("connection failed: %v", err)
	}

	c.socket = conn
	c.connectionStatus = ConnectionEstablished
	c.stopReconnection = false

	if err := c.initializeConnections(); err != nil {
		return false, err
	}

	return true, nil
}

// initializeConnections sets up the connection streams and starts background processes
func (c *ClientConnectionHandler) initializeConnections() error {
	if c.socket == nil {
		c.connectionStatus = ConnectionNotEstablished
		return errors.New("socket is not initialized")
	}

	// Configure socket options
	if err := c.socket.(*net.TCPConn).SetKeepAlive(c.connBean.KeepAliveFlag); err != nil {
		return fmt.Errorf("failed to set keepalive: %v", err)
	}

	// Create buffered streams
	c.bis = bufio.NewReader(c.socket)
	c.bos = bufio.NewWriter(c.socket)

	// Start async reader if configured
	if c.connBean.ReadMode == ReadAsync {
		if err := c.startAsyncReader(); err != nil {
			return err
		}
	}
	return nil
}

// WriteRead sends data and waits for a response (synchronous)
func (c *ClientConnectionHandler) WriteRead(input []byte) ([]byte, error) {
	// Establish connection
	if c.connBean.Persistent {
		if _, err := c.Connect(); err != nil {
			return nil, err
		}
		defer c.CloseConnection(ReasonNonPersistent) // Ensure the connection is closed after operation
	}
	if err := c.WriteBytes(input); err != nil {
		return nil, err
	}
	response, err := c.readBytes()
	if err != nil {
		return nil, err
	}
	return c.checkIncomingMessage(response)
}

// WriteBytes sends data to the connection
func (c *ClientConnectionHandler) WriteBytes(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connectionStatus != ConnectionEstablished {
		return errors.New("connection is not established")
	}

	c.lastHitInMillis = time.Now().UnixMilli()

	dataToWrite := c.calculateLength(data)
	if _, err := c.bos.Write(dataToWrite); err != nil {
		return fmt.Errorf("write failed: %v", err)
	}

	if err := c.bos.Flush(); err != nil {
		return fmt.Errorf("flush failed: %v", err)
	}

	log.Printf("Sent %d bytes", len(dataToWrite))
	return nil
}

// readBytes reads data from the connection
func (c *ClientConnectionHandler) readBytes() ([]byte, error) {
	if c.connBean.LengthType == "NO_LENGTH" {
		buffer := make([]byte, 15360)
		n, err := c.bis.Read(buffer)
		if err != nil {
			return nil, err
		}
		return buffer[:n], nil
	}

	// Read length header
	header := make([]byte, c.connBean.LengthOfLength)
	if _, err := io.ReadFull(c.bis, header); err != nil {
		return nil, err
	}

	messageLength := c.getMessageLength(header)
	if messageLength <= 0 {
		return nil, errors.New("invalid message length")
	}

	// Read message body
	message := make([]byte, messageLength)
	if _, err := io.ReadFull(c.bis, message); err != nil {
		return nil, err
	}

	log.Printf("Received %d bytes", len(message))
	return message, nil
}

// checkIncomingMessage validates the message against the length header
func (c *ClientConnectionHandler) checkIncomingMessage(message []byte) ([]byte, error) {
	if c.connBean.LengthType == "NO_LENGTH" {
		return message, nil
	}

	if len(message) < c.connBean.LengthOfLength {
		return nil, errors.New("message too short for length header")
	}

	header := message[:c.connBean.LengthOfLength]
	body := message[c.connBean.LengthOfLength:]

	expectedLength := c.getMessageLength(header)
	if expectedLength != len(body) {
		return nil, fmt.Errorf("message length mismatch: expected %d, got %d", expectedLength, len(body))
	}

	return body, nil
}

// getMessageLength extracts the message length from the header based on format
func (c *ClientConnectionHandler) getMessageLength(header []byte) int {
	switch c.connBean.LengthFormat {
	case FormatASCII:
		lengthStr := string(header)
		var length int
		fmt.Sscanf(lengthStr, "%d", &length)
		return length
	case FormatBinary:
		length := 0
		for _, b := range header {
			length = length<<8 | int(b)
		}
		return length
	// Implement other formats similarly
	default:
		return 0
	}
}

// calculateLength adds length header to the message
func (c *ClientConnectionHandler) calculateLength(message []byte) []byte {
	if c.connBean.LengthType == "NO_LENGTH" {
		return message
	}

	length := len(message)
	if c.connBean.LengthType[2] == 'L' {
		length += c.connBean.LengthOfLength
	}

	var header []byte
	switch c.connBean.LengthFormat {
	case FormatASCII:
		header = []byte(fmt.Sprintf("%0*d", c.connBean.LengthOfLength, length))
	case FormatBinary:
		header = make([]byte, c.connBean.LengthOfLength)
		for i := range header {
			shift := 8 * (c.connBean.LengthOfLength - 1 - i)
			header[i] = byte((length >> shift) & 0xff)
		}
	}

	if header == nil {
		return message
	}

	return append(header, message...)
}

// CloseConnection terminates the connection
func (c *ClientConnectionHandler) CloseConnection(reason CloseConnectionReason) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connectionStatus != ConnectionEstablished {
		return
	}

	log.Printf(" Closing connection due to %s", reason)

	c.stopReconnection = true
	if c.reader != nil {
		c.reader.Stop()
		c.readerWg.Wait()
	}

	if err := c.socket.Close(); err != nil {
		log.Printf(" Error closing socket: %v", err)
	}
	c.connectionStatus = ConnectionDisconnected
	c.connBean.ConnectionEvent.ConnectionClosed(c)
	log.Printf(" Connection closed")
}

// startAsyncReader begins asynchronous reading
func (c *ClientConnectionHandler) startAsyncReader() error {
	c.reader = NewAsyncRead(c.connBean, c)
	c.readerWg.Add(1)
	go func() {
		defer c.readerWg.Done()
		c.reader.Run()
	}()
	return nil
}

// getIPAddressAndPort returns the connection address as a string
func (c *ClientConnectionHandler) getIPAddressAndPort() string {
	return fmt.Sprintf("%s:%d", c.connBean.IPAddress, c.connBean.Port)
}

// NewAsyncRead creates a new async reader
func NewAsyncRead(connBean *ConnectionBean, handler *ClientConnectionHandler) *AsyncRead {
	return &AsyncRead{
		connBean: connBean,
		handler:  handler,
		stopChan: make(chan struct{}),
	}
}

// Run starts the async reading loop
func (a *AsyncRead) Run() {
	for {
		select {
		case <-a.stopChan:
			return
		default:
			data, err := a.handler.readBytes()
			if err != nil {
				log.Printf(": Async read error: %v", err)
				if a.handler.connBean.ConnectionEvent.OnWriteFailure(a.handler) {
					a.handler.CloseConnection(ReasonError)
				}
				return
			}

			processed, err := a.handler.checkIncomingMessage(data)
			if err != nil {
				log.Printf(": Message validation error: %v", err)
				continue
			}

			select {
			case a.connBean.Queue <- processed:
			default:
				log.Printf(": Queue full, dropping message")
			}
		}
	}
}

// Stop signals the async reader to terminate
func (a *AsyncRead) Stop() {
	close(a.stopChan)
}
