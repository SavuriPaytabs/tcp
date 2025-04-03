package tcpcommon

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

// ConnectionEventHandler defines callbacks for connection events.
type ConnectionEventHandler interface {
	ConnectionClosed(handler *ClientConnectionHandler)
	AfterEveryConnection(handler *ClientConnectionHandler)
	OnWriteFailure(handler *ClientConnectionHandler) bool // Return true to close connection
	DoSignOn(handler *ClientConnectionHandler)
	DoSignOff(handler *ClientConnectionHandler)
	DoDKEProcess(handler *ClientConnectionHandler)
	DoEchoTest(handler *ClientConnectionHandler)
	DoCutOver(handler *ClientConnectionHandler)
}

// NewClientConnectionHandler creates a handler with an existing socket.
func NewClientConnectionHandler(connBean *ConnectionBean, socket net.Conn) (*ClientConnectionHandler, error) {
	if socket == nil {
		return nil, errors.New("socket cannot be nil")
	}

	handler := &ClientConnectionHandler{
		connBean:      connBean,
		socket:        socket,
		status:        ConnectionNotEstablished,
		retryRequired: true,
	}

	if err := handler.initializeConnection(); err != nil {
		return nil, err
	}

	handler.status = ConnectionEstablished
	handler.connBean.ConnectionName = handler.getAddress()
	return handler, nil
}

// NewClientConnection creates a handler without an existing socket.
func NewClientConnection(connBean *ConnectionBean) *ClientConnectionHandler {
	handler := &ClientConnectionHandler{
		connBean:      connBean,
		status:        ConnectionNotEstablished,
		retryRequired: true,
	}

	if connBean.Persistent {
		go handler.maintainPersistentConnection()
	}

	return handler
}

// Connect establishes a TCP connection to the server.
func (c *ClientConnectionHandler) Connect() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == ConnectionEstablished {
		return true, nil
	}

	address := net.JoinHostPort(c.connBean.IPAddress, fmt.Sprintf("%d", c.connBean.Port))
	log.Printf("Connecting to %s", address)

	dialer := &net.Dialer{Timeout: time.Duration(c.connBean.TimeoutPeriod) * time.Second}
	var conn net.Conn
	var err error

	if c.connBean.TLSConfig.Enabled {
		tlsConfig := &tls.Config{
			ServerName:         c.connBean.TLSConfig.ServerName,
			InsecureSkipVerify: c.connBean.TLSConfig.SkipVerify,
		}

		if c.connBean.TLSConfig.CAFile != "" {
			caCert, err := os.ReadFile(c.connBean.TLSConfig.CAFile)
			if err != nil {
				return false, fmt.Errorf("failed to read CA file: %v", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}

		if c.connBean.TLSConfig.CertFile != "" && c.connBean.TLSConfig.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.connBean.TLSConfig.CertFile, c.connBean.TLSConfig.KeyFile)
			if err != nil {
				return false, fmt.Errorf("failed to load TLS certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		conn, err = tls.DialWithDialer(dialer, "tcp", address, tlsConfig)
	} else {
		conn, err = dialer.Dial("tcp", address)
	}

	if err != nil {
		c.status = ConnectionNotEstablished
		return false, fmt.Errorf("connection failed: %v", err)
	}

	c.socket = conn
	c.status = ConnectionEstablished
	c.stopReconnect = false

	if err := c.initializeConnection(); err != nil {
		_ = conn.Close()
		c.status = ConnectionNotEstablished
		return false, fmt.Errorf("failed to initialize connection: %v", err)
	}

	if c.connBean.ConnectionEvent != nil {
		c.connBean.ConnectionEvent.AfterEveryConnection(c)
	}

	c.connBean.ConnectionName = c.getAddress()
	return true, nil
}

// initializeConnection sets up the connection streams and background tasks.
func (c *ClientConnectionHandler) initializeConnection() error {
	if c.socket == nil {
		return errors.New("socket is not initialized")
	}

	if tcpConn, ok := c.socket.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(c.connBean.KeepAliveFlag); err != nil {
			return fmt.Errorf("failed to set keep-alive: %v", err)
		}
	}

	c.reader = bufio.NewReader(c.socket)
	c.writer = bufio.NewWriter(c.socket)
	c.lastActivity = time.Now().UnixMilli()

	if c.connBean.ReadMode == ReadAsync {
		if err := c.startAsyncReader(); err != nil {
			return err
		}
	}

	return nil
}

// WriteRead performs a synchronous write and read operation.
func (c *ClientConnectionHandler) WriteRead(input []byte) ([]byte, error) {
	if !c.connBean.Persistent {
		if err := c.ensureConnected(); err != nil {
			return nil, err
		}
		defer c.CloseConnection(ReasonNonPersistent)
	} else if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	if err := c.Write(input); err != nil {
		return nil, err
	}

	response, err := c.read()
	if err != nil {
		return nil, err
	}

	return c.validateMessage(response)
}

// Write sends data to the server.
func (c *ClientConnectionHandler) Write(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status != ConnectionEstablished {
		return errors.New("connection not established")
	}

	dataToWrite := c.formatMessage(data)
	_, err := c.writer.Write(dataToWrite)
	if err != nil {
		if c.connBean.ConnectionEvent != nil && c.connBean.ConnectionEvent.OnWriteFailure(c) {
			c.CloseConnection(ReasonError)
		}
		return fmt.Errorf("write failed: %v", err)
	}

	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("flush failed: %v", err)
	}

	c.lastActivity = time.Now().UnixMilli()
	log.Printf("Sent %d bytes", len(dataToWrite))
	return nil
}

// read retrieves data from the connection.
func (c *ClientConnectionHandler) read() ([]byte, error) {
	if c.status != ConnectionEstablished {
		return nil, errors.New("connection not established")
	}

	if c.connBean.LengthType == "NO_LENGTH" {
		buffer := make([]byte, 15360)
		n, err := c.reader.Read(buffer)
		if err != nil {
			return nil, fmt.Errorf("read error: %v", err)
		}
		return buffer[:n], nil
	}

	header := make([]byte, c.connBean.LengthOfLength)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return nil, fmt.Errorf("failed to read length header: %v", err)
	}

	length := c.parseLength(header)
	if length <= 0 {
		return nil, fmt.Errorf("invalid message length: %d", length)
	}

	if len(c.connBean.LengthType) >= 3 && c.connBean.LengthType[2] == 'L' {
		length -= c.connBean.LengthOfLength
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return nil, fmt.Errorf("failed to read message body: %v", err)
	}

	log.Printf("Received %d bytes", len(body))
	return body, nil
}

// validateMessage checks the received message.
func (c *ClientConnectionHandler) validateMessage(message []byte) ([]byte, error) {
	if c.connBean.LengthType == "NO_LENGTH" {
		return message, nil
	}
	return message, nil // Header already stripped in read()
}

// parseLength extracts the message length from the header.
func (c *ClientConnectionHandler) parseLength(header []byte) int {
	switch c.connBean.LengthFormat {
	case FormatASCII:
		var length int
		fmt.Sscanf(string(header), "%d", &length)
		return length
	case FormatBinary:
		length := 0
		for _, b := range header {
			length = (length << 8) | int(b)
		}
		return length
	case FormatBCD:
		length := 0
		for _, b := range header {
			high := (b >> 4) & 0x0F
			low := b & 0x0F
			length = length*10 + int(high)
			length = length*10 + int(low)
		}
		return length
	case FormatHEX:
		var length int
		fmt.Sscanf(string(header), "%x", &length)
		return length
	case FormatCustom:
		if c.lengthHandler != nil {
			return c.lengthHandler.ExtractLength(header, c.connBean.LengthType)
		}
		return 0
	default:
		return 0
	}
}

// formatMessage adds a length header to the message.
func (c *ClientConnectionHandler) formatMessage(message []byte) []byte {
	if c.connBean.LengthType == "NO_LENGTH" {
		return message
	}

	length := len(message)
	if len(c.connBean.LengthType) >= 3 && c.connBean.LengthType[2] == 'L' {
		length += c.connBean.LengthOfLength
	}

	var header []byte
	switch c.connBean.LengthFormat {
	case FormatASCII:
		header = []byte(fmt.Sprintf("%0*d", c.connBean.LengthOfLength, length))
	case FormatBinary:
		header = make([]byte, c.connBean.LengthOfLength)
		for i := 0; i < c.connBean.LengthOfLength; i++ {
			shift := 8 * (c.connBean.LengthOfLength - 1 - i)
			header[i] = byte((length >> shift) & 0xFF)
		}
	case FormatBCD:
		header = make([]byte, c.connBean.LengthOfLength)
		digits := fmt.Sprintf("%0*d", c.connBean.LengthOfLength*2, length)
		for i := 0; i < c.connBean.LengthOfLength; i++ {
			if i*2+1 < len(digits) {
				high := digits[i*2] - '0'
				low := digits[i*2+1] - '0'
				header[i] = byte((high << 4) | low)
			}
		}
	case FormatHEX:
		header = []byte(fmt.Sprintf("%0*X", c.connBean.LengthOfLength, length))
	case FormatCustom:
		if c.lengthHandler != nil {
			return c.lengthHandler.ConstructWithLength(message, c.connBean.LengthOfLength, c.connBean.LengthType)
		}
		return message
	default:
		return message
	}

	return append(header, message...)
}

// CloseConnection terminates the connection.
func (c *ClientConnectionHandler) CloseConnection(reason CloseConnectionReason) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status != ConnectionEstablished {
		return
	}

	log.Printf("Closing connection due to %s", reason)
	c.stopReconnect = true

	if c.asyncReader != nil {
		c.asyncReader.Stop()
		c.readerWg.Wait()
	}

	if c.connBean.KeepAliveThread != nil {
		c.connBean.KeepAliveThread.Stop()
		c.keepAliveWg.Wait()
	}

	if c.socket != nil {
		if err := c.socket.Close(); err != nil {
			log.Printf("Error closing socket: %v", err)
		}
		c.socket = nil
	}

	c.status = ConnectionDisconnected
	if c.connBean.ConnectionEvent != nil {
		c.connBean.ConnectionEvent.ConnectionClosed(c)
	}

	log.Printf("Connection closed")
}

// startAsyncReader initializes the asynchronous reader.
func (c *ClientConnectionHandler) startAsyncReader() error {
	if c.connBean.Queue == nil {
		c.connBean.Queue = make(chan []byte, 100)
	}

	c.asyncReader = NewAsyncRead(c.connBean, c)
	c.readerWg.Add(1)
	go func() {
		defer c.readerWg.Done()
		c.asyncReader.Run()
	}()

	return nil
}

// maintainPersistentConnection handles reconnection for persistent connections.
func (c *ClientConnectionHandler) maintainPersistentConnection() {
	for {
		c.mu.Lock()
		if c.stopReconnect || !c.connBean.Persistent {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		if !c.IsConnected() {
			if _, err := c.Connect(); err != nil {
				log.Printf("Reconnection failed: %v", err)
				time.Sleep(time.Duration(c.connBean.SocketReconnectInterval) * time.Second)
				continue
			}
		}
		time.Sleep(1 * time.Second) // Checking every second
	}
}

// getAddress returns the connection address as a string.
func (c *ClientConnectionHandler) getAddress() string {
	return fmt.Sprintf("%s:%d", c.connBean.IPAddress, c.connBean.Port)
}

// SetLengthHandler sets a custom length handler. ununsed as of now..
func (c *ClientConnectionHandler) SetLengthHandler(handler LengthHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lengthHandler = handler
}

// IsConnected checks if the connection is active or not.
func (c *ClientConnectionHandler) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status == ConnectionEstablished && c.socket != nil
}

// ensureConnected ensures the connection is active, reconnecting if necessary.
func (c *ClientConnectionHandler) ensureConnected() error {
	if c.IsConnected() {
		return nil
	}
	connected, err := c.Connect()
	if !connected || err != nil {
		return fmt.Errorf("failed to establish connection: %v", err)
	}
	return nil
}

// NewAsyncRead creates a new async reader.
func NewAsyncRead(connBean *ConnectionBean, handler *ClientConnectionHandler) *AsyncRead {
	return &AsyncRead{
		connBean: connBean,
		handler:  handler,
		stopChan: make(chan struct{}),
	}
}

// Run starts the async reading loop.
func (a *AsyncRead) Run() {
	for {
		select {
		case <-a.stopChan:
			return
		default:
			data, err := a.handler.read()
			if err != nil {
				log.Printf("Async read error: %v", err)
				if a.handler.connBean.ConnectionEvent != nil && a.handler.connBean.ConnectionEvent.OnWriteFailure(a.handler) {
					a.handler.CloseConnection(ReasonError)
				}
				return
			}

			processed, err := a.handler.validateMessage(data)
			if err != nil {
				log.Printf("Message validation error: %v", err)
				continue
			}

			select {
			case a.connBean.Queue <- processed:
				// Message queued successfully
			default:
				log.Printf("Queue full, dropping message")
				if a.connBean.ConnectionEvent != nil {
					a.connBean.ConnectionEvent.OnWriteFailure(a.handler)
				}
			}
		}
	}
}

// Stop terminates the async reader.
func (a *AsyncRead) Stop() {
	close(a.stopChan)
}
