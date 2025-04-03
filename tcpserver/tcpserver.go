package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

/*
-------------------------------------------
simple server that accepts TCP connections and handles messages with a binary length header.
-------------------------------------------
*/

// Server represents a simple TCP server.
type Server struct {
	listener    net.Listener
	wg          sync.WaitGroup
	stopChan    chan struct{}
	connections map[string]*ClientConnection // to Track active connections
	connMu      sync.Mutex                   // to Protect connections map
}

// ClientConnection represents a singe connected client.
type ClientConnection struct {
	conn   net.Conn
	writer *bufio.Writer
	stop   chan struct{}
}

// NewServer creates and returns new TCP server instance.
func NewServer(address string) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to start server: %v", err)
	}

	return &Server{
		listener:    listener,
		stopChan:    make(chan struct{}),
		connections: make(map[string]*ClientConnection),
	}, nil
}

// Run starts the server and listens for incoming connections coming at this address.
func (s *Server) Run() {
	log.Printf("Server listening on %s", s.listener.Addr().String())

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.stopChan:
				return
			default:
				conn, err := s.listener.Accept()
				if err != nil {
					select {
					case <-s.stopChan:
						return
					default:
						log.Printf("Accept error: %v", err)
						continue
					}
				}
				s.wg.Add(1)
				go s.handleConnection(conn)
			}
		}
	}()

	// Start sending async messages to all connections.. just simulated for testing purposes. can cause race conditions if initiated with ReadMode as ReadSync.
	s.wg.Add(1)
	go s.sendAsyncMessages()
}

// handleConnection processes a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("New connection from %s", remoteAddr)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	stopChan := make(chan struct{})

	// Register connection
	clientConn := &ClientConnection{conn: conn, writer: writer, stop: stopChan}
	s.connMu.Lock()
	s.connections[remoteAddr] = clientConn
	s.connMu.Unlock()

	defer func() {
		s.connMu.Lock()
		delete(s.connections, remoteAddr)
		s.connMu.Unlock()
		close(stopChan)
	}()

	for {
		// Read length header (2 bytes, binary)
		header := make([]byte, 2)
		_, err := io.ReadFull(reader, header)
		if err != nil {
			if err == io.EOF {
				log.Printf("Connection closed by %s", remoteAddr)
			} else {
				log.Printf("Read header error from %s: %v", remoteAddr, err)
			}
			return
		}

		// Parse length from binary header
		length := int(header[0])<<8 | int(header[1])
		if length <= 0 || length > 1024 {
			log.Printf("Invalid length from %s: %d", remoteAddr, length)
			return
		}

		// Read message body
		body := make([]byte, length)
		_, err = io.ReadFull(reader, body)
		if err != nil {
			log.Printf("Read body error from %s: %v", remoteAddr, err)
			return
		}

		log.Printf("Received from %s: %s", remoteAddr, string(body))

		// Prepare response (echo back with binary length header)
		response := fmt.Sprintf("Echo: %s", string(body))
		respLength := len(response)
		if respLength > 65535 {
			log.Printf("Response too long for %s: %d bytes", remoteAddr, respLength)
			return
		}

		respWithHeader := make([]byte, 2+respLength)
		respWithHeader[0] = byte(respLength >> 8)   // High byte
		respWithHeader[1] = byte(respLength & 0xFF) // Low byte
		copy(respWithHeader[2:], response)

		// Send response
		_, err = writer.Write(respWithHeader)
		if err != nil {
			log.Printf("Write error to %s: %v", remoteAddr, err)
			return
		}
		err = writer.Flush()
		if err != nil {
			log.Printf("Flush error to %s: %v", remoteAddr, err)
			return
		}

		log.Printf("Sent to %s: %d bytes (length=%d, body=%s)", remoteAddr, len(respWithHeader), respLength, response)
	}
}

// sendAsyncMessages sends periodic messages to all connected clients.
func (s *Server) sendAsyncMessages() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	msgCount := 0
	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			msg := fmt.Sprintf("Async Server Msg #%d", msgCount)
			msgLength := len(msg)
			data := make([]byte, 2+msgLength)
			data[0] = byte(msgLength >> 8)   // High byte
			data[1] = byte(msgLength & 0xFF) // Low byte
			copy(data[2:], msg)

			s.connMu.Lock()
			for addr, client := range s.connections {
				_, err := client.writer.Write(data)
				if err != nil {
					log.Printf("Async write error to %s: %v", addr, err)
					continue
				}
				err = client.writer.Flush()
				if err != nil {
					log.Printf("Async flush error to %s: %v", addr, err)
					continue
				}
				log.Printf("Sent async to %s: %d bytes (length=%d, body=%s)", addr, len(data), msgLength, msg)
			}
			s.connMu.Unlock()
			msgCount++
		}
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	close(s.stopChan)
	s.listener.Close()
	s.wg.Wait()
	log.Println("Server stopped")
}

func main() {
	// Setup logging with timestamps
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	server, err := NewServer("127.0.0.1:8080")
	if err != nil {
		log.Fatalf("Server setup failed: %v", err)
	}

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go server.Run()

	<-sigChan
	log.Println("Shutting down server...")
	server.Stop()
}
