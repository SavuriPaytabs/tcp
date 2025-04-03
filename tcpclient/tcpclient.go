package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"tcp/tcpcommon"
	"time"
)

// This is a simple TCP client that demonstrates both persistent and non-persistent connections.

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Persistent connection test
	log.Println("Starting persistent connection test...")
	persistentConnBean := &tcpcommon.ConnectionBean{
		IPAddress:               "127.0.0.1",
		Port:                    8080,
		TimeoutPeriod:           5,
		Persistent:              true,
		ReadMode:                tcpcommon.ReadAsync,
		LengthType:              "LL",
		LengthOfLength:          2,
		LengthFormat:            tcpcommon.FormatBinary,
		Queue:                   make(chan []byte, 100),
		SocketReconnectInterval: 2,
	}

	persistentHandler := tcpcommon.NewClientConnection(persistentConnBean)

	// Non-persistent connection test
	log.Println("Starting non-persistent connection test...")
	nonPersistentConnBean := &tcpcommon.ConnectionBean{
		IPAddress:      "127.0.0.1",
		Port:           8080,
		TimeoutPeriod:  5,
		Persistent:     false,
		ReadMode:       tcpcommon.ReadSync,
		LengthType:     "LL",
		LengthOfLength: 2,
		LengthFormat:   tcpcommon.FormatBinary,
	}

	var wg sync.WaitGroup

	// Persistent connection: Sending 5 messages
	/*
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				msg := []byte("Persistent Msg #" + fmt.Sprint(i))
				resp, err := persistentHandler.WriteRead(msg)
				if err != nil {
					log.Printf("Persistent write/read error: %v", err)
				} else {
					log.Printf("Persistent response: %s", resp)
				}
				time.Sleep(3 * time.Second)
			}
		}()
	*/

	// Async reader for persistent connection
	if persistentConnBean.ReadMode == tcpcommon.ReadAsync {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range persistentConnBean.Queue {
				log.Printf("Received async message from server: %s", msg)
			}
		}()
	}

	// Non persistent connection: Sendinf 5 messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			handler := tcpcommon.NewClientConnection(nonPersistentConnBean)
			msg := []byte("Non-Persistent Msg #" + fmt.Sprint(i))
			resp, err := handler.WriteRead(msg)
			if err != nil {
				log.Printf("Non-persistent write/read error: %v", err)
			} else {
				log.Printf("Non-persistent response: %s", resp)
			}
			time.Sleep(4 * time.Second)
		}
	}()

	sigChan := make(chan os.Signal, 1) //for shutdown
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Shutting down client...")
	persistentHandler.CloseConnection(tcpcommon.ReasonManualClose)
	close(persistentConnBean.Queue)
	wg.Wait()
	log.Println("Client stopped")
}
