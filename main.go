package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Hold incoming data
	buff := make([]byte, 1024)

	for {
		n, err := conn.Read(buff)

		if err != nil {
			if err != io.EOF {
				fmt.Println("Read error:", err)
			}
			break
		}

		command := string(buff[:n])

		fmt.Printf("Received: %q\n", command)

		conn.Write([]byte("Echo: " + command))
	}
}

func main() {
	listener, err := net.Listen("tcp", ":6379")

	if err != nil {
		log.Fatalf("❌ Failed to bind to port 6379: %v", err)
	}

	defer listener.Close()

	fmt.Println("🚀 EchoVault is listening on port 6379...")

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}
