package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	resp := NewResp(conn)

	for {
		value, err := resp.Read()

		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
				break
			}
		}

		if value.Typ == "array" {
			fmt.Println("Received Command:")

			for _, v := range value.Array {
				fmt.Printf("  %s\n", v.Bulk)
			}
		}
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
