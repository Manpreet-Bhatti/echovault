package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
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

			fmt.Println("Error reading from client:", err)
			break
		}

		if value.Typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(value.Array) == 0 {
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]
		writer := NewWriter(conn)
		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{Typ: "error", Str: "ERR unknown command"})
			continue
		}

		result := handler(args)

		writer.Write(result)
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
