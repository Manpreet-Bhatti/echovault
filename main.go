package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

func handleConnection(conn net.Conn, aof *Aof) {
	defer conn.Close()

	resp := NewResp(conn)

	for {
		value, err := resp.Read()

		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Println(err)
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

		if command == "SET" {
			aof.Write(value)
		}

		result := handler(args)

		writer.Write(result)
	}
}

func main() {
	fmt.Println("Listening on port :6379")

	listener, err := net.Listen("tcp", ":6379")

	if err != nil {
		log.Fatal(err)
	}

	aof, err := NewAof("database.aof")

	if err != nil {
		log.Fatal(err)
	}

	defer aof.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]
		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command in AOF: ", command)
			return
		}

		handler(args)
	})

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleConnection(conn, aof)
	}
}
