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

		if value.Typ != "array" || len(value.Array) == 0 {
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]
		writer := NewWriter(conn)
		handler, ok := Handlers[command]

		if !ok {
			writer.Write(Value{Typ: "error", Str: "ERR unknown command"})
			continue
		}

		if command == "SET" || command == "DEL" {
			aof.Write(value)
		}

		result := handler(args)
		writer.Write(result)
	}
}

func connectToMaster(address string) {
	parts := strings.Split(address, " ")
	masterConn, err := net.Dial("tcp", parts[0]+":"+parts[1])
	if err != nil {
		fmt.Println("Failed to connect to master:", err)
		return
	}

	fmt.Println("✅ Connected to Master!")

	resp := NewResp(masterConn)

	for {
		value, err := resp.Read()
		if err != nil {
			fmt.Println("Disconnected from master")
			return
		}

		if value.Typ == "array" && len(value.Array) > 0 {
			command := strings.ToUpper(value.Array[0].Bulk)
			args := value.Array[1:]

			if handler, ok := Handlers[command]; ok {
				handler(args)
				fmt.Printf("Replicated: %s\n", command)
			}
		}
	}
}

func main() {
	InitConfig()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", CurrentConfig.Port))
	if err != nil {
		log.Fatal(err)
	}

	aof, err := NewAof(fmt.Sprintf("database_%d.aof", CurrentConfig.Port))
	if err != nil {
		log.Fatal(err)
	}
	defer aof.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]
		handler, ok := Handlers[command]

		if !ok {
			return
		}

		handler(args)
	})

	if CurrentConfig.ReplicaOf != "" {
		go connectToMaster(CurrentConfig.ReplicaOf)
	}

	fmt.Printf("🚀 EchoVault listening on port %d...\n", CurrentConfig.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn, aof)
	}
}
