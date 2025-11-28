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

	PeersMu.Lock()
	Peers[conn] = true
	PeersMu.Unlock()

	defer func() {
		PeersMu.Lock()
		delete(Peers, conn)
		PeersMu.Unlock()
	}()

	resp := NewResp(conn)
	writer := NewWriter(conn)
	inMulti := false
	queue := [][]Value{}

	for {
		value, err := resp.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Println("Error:", err)
			break
		}

		if value.Typ != "array" || len(value.Array) == 0 {
			continue
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		if command == "MULTI" {
			if inMulti {
				writer.Write(Value{Typ: "error", Str: "ERR MULTI calls can not be nested"})
			} else {
				inMulti = true
				writer.Write(Value{Typ: "string", Str: "OK"})
			}
			continue
		}

		if command == "DISCARD" {
			if !inMulti {
				writer.Write(Value{Typ: "error", Str: "ERR DISCARD without MULTI"})
			} else {
				inMulti = false
				queue = [][]Value{}
				writer.Write(Value{Typ: "string", Str: "OK"})
			}
			continue
		}

		if command == "EXEC" {
			if !inMulti {
				writer.Write(Value{Typ: "error", Str: "ERR EXEC without MULTI"})
				continue
			}

			results := []Value{}

			SETsMu.Lock()
			for _, qArgs := range queue {
				qCmd := strings.ToUpper(qArgs[0].Bulk)
				qParams := qArgs[1:]

				handler, ok := CoreHandlers[qCmd]
				if ok {
					val := handler(qParams)
					results = append(results, val)

					if qCmd == "SET" || qCmd == "DEL" {
						aof.Write(Value{Typ: "array", Array: qArgs})
					}
				} else {
					results = append(results, Value{Typ: "error", Str: "ERR unknown command"})
				}
			}
			SETsMu.Unlock()

			inMulti = false
			queue = [][]Value{}

			writer.Write(Value{Typ: "array", Array: results})
			continue
		}

		if inMulti {
			_, ok := Handlers[command]
			if !ok {
				writer.Write(Value{Typ: "error", Str: "ERR unknown command"})
				continue
			}

			queue = append(queue, value.Array)
			writer.Write(Value{Typ: "string", Str: "QUEUED"})
			continue
		}

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
