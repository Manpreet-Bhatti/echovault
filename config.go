package main

import (
	"flag"
	"fmt"
)

type Config struct {
	Port      int
	ReplicaOf string
}

var CurrentConfig Config

func InitConfig() {
	flag.IntVar(&CurrentConfig.Port, "port", 6379, "Port to listen on")
	flag.StringVar(&CurrentConfig.ReplicaOf, "replicaof", "", "Start as a replica of host port")
	flag.Parse()

	fmt.Printf("⚙️  Config: Port %d, ReplicaOf %q\n", CurrentConfig.Port, CurrentConfig.ReplicaOf)
}
