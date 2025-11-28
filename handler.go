package main

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"PING":   ping,
	"SET":    setLocked,
	"GET":    get,
	"DEL":    delLocked,
	"BGSAVE": bgsave,
}

var CoreHandlers = map[string]func([]Value) Value{
	"PING": ping,
	"SET":  setCore,
	"GET":  get,
	"DEL":  delCore,
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}
var HSETs = map[string]time.Time{}
var HSETsMu = sync.RWMutex{}
var Peers = map[net.Conn]bool{}
var PeersMu = sync.Mutex{}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{Typ: "string", Str: "PONG"}
	}

	return Value{Typ: "string", Str: args[0].Bulk}
}

func setCore(args []Value) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk
	var expiresAt time.Time

	if len(args) == 4 && strings.ToUpper(args[2].Bulk) == "EX" {
		seconds, _ := strconv.ParseInt(args[3].Bulk, 10, 64)
		expiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
	}

	SETs[key] = value

	HSETsMu.Lock()
	if !expiresAt.IsZero() {
		HSETs[key] = expiresAt
	} else {
		delete(HSETs, key)
	}
	HSETsMu.Unlock()

	cmd := Value{
		Typ:   "array",
		Array: append([]Value{{Typ: "bulk", Bulk: "SET"}}, args...),
	}

	PeersMu.Lock()
	for peer := range Peers {
		peer.Write(cmd.Marshal())
	}
	PeersMu.Unlock()

	return Value{Typ: "string", Str: "OK"}
}

func setLocked(args []Value) Value {
	SETsMu.Lock()
	defer SETsMu.Unlock()
	return setCore(args)
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Bulk

	HSETsMu.RLock()
	expiry, hasExpiry := HSETs[key]
	HSETsMu.RUnlock()

	if hasExpiry && time.Now().After(expiry) {
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()

		HSETsMu.Lock()
		delete(HSETs, key)
		HSETsMu.Unlock()

		return Value{Typ: "null"}
	}

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{Typ: "null"}
	}

	return Value{Typ: "bulk", Bulk: value}
}

func delCore(args []Value) Value {
	if len(args) != 1 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'del' command"}
	}

	key := args[0].Bulk

	_, ok := SETs[key]
	delete(SETs, key)

	HSETsMu.Lock()
	delete(HSETs, key)
	HSETsMu.Unlock()

	if ok {
		return Value{Typ: "integer", Num: 1}
	}
	return Value{Typ: "integer", Num: 0}
}

func delLocked(args []Value) Value {
	SETsMu.Lock()
	defer SETsMu.Unlock()
	return delCore(args)
}
