package main

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]Value) Value{
	"PING": ping,
	"SET":  set,
	"GET":  get,
	"DEL":  del,
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}
var HSETs = map[string]time.Time{}
var HSETsMu = sync.RWMutex{}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{Typ: "string", Str: "PONG"}
	}

	return Value{Typ: "string", Str: args[0].Bulk}
}

func set(args []Value) Value {
	if len(args) < 2 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].Bulk
	value := args[1].Bulk
	var expiresAt time.Time

	if len(args) == 4 {
		if strings.ToUpper(args[2].Bulk) == "EX" {
			seconds, err := strconv.ParseInt(args[3].Bulk, 10, 64)

			if err != nil {
				return Value{Typ: "error", Str: "ERR value is not an integer or out of range"}
			}

			expiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
		}
	}

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	HSETsMu.Lock()
	if !expiresAt.IsZero() {
		HSETs[key] = expiresAt
	} else {
		delete(HSETs, key)
	}
	HSETsMu.Unlock()

	return Value{Typ: "string", Str: "OK"}
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

func del(args []Value) Value {
	if len(args) != 1 {
		return Value{Typ: "error", Str: "ERR wrong number of arguments for 'del' command"}
	}

	key := args[0].Bulk

	SETsMu.Lock()
	_, ok := SETs[key]
	delete(SETs, key)
	SETsMu.Unlock()

	HSETsMu.Lock()
	delete(HSETs, key)
	HSETsMu.Unlock()

	if ok {
		return Value{Typ: "integer", Num: 1}
	}
	return Value{Typ: "integer", Num: 0}
}
