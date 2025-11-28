package main

import (
	"bufio"
	"io"
	"os"
	"sync"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)

	if err != nil {
		return nil, err
	}

	return &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}, nil
}

func (aof *Aof) Close() error {
	aof.mu.Lock()

	defer aof.mu.Unlock()

	return aof.file.Close()
}

func (aof *Aof) Write(v Value) error {
	aof.mu.Lock()

	defer aof.mu.Unlock()

	_, err := aof.file.Write(v.Marshal())

	if err != nil {
		return err
	}

	return aof.file.Sync()
}

func (aof *Aof) Read(fn func(value Value)) error {
	aof.mu.Lock()

	defer aof.mu.Unlock()

	aof.file.Seek(0, 0)

	resp := NewResp(aof.file)

	for {
		value, err := resp.Read()

		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		fn(value)
	}

	return nil
}
