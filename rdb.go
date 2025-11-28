package main

import (
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"time"
)

const (
	RDBMagic   = "ECHOVAULT"
	RDBVersion = 1
)

type RDB struct {
	path     string
	interval time.Duration
	stopChan chan struct{}
}

func NewRDB(path string, interval time.Duration) *RDB {
	return &RDB{
		path:     path,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

type snapshotData struct {
	keys   map[string]string
	expiry map[string]time.Time
}

func takeSnapshot() snapshotData {
	SETsMu.RLock()
	HSETsMu.RLock()

	keys := make(map[string]string, len(SETs))
	maps.Copy(keys, SETs)

	expiry := make(map[string]time.Time, len(HSETs))
	maps.Copy(expiry, HSETs)

	HSETsMu.RUnlock()
	SETsMu.RUnlock()

	return snapshotData{keys: keys, expiry: expiry}
}

func (r *RDB) Save() error {
	snapshot := takeSnapshot()

	tempPath := r.path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := file.WriteString(RDBMagic); err != nil {
		file.Close()
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, uint8(RDBVersion)); err != nil {
		file.Close()
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, uint32(len(snapshot.keys))); err != nil {
		file.Close()
		return err
	}

	for key, value := range snapshot.keys {
		if err := binary.Write(file, binary.LittleEndian, uint32(len(key))); err != nil {
			file.Close()
			return err
		}
		if _, err := file.WriteString(key); err != nil {
			file.Close()
			return err
		}

		if err := binary.Write(file, binary.LittleEndian, uint32(len(value))); err != nil {
			file.Close()
			return err
		}
		if _, err := file.WriteString(value); err != nil {
			file.Close()
			return err
		}

		expiry, hasExpiry := snapshot.expiry[key]
		if hasExpiry {
			if err := binary.Write(file, binary.LittleEndian, uint8(1)); err != nil {
				file.Close()
				return err
			}
			if err := binary.Write(file, binary.LittleEndian, expiry.UnixNano()); err != nil {
				file.Close()
				return err
			}
		} else {
			if err := binary.Write(file, binary.LittleEndian, uint8(0)); err != nil {
				file.Close()
				return err
			}
		}
	}

	file.Close()

	if err := os.Rename(tempPath, r.path); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

func (r *RDB) Load() error {
	file, err := os.Open(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	magic := make([]byte, len(RDBMagic))
	if _, err := file.Read(magic); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}
	if string(magic) != RDBMagic {
		return fmt.Errorf("invalid RDB file: bad magic")
	}

	var version uint8
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if version != RDBVersion {
		return fmt.Errorf("unsupported RDB version: %d", version)
	}

	var numKeys uint32
	if err := binary.Read(file, binary.LittleEndian, &numKeys); err != nil {
		return fmt.Errorf("failed to read key count: %w", err)
	}

	SETsMu.Lock()
	HSETsMu.Lock()

	for i := uint32(0); i < numKeys; i++ {

		var keyLen uint32
		if err := binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			HSETsMu.Unlock()
			SETsMu.Unlock()
			return fmt.Errorf("failed to read key length: %w", err)
		}

		keyBytes := make([]byte, keyLen)
		if _, err := file.Read(keyBytes); err != nil {
			HSETsMu.Unlock()
			SETsMu.Unlock()
			return fmt.Errorf("failed to read key: %w", err)
		}

		key := string(keyBytes)

		var valueLen uint32
		if err := binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
			HSETsMu.Unlock()
			SETsMu.Unlock()
			return fmt.Errorf("failed to read value length: %w", err)
		}

		valueBytes := make([]byte, valueLen)
		if _, err := file.Read(valueBytes); err != nil {
			HSETsMu.Unlock()
			SETsMu.Unlock()
			return fmt.Errorf("failed to read value: %w", err)
		}

		value := string(valueBytes)

		var hasExpiry uint8
		if err := binary.Read(file, binary.LittleEndian, &hasExpiry); err != nil {
			HSETsMu.Unlock()
			SETsMu.Unlock()
			return fmt.Errorf("failed to read expiry flag: %w", err)
		}

		SETs[key] = value

		if hasExpiry == 1 {
			var expiryNano int64
			if err := binary.Read(file, binary.LittleEndian, &expiryNano); err != nil {
				HSETsMu.Unlock()
				SETsMu.Unlock()
				return fmt.Errorf("failed to read expiry: %w", err)
			}

			expiry := time.Unix(0, expiryNano)
			if time.Now().Before(expiry) {
				HSETs[key] = expiry
			}
		}
	}

	HSETsMu.Unlock()
	SETsMu.Unlock()

	return nil
}

// Starts a goroutine that saves snapshots periodically
func (r *RDB) StartBackgroundSave() {
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				start := time.Now()
				if err := r.Save(); err != nil {
					fmt.Printf("❌ RDB save failed: %v\n", err)
				} else {
					fmt.Printf("💾 RDB snapshot saved (%s) in %v\n", r.path, time.Since(start))
				}
			case <-r.stopChan:
				return
			}
		}
	}()
}

func (r *RDB) Stop() { close(r.stopChan) }

func bgsave(args []Value) Value {
	go func() {
		rdb := NewRDB(fmt.Sprintf("database_%d.rdb", CurrentConfig.Port), 0)
		if err := rdb.Save(); err != nil {
			fmt.Printf("❌ BGSAVE failed: %v\n", err)
		} else {
			fmt.Println("💾 BGSAVE completed")
		}
	}()
	return Value{Typ: "string", Str: "Background saving started"}
}
