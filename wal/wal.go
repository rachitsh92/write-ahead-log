package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

// LogRecord represents a single log entry
type LogRecord struct {
	LSN       uint64
	Operation string
	Data      string
	CRC32     uint32
}

// WAL represents a write-ahead log
type WAL struct {
	Records     []LogRecord
	File        *os.File
	inMemoryDB  map[string]string // Simple in-memory database
	logMutex    sync.Mutex
	dbMutex     sync.Mutex
	currentLSN  uint64
	version     uint64
	committedLSN uint64
}

// NewWAL creates a new WAL
func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		Records:    []LogRecord{},
		File:       file,
		inMemoryDB: make(map[string]string),
		currentLSN: 0,
		version:    0,
		committedLSN: 0,
	}, nil
}

// WriteRecord writes a log record to the WAL
func (wal *WAL) WriteRecord(operation, data string) error {
	wal.logMutex.Lock()
	defer wal.logMutex.Unlock()

	lsn := wal.currentLSN + 1
	record := LogRecord{
		LSN:       lsn,
		Operation: operation,
		Data:      data,
	}

	// Calculate CRC32
	crc32Checksum := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%d%s%s", record.LSN, record.Operation, record.Data)))
	record.CRC32 = crc32Checksum

	// Write to in-memory log
	wal.Records = append(wal.Records, record)
	wal.currentLSN = lsn

	// Write to disk
	err := wal.writeToDisk(record)
	if err != nil {
		return err
	}

	return nil
}

// writeToDisk writes a log record to disk
func (wal *WAL) writeToDisk(record LogRecord) error {
	buf := make([]byte, 0)
	buf = append(buf, uint64ToBytes(record.LSN)...)
	buf = append(buf, []byte(record.Operation)...)
	buf = append(buf, []byte(record.Data)...)
	buf = append(buf, uint32ToBytes(record.CRC32)...)

	_, err := wal.File.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

// applyChanges applies a log record to the in-memory database
func (wal *WAL) applyChanges(record LogRecord) error {
	wal.dbMutex.Lock()
	defer wal.dbMutex.Unlock()

	switch record.Operation {
	case "BEGIN TRANSACTION":
		// Handle begin transaction if necessary
	case "COMMIT TRANSACTION":
		// Handle commit transaction if necessary
	default:
		// Assume it's an update operation in the format "UPDATE table SET column=value WHERE condition"
		// For simplicity, we handle a single update operation
		fmt.Printf("Applying change to DB: %s\n", record.Data)
		// This example doesn't parse the SQL, just a simplified update
		wal.inMemoryDB["balance"] = record.Data
	}

	wal.version++

	return nil
}

// flushDB flushes the in-memory database to disk
func (wal *WAL) flushDB() error {
	wal.dbMutex.Lock()
	defer wal.dbMutex.Unlock()

	file, err := os.Create("database_state")
	if err != nil {
		return err
	}
	defer file.Close()

	for key, value := range wal.inMemoryDB {
		_, err := file.WriteString(fmt.Sprintf("%s=%s\n", key, value))
		if err != nil {
			return err
		}
	}

	wal.committedLSN = wal.currentLSN

	return nil
}

// ReadDB reads the current state of the in-memory database
func (wal *WAL) ReadDB() map[string]string {
	wal.dbMutex.Lock()
	defer wal.dbMutex.Unlock()

	// Return a copy of the in-memory database to ensure thread-safety
	result := make(map[string]string)
	for key, value := range wal.inMemoryDB {
		result[key] = value
	}
	return result
}

// uint64ToBytes converts a uint64 to a byte slice
func uint64ToBytes(num uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, num)
	return buf
}

// uint32ToBytes converts a uint32 to a byte slice
func uint32ToBytes(num uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, num)
	return buf
}

// commitTransaction commits the current transaction and flushes changes to the database
func (wal *WAL) CommitTransaction() error {
	wal.logMutex.Lock()
	defer wal.logMutex.Unlock()

	// Create a commit log record
	commitRecord := LogRecord{
		LSN:       wal.currentLSN + 1,
		Operation: "COMMIT TRANSACTION",
		Data:      "",
		CRC32:     0, // CRC32 will be calculated below
	}

	// Calculate CRC32
	commitRecord.CRC32 = crc32.ChecksumIEEE([]byte(fmt.Sprintf("%d%s%s", commitRecord.LSN, commitRecord.Operation, commitRecord.Data)))

	// Write to in-memory log
	wal.Records = append(wal.Records, commitRecord)
	wal.currentLSN = commitRecord.LSN

	// Write to disk
	err := wal.writeToDisk(commitRecord)
	if err != nil {
		return err
	}

	// Apply all changes to the in-memory database
	for _, record := range wal.Records {
		if err := wal.applyChanges(record); err != nil {
			return err
		}
	}

	// Flush the in-memory database state to disk
	if err := wal.flushDB(); err != nil {
		return err
	}

	// Clear the log
	wal.Records = []LogRecord{}

	return nil
}