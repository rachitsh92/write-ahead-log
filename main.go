package main

import (
	"fmt"
	"github.com/rachitsh92/write-ahead-log/wal")

func main() {
	write_ahead_log, err := wal.NewWAL("wal.log")
	if err != nil {
		fmt.Println("Error creating WAL:", err)
		return
	}
	defer write_ahead_log.File.Close()

	// Example transactions
	err = write_ahead_log.WriteRecord("BEGIN TRANSACTION", "T1")
	if err != nil {
		fmt.Println("Error writing record:", err)
		return
	}

	for i := 0; i < 100; i++ {
		err = write_ahead_log.WriteRecord("UPDATE account SET balance = 1200 WHERE account_id = 1234", fmt.Sprintf("%d", 1200+i))
		if err != nil {
			fmt.Println("Error writing record:", err)
			return
		}
	}

	err = write_ahead_log.CommitTransaction()
	if err != nil {
		fmt.Println("Error committing transaction:", err)
		return
	}

	fmt.Println("First transaction committed successfully")

	// Read the state of the in-memory database
	dbState := write_ahead_log.ReadDB()
	fmt.Printf("Current DB State: %v\n", dbState)

	// Simulate a second transaction
	err = write_ahead_log.WriteRecord("BEGIN TRANSACTION", "T2")
	if err != nil {
		fmt.Println("Error writing record:", err)
		return
	}

	err = write_ahead_log.WriteRecord("UPDATE account SET balance = 1500 WHERE account_id = 5678", "1500")
	if err != nil {
		fmt.Println("Error writing record:", err)
		return
	}

	err = write_ahead_log.CommitTransaction()
	if err != nil {
		fmt.Println("Error committing transaction:", err)
		return
	}

	fmt.Println("Second transaction committed successfully")

	// Read the state of the in-memory database again
	dbState = write_ahead_log.ReadDB()
	fmt.Printf("Current DB State: %v\n", dbState)
}
