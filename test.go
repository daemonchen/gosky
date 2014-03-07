package sky

import (
	"testing"
)

const testTableName = "sky-go-integration"

// Executes a function within the context of a client and existing table.
func run(t *testing.T, f func(*Client, *Table)) {
	c := &Client{Host: "localhost:8589"}
	if !c.Ping() {
		t.Fatalf("Server is not running")
	}
	c.DeleteTable(testTableName)

	// Create the table.
	table := &Table{Name: testTableName}
	err := c.CreateTable(table)
	if err != nil {
		t.Fatalf("Unable to setup test table: %v", err)
	}
	defer c.DeleteTable(testTableName)

	f(c, table)
}
