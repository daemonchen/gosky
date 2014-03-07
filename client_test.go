package sky

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Ensure that we can retrieve a single table.
func TestClientTable(t *testing.T) {
	run(t, func(c *Client, _ *Table) {
		table, err := c.Table("sky-go-integration")
		assert.NoError(t, err)
		if assert.NotNil(t, table) {
			assert.Equal(t, table.Name, "sky-go-integration")
		}
	})
}

// Ensure that we retrieve a list of all tables.
func TestClientTables(t *testing.T) {
	run(t, func(c *Client, _ *Table) {
		tables, err := c.Tables()
		assert.NoError(t, err)
		assert.NotEqual(t, len(tables), 0)
	})
}

func TestClientEventStream(t *testing.T) {
	run(t, func(c *Client, table *Table) {
		stream, err := c.Stream()
		if err != nil {
			t.Fatalf("Failed to create event stream: (%v)", err)
		}
		now := time.Now()
		for i := 0; i < 10; i++ {
			timestamp := now.Add(time.Duration(i) * time.Hour)
			event := &Event{timestamp, make(map[string]interface{})}
			err = stream.InsertEvent(table, "xyz", event)
			if err != nil {
				t.Fatalf("Failed to create event #%d: %v (%v)", i, event, err)
			}
		}
		err = stream.Close()
		if err != nil {
			t.Fatalf("Closing stream failed: (%v)", err)
		}
		events, err := table.Events("xyz")
		if err != nil || len(events) != 10 {
			t.Fatalf("Failed to get 10 events back: %d events, (%v)", len(events), err)
		}
	})
}

func TestClientTableEventStream(t *testing.T) {
	run(t, func(c *Client, table *Table) {
		stream, err := table.Stream()
		if err != nil {
			t.Fatalf("Failed to create event stream: (%v)", err)
		}
		now := time.Now()
		for i := 0; i < 10; i++ {
			timestamp := now.Add(time.Duration(i) * time.Hour)
			event := &Event{timestamp, make(map[string]interface{})}
			err = stream.InsertEvent("xyz", event)
			if err != nil {
				t.Fatalf("Failed to create event #%d: %v (%v)", i, event, err)
			}
		}
		err = stream.Close()
		if err != nil {
			t.Fatalf("Closing stream failed: (%v)", err)
		}
		events, err := table.Events("xyz")
		if err != nil || len(events) != 10 {
			t.Fatalf("Failed to get 10 events back: %d events, (%v)", len(events), err)
		}
	})
}
