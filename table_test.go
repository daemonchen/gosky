package sky

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that we can create and delete properties.
func TestTableCreateProperty(t *testing.T) {
	run(t, func(c *Client, table *Table) {
		p := &Property{Name: "purchase_price", Transient: true, DataType: String}

		// Create the property.
		err := table.CreateProperty(p)
		assert.NoError(t, err)

		// Delete the property.
		err = table.DeleteProperty("purchase_price")
		assert.NoError(t, err)
	})
}

// Ensure that we can get a single property.
func TestTableProperties(t *testing.T) {
	run(t, func(c *Client, table *Table) {
		table.CreateProperty(&Property{Name: "gender", Transient: false, DataType: Factor})
		table.CreateProperty(&Property{Name: "name", Transient: false, DataType: String})
		table.CreateProperty(&Property{Name: "myNum", Transient: true, DataType: Integer})

		// Get a single property.
		p, err := table.Property("gender")
		assert.NoError(t, err)
		if assert.NotNil(t, p) {
			assert.Equal(t, p.Name, "gender")
			assert.Equal(t, p.Transient, false)
			assert.Equal(t, p.DataType, Factor)
		}

		// Rename a property.
		err = table.RenameProperty("gender", "gender2")
		assert.NoError(t, err)

		// Get all properties.
		properties, err := table.Properties()
		assert.NoError(t, err)
		if assert.Equal(t, len(properties), 3) {
			assert.Equal(t, properties[0].Name, "myNum")
			assert.Equal(t, properties[0].Transient, true)
			assert.Equal(t, properties[0].DataType, Integer)

			assert.Equal(t, properties[1].Name, "gender2")
			assert.Equal(t, properties[1].Transient, false)
			assert.Equal(t, properties[1].DataType, Factor)

			assert.Equal(t, properties[2].Name, "name")
			assert.Equal(t, properties[2].Transient, false)
			assert.Equal(t, properties[2].DataType, String)
		}
	})
}

// Ensure that we can insert an event and merge an update into it.
func TestTableInsertEvent(t *testing.T) {
	run(t, func(c *Client, table *Table) {
		timestamp, _ := ParseTimestamp("1970-01-01T00:00:01.5Z")
		table.CreateProperty(&Property{Name: "p0", Transient: false, DataType: String})
		table.CreateProperty(&Property{Name: "t0", Transient: true, DataType: Integer})
		e0 := &Event{timestamp, map[string]interface{}{"p0": "foo", "t0": 10}}
		e1 := &Event{timestamp, map[string]interface{}{"t0": 20}}

		// Add the event.
		err := table.InsertEvent("o0", e0)
		assert.NoError(t, err)

		// Add another event.
		err = table.InsertEvent("o0", e1)
		assert.NoError(t, err)

		// Check the result.
		e, err := table.Event("o0", timestamp)
		assert.NoError(t, err)
		if assert.NotNil(t, e) {
			assert.Equal(t, e.Data["t0"], float64(20))
			assert.Equal(t, e.Data["p0"], "foo")
		}
	})
}

// Ensure that we can delete an event.
func TestTableDeleteEvent(t *testing.T) {
	run(t, func(c *Client, table *Table) {
		timestamp, _ := ParseTimestamp("1970-01-01T00:00:01.5Z")
		table.CreateProperty(&Property{Name: "p0", Transient: false, DataType: String})
		table.CreateProperty(&Property{Name: "t0", Transient: true, DataType: Integer})
		e0 := &Event{timestamp, map[string]interface{}{"p0": "foo", "t0": 10}}

		// Add the event.
		err := table.InsertEvent("o0", e0)
		assert.NoError(t, err)

		// Delete the event.
		err = table.DeleteEvent("o0", timestamp)
		assert.NoError(t, err)

		// Get the event to verify.
		e, err := table.Event("o0", timestamp)
		assert.NoError(t, err)
		assert.Nil(t, e)
	})
}

// Ensure that we can replace an event into another one.
func TestTableQuery(t *testing.T) {
	run(t, func(c *Client, table *Table) {
		table.CreateProperty(&Property{Name: "action", Transient: false, DataType: Factor})
		t0, _ := ParseTimestamp("1970-01-01T00:00:00Z")
		t1, _ := ParseTimestamp("1970-01-01T00:00:01Z")
		t2, _ := ParseTimestamp("1970-01-01T00:00:01.5Z")
		table.InsertEvent("o0", &Event{t0, map[string]interface{}{"action": "A0"}})
		table.InsertEvent("o0", &Event{t1, map[string]interface{}{"action": "A1"}})
		table.InsertEvent("o0", &Event{t2, map[string]interface{}{"action": "A2"}})

		// Run a simple count query.
		results, err := table.Query("SELECT count()")
		assert.NoError(t, err)
		if assert.NotNil(t, results) {
			assert.Equal(t, results["count"], float64(3))
		}
	})
}
