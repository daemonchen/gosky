package sky

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Table struct {
	Client *Client
	Name   string `json:"name"`
}

// Property retrieves a single property on the table by name.
func (t *Table) Property(name string) (*Property, error) {
	if t.Client == nil {
		return nil, ErrClientRequired
	} else if name == "" {
		return nil, ErrPropertyNameRequired
	}
	property := &Property{}
	if err := t.Client.Send("GET", fmt.Sprintf("/tables/%s/properties/%s", t.Name, name), nil, property); err != nil {
		return nil, err
	}
	return property, nil
}

// Properties retrieves a list of all properties on the table.
func (t *Table) Properties() ([]*Property, error) {
	if t.Client == nil {
		return nil, ErrClientRequired
	}
	properties := []*Property{}
	if err := t.Client.Send("GET", fmt.Sprintf("/tables/%s/properties", t.Name), nil, &properties); err != nil {
		return nil, err
	}
	return properties, nil
}

// CreateProperty creates a property on the table.
func (t *Table) CreateProperty(property *Property) error {
	if t.Client == nil {
		return ErrClientRequired
	} else if property == nil {
		return ErrPropertyRequired
	}
	return t.Client.Send("POST", fmt.Sprintf("/tables/%s/properties", t.Name), property, property)
}

// RenameProperty changes the name of a property on the table.
func (t *Table) RenameProperty(oldName string, newName string) error {
	if t.Client == nil {
		return ErrClientRequired
	} else if oldName == "" || newName == "" {
		return ErrPropertyNameRequired
	}
	return t.Client.Send("PATCH", fmt.Sprintf("/tables/%s/properties/%s", t.Name, oldName), &Property{Name: newName}, nil)
}

// DeleteProperty removes a property from the table.
func (t *Table) DeleteProperty(name string) error {
	if t.Client == nil {
		return ErrClientRequired
	} else if name == "" {
		return ErrPropertyNameRequired
	}
	return t.Client.Send("DELETE", fmt.Sprintf("/tables/%s/properties/%s", t.Name, name), nil, nil)
}

// Event retrieves a single event for an object at a given time.
func (t *Table) Event(id string, timestamp time.Time) (*Event, error) {
	if t.Client == nil {
		return nil, ErrClientRequired
	} else if id == "" {
		return nil, ErrIDRequired
	}

	e := map[string]interface{}{}
	if err := t.Client.Send("GET", fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.Name, id, FormatTimestamp(timestamp)), nil, &e); err != nil {
		return nil, err
	} else if len(e) == 0 {
		return nil, nil
	}

	// Deserialize event data.
	event := &Event{}
	if err := event.Deserialize(e); err != nil {
		return nil, err
	}
	return event, nil
}

// Events retrieves a list of all events for an object.
func (t *Table) Events(id string) ([]*Event, error) {
	if t.Client == nil {
		return nil, ErrClientRequired
	} else if id == "" {
		return nil, ErrIDRequired
	}

	output := make([]map[string]interface{}, 0)
	if err := t.Client.Send("GET", fmt.Sprintf("/tables/%s/objects/%s/events", t.Name, id), nil, &output); err != nil {
		return nil, err
	}

	// Deserialize.
	events := []*Event{}
	for _, i := range output {
		event := &Event{}
		event.Deserialize(i)
		events = append(events, event)
	}
	return events, nil
}

// InsertEvent adds an event to an object.
func (t *Table) InsertEvent(id string, e *Event) error {
	if t.Client == nil {
		return ErrClientRequired
	} else if id == "" {
		return ErrIDRequired
	} else if e == nil {
		return ErrEventRequired
	}
	return t.Client.Send("PATCH", fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.Name, id, FormatTimestamp(e.Timestamp)), e.Serialize(), nil)
}

// DeleteEvent deletes an event on an object at the given time.
func (t *Table) DeleteEvent(id string, timestamp time.Time) error {
	if t.Client == nil {
		return ErrClientRequired
	} else if id == "" {
		return ErrIDRequired
	}
	return t.Client.Send("DELETE", fmt.Sprintf("/tables/%s/objects/%s/events/%s", t.Name, id, FormatTimestamp(timestamp)), nil, nil)
}

// DeleteEvents deletes all events for an object.
func (t *Table) DeleteEvents(id string) error {
	if t.Client == nil {
		return ErrClientRequired
	} else if id == "" {
		return ErrIDRequired
	}
	return t.Client.Send("DELETE", fmt.Sprintf("/tables/%s/objects/%s/events", t.Name, id), nil, nil)
}

// Stream returns a new stream for the table.
func (t *Table) Stream() (*TableEventStream, error) {
	return NewTableEventStream(t.Client, t)
}

// Stats retrieves basic statistics on the table.
func (t *Table) Stats() (*Stats, error) {
	if t.Client == nil {
		return nil, errors.New("Table is not attached to a client")
	}
	output := &Stats{}
	if err := t.Client.Send("GET", fmt.Sprintf("/tables/%s/stats", t.Name), nil, &output); err != nil {
		return nil, err
	}
	return output, nil
}

// Query executes a SkyQL query on the table and returns the result.
func (t *Table) Query(q string) (map[string]interface{}, error) {
	if t.Client == nil {
		return nil, ErrClientRequired
	}
	if q == "" {
		return nil, ErrQueryRequired
	}
	output := map[string]interface{}{}
	if err := t.Client.Send("POST", fmt.Sprintf("/tables/%s/query", t.Name), q, &output); err != nil {
		return nil, err
	}
	return output, nil
}

func (t *Table) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(map[string]interface{}{"name": t.Name})
	return b, err
}

func (t *Table) UnmarshalJSON(data []byte) error {
	tmp := map[string]interface{}{}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	t.Name, _ = tmp["name"].(string)
	return nil
}
