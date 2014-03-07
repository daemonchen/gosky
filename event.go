package sky

import (
	"fmt"
	"time"
)

// Event represents a state or action at a given point in time.
type Event struct {
	Timestamp time.Time
	Data      map[string]interface{}
}

// Serialize encodes an event into an untyped map.
func (e *Event) Serialize() map[string]interface{} {
	return map[string]interface{}{
		"timestamp": FormatTimestamp(e.Timestamp),
		"data":      e.Data,
	}
}

// Deserialize decodes an event from an untyped map.
func (e *Event) Deserialize(obj map[string]interface{}) error {
	if obj == nil {
		return nil
	}

	// Deserialize "timestamp".
	if str, ok := obj["timestamp"].(string); ok {
		if timestamp, err := ParseTimestamp(str); err == nil {
			e.Timestamp = timestamp
		} else {
			return err
		}
	} else {
		return fmt.Errorf("sky.Event: Invalid timestamp: %v", obj["timestamp"])
	}

	// Deserialize "data".
	if data, ok := obj["data"].(map[string]interface{}); ok {
		e.Data = data
	} else if data == nil {
		e.Data = map[string]interface{}{}
	} else {
		return fmt.Errorf("invalid data: %v", obj["data"])
	}

	return nil
}
