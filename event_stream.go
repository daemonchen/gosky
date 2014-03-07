package sky

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

// Stream maintains an open connection to the database to send events in bulk.
type Stream struct {
	Client  *Client
	header  []byte
	encoder *json.Encoder
	chunker *chunkWriter
	buffer  *bufio.Writer
	conn    net.Conn
}

// EventStream is a table-less stream.
type EventStream struct {
	*Stream
}

// TableEventStream is a stream to a specific table.
type TableEventStream struct {
	*Stream
	table *Table
}

func NewTableEventStream(c *Client, t *Table) (*TableEventStream, error) {
	header := fmt.Sprintf("PATCH /tables/%s/events HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n", t.Name, c.Host)
	s := &TableEventStream{&Stream{Client: c, header: []byte(header)}, t}
	return s, s.Reconnect()
}

func NewEventStream(c *Client) (*EventStream, error) {
	header := fmt.Sprintf("PATCH /events HTTP/1.0\r\nHost: %s\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n", c.Host)
	s := &EventStream{&Stream{Client: c, header: []byte(header)}}
	return s, s.Reconnect()
}

// AddEvent sends an event through the stream.
func (s *TableEventStream) InsertEvent(id string, event *Event) error {
	if id == "" {
		return errors.New("Object identifier required")
	}
	if event == nil {
		return errors.New("Event required")
	}

	// Attach the object identifier at the root of the event.
	data := event.Serialize()
	data["id"] = id

	// Encode the serialized data into the stream.
	return s.encoder.Encode(data)
}

// InsertEvent sends an event through the stream.
func (s *EventStream) InsertEvent(t *Table, id string, event *Event) error {
	if id == "" {
		return errors.New("Object identifier required")
	}
	if t == nil {
		return ErrTableRequired
	}
	if event == nil {
		return errors.New("Event required")
	}

	// Attach the object identifier at the root of the event.
	data := event.Serialize()
	data["id"] = id
	data["table"] = t.Name

	// Encode the serialized data into the stream.
	return s.encoder.Encode(data)
}

// Flush sends any buffered events to the server.
func (s *Stream) Flush() error {
	return s.buffer.Flush()
}

// Close closes the event stream.
func (s *Stream) Close() error {
	defer s.conn.Close()

	// Flush any buffered events
	if err := s.Flush(); err != nil {
		return err
	}

	// Write an empty chunk
	if _, err := s.chunker.Write([]byte{}); err != nil {
		return err
	}

	// Check server response status
	reader := bufio.NewReader(s.conn)
	status, err := reader.ReadString('\r')
	if err != nil {
		return err
	}
	if strings.HasPrefix(status, "HTTP/1.0 200") {
		return nil
	}
	return errors.New(status)
}

// Reconnect attempts to reconnect the event stream with the server.
func (s *Stream) Reconnect() error {

	// Close the existing connection
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}

	// Open new connection
	conn, err := net.Dial("tcp", s.Client.Host)
	if err != nil {
		return err
	}

	// Write the request header (chunked transfer encoding)
	if _, err = conn.Write(s.header); err != nil {
		conn.Close()
		return err
	}

	// Finish setting up the stream
	s.conn = conn
	s.chunker = &chunkWriter{conn}
	s.buffer = bufio.NewWriter(s.chunker)
	s.encoder = json.NewEncoder(s.buffer)
	return nil
}

// chunkWriter is an io.Writer that will emit any writes in HTTP chunk format
type chunkWriter struct {
	w io.Writer
}

func (cw *chunkWriter) Write(p []byte) (int, error) {
	var err error

	// Emit the chunk header
	if _, err = fmt.Fprintf(cw.w, "%x\r\n", len(p)); err != nil {
		return 0, err
	}

	// Emit the chunk body
	var total, count int
	for len(p) > 0 {
		count, err = cw.w.Write(p)
		if !(count > 0) {
			break
		}
		p = p[count:]
		total += count
	}
	if err != nil {
		return total, err
	}

	// Emit chunk trailer
	if _, err = fmt.Fprint(cw.w, "\r\n"); err != nil {
		return total, err
	}
	return total, nil
}
