package sky

import (
	"errors"
)

var (
	// ErrClientRequired is returned when a table does not have an associated
	// client reference.
	ErrClientRequired = errors.New("client required")

	// ErrTableRequired is returned when a nil table reference is used.
	ErrTableRequired = errors.New("table required")

	// ErrTableNameRequired is returned when a blank table name is used.
	ErrTableNameRequired = errors.New("table name required")

	// ErrPropertyRequired is returned when a nil property is used.
	ErrPropertyRequired = errors.New("property required")

	// ErrPropertyNameRequired is returned when a blank property name is used.
	ErrPropertyNameRequired = errors.New("property name required")

	// ErrIDRequired is returned when a blank object identifer is used.
	ErrIDRequired = errors.New("id required")

	// ErrEventRequired is returned when a nil Event reference is used.
	ErrEventRequired = errors.New("event required")

	// ErrQueryRequired is returned when a blank query string is used.
	ErrQueryRequired = errors.New("query required")
)
