package sky

const (
	String  = "string"
	Integer = "integer"
	Float   = "float"
	Boolean = "boolean"
	Factor  = "factor"
)

// Property represents a field in a Sky table.
type Property struct {
	Name      string `json:"name"`
	Transient bool   `json:"transient"`
	DataType  string `json:"dataType"`
}
