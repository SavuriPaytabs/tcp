package tcpclient

// ConnectionStatus represents the state of the connection
type ConnectionStatus int

const (
	ConnectionNotEstablished ConnectionStatus = iota
	ConnectionEstablished
	ConnectionDisconnected
)

// LengthFormat defines how message lengths are encoded
type LengthFormat int

const (
	FormatASCII LengthFormat = iota
	FormatBCD
	FormatBinary
	FormatHEX
	FormatCustom
)

// ReadMode specifies synchronous or asynchronous reading
type ReadMode int

const (
	ReadSync ReadMode = iota
	ReadAsync
)

// CloseConnectionReason explains why a connection was closed
type CloseConnectionReason string

const (
	ReasonNonPersistent CloseConnectionReason = "NON_PERSISTENT_CONNECTION"
	ReasonManualClose   CloseConnectionReason = "MANUAL_CLOSE"
	ReasonError         CloseConnectionReason = "ERROR"
)
