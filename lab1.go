package triblab

import . "trib"

// Creates an RPC client that connects to addr.
func NewClient(addr string) Storage {
	return &client{addr: addr}
}

// Serve as a backend based on the given configuration
func ServeBack(b *BackConfig) error {
	s := server{};
	return s.init(b);
}
