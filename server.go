package memsql

import (
	"fmt"
	"net"
)

type Server struct {
	listener  net.Listener
	databases map[string]*Database
}

func NewServer() *Server {
	return &Server{
		databases: make(map[string]*Database),
	}
}

func (s *Server) Listen(port int) error {
	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer s.listener.Close()

	fmt.Printf("Server listening on port %d\n", port)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		client := NewClient(conn, s)
		go client.Handle()
	}
}

func (s *Server) CreateDatabase(name string) error {
	if _, ok := s.databases[name]; ok {
		return fmt.Errorf("Database already exists")
	}
	s.databases[name] = NewDatabase()
	return nil
}

func (s *Server) GetDatabase(name string) (*Database, error) {
	if db, ok := s.databases[name]; ok {
		return db, nil
	}
	return nil, fmt.Errorf("Database not found")
}

func (s *Server) DeleteDatabase(name string) error {
	if _, ok := s.databases[name]; !ok {
		return fmt.Errorf("Database not found")
	}
	delete(s.databases, name)
	return nil
}

