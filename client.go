package memsql

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type Client struct {
	conn   net.Conn
	server *Server
	db     *Database
}

func NewClient(conn net.Conn, server *Server) *Client {
	return &Client{
		conn:   conn,
		server: server,
	}
}

func (c *Client) Handle() {
	defer c.conn.Close()

	reader := bufio.NewReader(c.conn)
	writer := bufio.NewWriter(c.conn)

	for {
		query, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		query = strings.TrimSpace(query)

		if query == "quit" {
			return
		}

		parts := strings.Split(query, " ")

		if len(parts) < 2 {
			writer.WriteString("Error: Invalid query\n")
			writer.Flush()
			continue
		}

		switch strings.ToLower(parts[0]) {
		case "use":
			name := parts[1]
			db, err := c.server.GetDatabase(name)
			if err != nil {
				writer.WriteString(fmt.Sprintf("Error: %s\n", err))
				writer.Flush()
				continue
			}
			c.db = db
			writer.WriteString(fmt.Sprintf("Using database %s\n", name))
			writer.Flush()

		case "create":
			if len(parts) < 3 || parts[1] != "database" {
				writer.WriteString("Error: Invalid query\n")
				writer.Flush()
				continue
			}
			name := parts[2]
			err := c.server.CreateDatabase(name)
			if err != nil {
				writer.WriteString(fmt.Sprintf("Error: %s\n", err))
				writer.Flush()
				continue
			}
			writer.WriteString(fmt.Sprintf("Created database %s\n", name))
			writer.Flush()

		case "delete":
			if len(parts) < 3 || parts[1] != "database" {
				writer.WriteString("Error: Invalid query\n")
				writer.Flush()
				continue
			}
			name := parts[2]
			err := c.server.DeleteDatabase(name)
			if err != nil {
				writer.WriteString(fmt.Sprintf("Error: %s\n", err))
				writer.Flush()
				continue
			}
			writer.WriteString(fmt.Sprintf("Deleted database %s\n", name))
			writer.Flush()

		default:
			if c.db == nil {
				writer.WriteString("Error: No database selected\n")
				writer.Flush()
				continue
			}
			rows, err := c.db.Execute(query)
			if err != nil {
				writer.WriteString(fmt.Sprintf("Error: %s\n", err))
				writer.Flush()
				continue
			}
			for _, row := range rows {
				for _, val := range row.data {
					writer.WriteString(fmt.Sprintf("%v ", val))
				}
				writer.WriteString("\n")
			}
			writer.Flush()
		}
	}
}
