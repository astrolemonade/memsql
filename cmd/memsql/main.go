package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

type MemSQLClient struct {
	conn net.Conn
}

func NewSQLClient(addr string) (*MemSQLClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &MemSQLClient{conn: conn}, nil
}

func (c *MemSQLClient) Close() error {
	return c.conn.Close()
}

func (c *MemSQLClient) Execute(query string) (string, error) {
	_, err := fmt.Fprintln(c.conn, query)
	if err != nil {
		return "", err
	}

	result, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(result), nil
}

func main() {
	client, err := NewSQLClient("localhost:8080")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error connecting to server:", err)
		return
	}
	defer client.Close()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error reading input:", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "exit" {
			break
		}

		result, err := client.Execute(input)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error executing query:", err)
			continue
		}

		fmt.Println(result)
	}
}
