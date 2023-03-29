package main

import (
	"github.com/hyperioxx/memsql"
)

func main() {
	server := memsql.NewServer()
	err := server.Listen(8080)
	if err != nil {
		panic(err)
	}
}
