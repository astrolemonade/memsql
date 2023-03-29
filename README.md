# MemSQL
MemSQL is a simple in-memory SQL database built in Go.

Features:
- Support for creating tables with custom schemas
- Support for inserting rows into tables
- Support for querying data from tables using SQL syntax


### Installation
To install MemSQL, first make sure you have Go installed on your system. Then run the following command:

```
go get github.com/yourusername/memsql
```
This will install MemSQL into your $GOPATH.

### Usage
To start a MemSQL server, run the following command:

```
memsql-server
```

This will start the server on the default port (8080) and with an empty database.

To connect to the server using a client, run the following command:

```
memsql
```
This will start the command-line interface for the MemSQL client.

### License
MemSQL is licensed under the GNU v2 license. See the LICENSE file for more information.

### Contributing
If you'd like to contribute to MemSQL, please fork the repository and create a pull request.