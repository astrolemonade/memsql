package memsql

import (
	"fmt"
	"strconv"
	"strings"
)

type Database struct {
	tables map[string]*Table
}

type Table struct {
	name   string
	schema []*Column
	rows   []*Row
}

type Column struct {
	Name string
	Kind string
}

type Row struct {
	data map[string]interface{}
}

func NewDatabase() *Database {
	return &Database{
		tables: make(map[string]*Table),
	}
}

func (db *Database) CreateTable(name string, schema []*Column) error {
	if _, ok := db.tables[name]; ok {
		return fmt.Errorf("Table already exists")
	}
	db.tables[name] = &Table{
		name:   name,
		schema: schema,
		rows:   []*Row{},
	}
	return nil
}

func (db *Database) InsertRow(tableName string, values map[string]interface{}) error {
	table, ok := db.tables[tableName]
	if !ok {
		return fmt.Errorf("Table not found")
	}
	row := &Row{
		data: make(map[string]interface{}),
	}
	for _, column := range table.schema {
		if val, ok := values[column.Name]; ok {
			row.data[column.Name] = val
		} else {
			row.data[column.Name] = nil
		}
	}
	table.rows = append(table.rows, row)
	return nil
}

func (db *Database) SelectRows(tableName string, columns []string, where string) ([]*Row, error) {
	table, ok := db.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("Table not found")
	}
	selectedColumns := make(map[string]bool)
	if len(columns) == 0 {
		for _, column := range table.schema {
			selectedColumns[column.Name] = true
		}
	} else {
		for _, column := range columns {
			selectedColumns[column] = true
		}
	}
	selectedRows := []*Row{}
	for _, row := range table.rows {
		if where != "" {
			if !db.evalWhere(row, where) {
				continue
			}
		}
		selectedRow := &Row{
			data: make(map[string]interface{}),
		}
		for name, val := range row.data {
			if selectedColumns[name] {
				selectedRow.data[name] = val
			}
		}
		selectedRows = append(selectedRows, selectedRow)
	}
	return selectedRows, nil
}

func (db *Database) evalWhere(row *Row, where string) bool {
	tokens := strings.Split(where, " ")
	columnName := tokens[0]
	operator := tokens[1]
	valueStr := tokens[2]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false
	}
	if val, ok := row.data[columnName]; ok {
		switch operator {
		case "=":
			return val == value
		case "<":
			return val.(int) < value
		case ">":
			return val.(int) > value
		default:
			return false
		}
	}
	return false
}

func (db *Database) Execute(query string) ([]*Row, error) {
	parts := strings.Split(query, " ")

	switch strings.ToLower(parts[0]) {
	case "create":
		if parts[1] != "table" {
			return nil, fmt.Errorf("Invalid query: %s", query)
		}
		tableName := parts[2]
		columns := strings.Split(parts[3], ",")
		schema := make([]*Column, len(columns))
		for i, col := range columns {
			parts := strings.Split(col, " ")
			schema[i] = &Column{
				Name: strings.TrimSpace(parts[0]),
				Kind: strings.TrimSpace(parts[1]),
			}
		}
		err := db.CreateTable(tableName, schema)
		if err != nil {
			return nil, err
		}
		return []*Row{}, nil

	case "insert":
		if parts[1] != "into" {
			return nil, fmt.Errorf("Invalid query: %s", query)
		}
		tableName := parts[2]
		columns := strings.Split(parts[3], ",")
		values := strings.Split(parts[5], ",")
		row := make(map[string]interface{})
		for i, col := range columns {
			colName := strings.TrimSpace(col)
			valueStr := strings.TrimSpace(values[i])
			value, err := strconv.Atoi(valueStr)
			if err != nil {
				return nil, fmt.Errorf("Invalid value for column %s: %s", colName, valueStr)
			}
			row[colName] = value
		}
		err := db.InsertRow(tableName, row)
		if err != nil {
			return nil, err
		}
		return []*Row{}, nil

	case "select":
		if parts[1] != "*" || parts[2] != "from" {
			return nil, fmt.Errorf("Invalid query: %s", query)
		}
		tableName := parts[3]
		where := ""
		if len(parts) > 4 && parts[4] == "where" {
			where = parts[5]
		}
		rows, err := db.SelectRows(tableName, []string{}, where)
		if err != nil {
			return nil, err
		}
		return rows, nil

	default:
		return nil, fmt.Errorf("Invalid query: %s", query)
	}
}
