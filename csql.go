package csql

import (
	"database/sql"
)

// RowScanner manages column scanning (SQL data types)
type RowScanner interface {
	Scan(args ...any) error
}

// Schema defines expected behaviors for SQL documents
type Schema[T any] interface {
	ScanRow(RowScanner) error
	Fields() []any
	*T
}

// SQLTable manages a SQL table through a Schema definition
type SQLTable[T any, R Schema[T]] interface {
	// Query returns rows
	Query(query string) ([]T, error)
	// QueryRow returns a single row
	QueryRow(query string, args ...any) (T, error)
	// Exec executes a query
	Exec(query string, args ...any) error
	// Transaction attempt the prepared transaction using the row fields
	Transaction(transaction string, rows []T) (bool, error)
}

type sqlTableManager[T any, R Schema[T]] struct {
	db *sql.DB
}

// NewSQLTableManager returns a SQLTableManager
func NewSQLTableManager[T any, R Schema[T]](db *sql.DB) *sqlTableManager[T, R] {
	return &sqlTableManager[T, R]{
		db: db,
	}
}

func (m *sqlTableManager[_, _]) Exec(query string, args ...interface{}) error {
	_, err := m.db.Exec(query, args...)
	return err
}

func (m *sqlTableManager[T, R]) Transaction(transaction string, rows []T) (bool, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return false, err
	}
	stmt, err := tx.Prepare(transaction)
	if err != nil {
		return false, err
	}
	defer stmt.Close()
	for _, row := range rows {
		_, err = stmt.Exec(R(&row).Fields()...)
		if err != nil {
			return false, tx.Rollback()
		}
	}
	err = tx.Commit()
	return err == nil, err
}

func (m *sqlTableManager[T, R]) Query(query string) (rows []T, err error) {
	queryRows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer queryRows.Close()
	for queryRows.Next() {
		box := new(T)
		err = R(box).ScanRow(queryRows)
		if err != nil {
			return nil, err
		}
		rows = append(rows, *box)
	}
	return
}

func (m *sqlTableManager[T, R]) QueryRow(query string, args ...any) (row T, err error) {
	queryRow := m.db.QueryRow(query, args...)
	err = R(&row).ScanRow(queryRow)
	return
}
