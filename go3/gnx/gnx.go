package gnx

/*
#cgo pkg-config: groonga
#include "gnx_cgo.h"
*/
import "C"

import (
	"fmt"
	"math"
)

// -- Data types --

type Bool uint8
type Int int64
type Float float64
type GeoPoint struct{ Latitude, Longitude int32 }
type Text []byte

type BoolVector []Bool
type IntVector []Int
type FloatVector []Float
type GeoPointVector []GeoPoint
type TextVector []Text

const (
	True  = Bool(3)
	False = Bool(0)
)

func NullBool() Bool         { return Bool(1) }
func NullInt() Int           { return Int(math.MinInt64) }
func NullFloat() Float       { return Float(math.NaN()) }
func NullGeoPoint() GeoPoint { return GeoPoint{math.MinInt32, math.MinInt32} }
func NullText() Text         { return nil }

func NullBoolVector() BoolVector         { return nil }
func NullIntVector() IntVector           { return nil }
func NullFloatVector() FloatVector       { return nil }
func NullGeoPointVector() GeoPointVector { return nil }
func NullTextVector() TextVector         { return nil }

type TypeID int

const (
	VoidID = TypeID(iota)
	BoolID
	IntID
	FloatID
	GeoPointID
	TextID
	BoolVectorID
	IntVectorID
	FloatVectorID
	GeoPointVectorID
	TextVectorID
)

func (id TypeID) String() string {
	switch id {
	case VoidID:
		return "Void"
	case BoolID:
		return "Bool"
	case IntID:
		return "Int"
	case FloatID:
		return "Float"
	case GeoPointID:
		return "GeoPoint"
	case TextID:
		return "Text"
	case BoolVectorID:
		return "BoolVector"
	case IntVectorID:
		return "IntVector"
	case FloatVectorID:
		return "FloatVector"
	case GeoPointVectorID:
		return "GeoPointVector"
	case TextVectorID:
		return "TextVector"
	default:
		return fmt.Sprintf("TypeID(%d)", id)
	}
}

// -- DB --

type DB struct {
	*GrnDB
	*GrnxxDB
	tables map[string]*Table
}

func newDB(grnDB *GrnDB, grnxxDB *GrnxxDB) *DB {
	return &DB{grnDB, grnxxDB, make(map[string]*Table)}
}

func CreateDB(path string) (*DB, error) {
	grnDB, err := CreateGrnDB(path)
	if err != nil {
		return nil, err
	}
	return newDB(grnDB, nil), nil
}

func OpenDB(path string) (*DB, error) {
	grnDB, err := OpenGrnDB(path)
	if err != nil {
		return nil, err
	}
	return newDB(grnDB, nil), nil
}

func (db *DB) Close() error {
	var grnErr error
	var grnxxErr error
	if db.GrnDB != nil {
		grnErr = db.GrnDB.Close()
	}
	if db.GrnxxDB != nil {
		grnxxErr = db.GrnxxDB.Close()
	}
	if grnErr != nil {
		return grnErr
	}
	return grnxxErr
}

func (db *DB) CreateTable(name string, options *TableOptions) (*Table, error) {
	// TODO
	return nil, fmt.Errorf("not supported yet")
}

func (db *DB) RemoveTable(name string) error {
	// TODO
	return fmt.Errorf("not supported yet")
}

func (db *DB) RenameTable(name, newName string) error {
	// TODO
	return fmt.Errorf("not supported yet")
}

func (db *DB) FindTable(name string) (*Table, error) {
	if table, ok := db.tables[name]; ok {
		return table, nil
	}
	// TODO
	return nil, fmt.Errorf("not supported yet")
}

func (db *DB) FindRow(tableName string, key interface{}) (Int, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return NullInt(), err
	}
	return table.FindRow(key)
}

func (db *DB) InsertRow(tableName string, key interface{}) (bool, Int, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return false, NullInt(), err
	}
	return table.InsertRow(key)
}

func (db *DB) CreateColumn(tableName, columnName, valueType string,
	options *ColumnOptions) (*Column, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.CreateColumn(columnName, valueType, options)
}

func (db *DB) RemoveColumn(tableName, columnName string) error {
	table, err := db.FindTable(tableName)
	if err != nil {
		return err
	}
	return table.RemoveColumn(columnName)
}

func (db *DB) RenameColumn(tableName, columnName, newColumnName string) error {
	table, err := db.FindTable(tableName)
	if err != nil {
		return err
	}
	return table.RenameColumn(columnName, newColumnName)
}

func (db *DB) FindColumn(tableName, columnName string) (*Column, error) {
	table, err := db.FindTable(tableName)
	if err != nil {
		return nil, err
	}
	return table.FindColumn(columnName)
}

func (db *DB) SetValue(
	tableName, columnName string, id Int, value interface{}) error {
	table, err := db.FindTable(tableName)
	if err != nil {
		return err
	}
	return table.SetValue(columnName, id, value)
}

// -- Table --

type Table struct {
	*GrnTable
	*GrnxxTable
	columns map[string]*Column
}

func (table *Table) FindRow(key interface{}) (Int, error) {
	// TODO
	return NullInt(), fmt.Errorf("not supported yet")
}

func (table *Table) InsertRow(key interface{}) (bool, Int, error) {
	// TODO
	return false, NullInt(), fmt.Errorf("not supported yet")
}

func (table *Table) CreateColumn(
	name, valueType string, options *ColumnOptions) (*Column, error) {
	// TODO
	return nil, fmt.Errorf("not supported yet")
}

func (table *Table) RemoveColumn(name string) error {
	// TODO
	return fmt.Errorf("not supported yet")
}

func (table *Table) RenameColumn(name, newName string) error {
	// TODO
	return fmt.Errorf("not supported yet")
}

func (table *Table) FindColumn(name string) (*Column, error) {
	if column, ok := table.columns[name]; ok {
		return column, nil
	}
	// TODO
	return nil, fmt.Errorf("not supported yet")
}

func (table *Table) SetValue(
	columnName string, id Int, value interface{}) error {
	column, err := table.FindColumn(columnName)
	if err != nil {
		return err
	}
	return column.SetValue(id, value)
}

// -- Column --

type Column struct {
	*GrnColumn
	*GrnxxColumn
}

func (column *Column) SetValue(id Int, value interface{}) error {
	// TODO
	return fmt.Errorf("not suported yet")
}
