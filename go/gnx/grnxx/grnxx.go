package grnxx

// #cgo CXXFLAGS: -std=c++11
// #cgo LDFLAGS: -lgrnxx -lstdc++
//
// #include "grnxx.h"
//
// #include <stdlib.h>
import "C"
import "fmt"
import "math"
import "reflect"
import "unsafe"

// -- Data types --

type Bool uint8
type Int int64
type Float float64
type GeoPoint struct {
	Latitude  int32
	Longitude int32
}
type Text []byte

type Valuer interface {
	IsNA() bool
}

func NABool() Bool {
	return Bool(1)
}
func NAInt() Int {
	return Int(math.MinInt64)
}
func NAFloat() Float {
	return Float(math.NaN())
}
func NAGeoPoint() GeoPoint {
	return GeoPoint{math.MinInt32, math.MinInt32}
}
func NAText() Text {
	return nil
}

func (this Bool) IsNA() bool {
	return this == 1
}
func (this Int) IsNA() bool {
	return this == math.MinInt64
}
func (this Float) IsNA() bool {
	return math.IsNaN(float64(this))
}
func (this GeoPoint) IsNA() bool {
	return this.Latitude == math.MinInt32
}
func (this Text) IsNA() bool {
	return this == nil
}

func (this Text) Convert() C.grnxx_text {
	switch {
	case this == nil:
		return C.grnxx_text{nil, C.int64_t(math.MinInt64)}
	case len(this) == 0:
		return C.grnxx_text{nil, C.int64_t(0)}
	default:
		ptr := (*C.char)(unsafe.Pointer(&this[0]))
		return C.grnxx_text{ptr, C.int64_t(len(this))}
	}
}

type Record struct {
	RowID Int
	Score Float
}

// -- Constants --

const (
	NA    = Bool(1)
	TRUE  = Bool(3)
	FALSE = Bool(0)
)

type DataType int

const (
	BOOL      = DataType(C.GRNXX_BOOL)
	INT       = DataType(C.GRNXX_INT)
	FLOAT     = DataType(C.GRNXX_FLOAT)
	GEO_POINT = DataType(C.GRNXX_GEO_POINT)
	TEXT      = DataType(C.GRNXX_TEXT)
)

type IndexType int

const (
	TREE_INDEX = IndexType(C.GRNXX_TREE_INDEX)
	HASH_INDEX = IndexType(C.GRNXX_HASH_INDEX)
)

type OrderType int

const (
	REGULAR_ORDER = OrderType(C.GRNXX_REGULAR_ORDER)
	REVERSE_ORDER = OrderType(C.GRNXX_REVERSE_ORDER)
)

type OperatorType int

const (
	LOGICAL_NOT    = OperatorType(C.GRNXX_LOGICAL_NOT)
	BITWISE_NOT    = OperatorType(C.GRNXX_BITWISE_NOT)
	POSITIVE       = OperatorType(C.GRNXX_POSITIVE)
	NEGATIVE       = OperatorType(C.GRNXX_NEGATIVE)
	TO_INT         = OperatorType(C.GRNXX_TO_INT)
	TO_FLOAT       = OperatorType(C.GRNXX_TO_FLOAT)
	LOGICAL_AND    = OperatorType(C.GRNXX_LOGICAL_AND)
	LOGICAL_OR     = OperatorType(C.GRNXX_LOGICAL_OR)
	EQUAL          = OperatorType(C.GRNXX_EQUAL)
	NOT_EQUAL      = OperatorType(C.GRNXX_NOT_EQUAL)
	LESS           = OperatorType(C.GRNXX_LESS)
	LESS_EQUAL     = OperatorType(C.GRNXX_LESS_EQUAL)
	GREATER        = OperatorType(C.GRNXX_GREATER)
	GREATER_EQUAL  = OperatorType(C.GRNXX_GREATER_EQUAL)
	BITWISE_AND    = OperatorType(C.GRNXX_BITWISE_AND)
	BITWISE_OR     = OperatorType(C.GRNXX_BITWISE_OR)
	BITWISE_XOR    = OperatorType(C.GRNXX_BITWISE_XOR)
	PLUS           = OperatorType(C.GRNXX_PLUS)
	MINUS          = OperatorType(C.GRNXX_MINUS)
	MULTIPLICATION = OperatorType(C.GRNXX_MULTIPLICATION)
	DIVISION       = OperatorType(C.GRNXX_DIVISION)
	MODULUS        = OperatorType(C.GRNXX_MODULUS)
	STARTS_WITH    = OperatorType(C.GRNXX_STARTS_WITH)
	ENDS_WITH      = OperatorType(C.GRNXX_ENDS_WITH)
	CONTAINS       = OperatorType(C.GRNXX_CONTAINS)
	SUBSCRIPT      = OperatorType(C.GRNXX_SUBSCRIPT)
)

type MergerOperatorType int

const (
	MERGER_AND            = MergerOperatorType(C.GRNXX_MERGER_AND)
	MERGER_OR             = MergerOperatorType(C.GRNXX_MERGER_OR)
	MERGER_XOR            = MergerOperatorType(C.GRNXX_MERGER_XOR)
	MERGER_PLUS           = MergerOperatorType(C.GRNXX_MERGER_PLUS)
	MERGER_MINUS          = MergerOperatorType(C.GRNXX_MERGER_MINUS)
	MERGER_MULTIPLICATION = MergerOperatorType(C.GRNXX_MERGER_MULTIPLICATION)
	MERGER_LEFT           = MergerOperatorType(C.GRNXX_MERGER_LEFT)
	MERGER_RIGHT          = MergerOperatorType(C.GRNXX_MERGER_RIGHT)
	MERGER_ZERO           = MergerOperatorType(C.GRNXX_MERGER_ZERO)
)

// -- Library --

func Package() string {
	return C.GoString(C.grnxx_package())
}

func Version() string {
	return C.GoString(C.grnxx_version())
}

// -- DB --

type DB struct {
	handle *C.grnxx_db
}

func CreateDB() (*DB, error) {
	db := C.grnxx_db_create()
	if db == nil {
		return nil, fmt.Errorf("grnxx_db_create() failed")
	}
	return &DB{db}, nil
}

func (db *DB) Close() error {
	C.grnxx_db_close(db.handle)
	return nil
}

func (db *DB) NumTables() int {
	return int(C.grnxx_db_num_tables(db.handle))
}

func (db *DB) CreateTable(name string) (*Table, error) {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	table := C.grnxx_db_create_table(db.handle, nameCString)
	if table == nil {
		return nil, fmt.Errorf("grnxx_db_create_table() failed")
	}
	return &Table{table}, nil
}

func (db *DB) RemoveTable(name string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	if !C.grnxx_db_remove_table(db.handle, nameCString) {
		return fmt.Errorf("grnxx_db_remove_table() failed")
	}
	return nil
}

func (db *DB) RenameTable(name string, newName string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	newNameCString := C.CString(newName)
	defer C.free(unsafe.Pointer(newNameCString))
	if !C.grnxx_db_rename_table(db.handle, nameCString, newNameCString) {
		return fmt.Errorf("grnxx_db_rename_table() failed")
	}
	return nil
}

func (db *DB) ReorderTable(name string, prevName string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	prevNameCString := C.CString(prevName)
	defer C.free(unsafe.Pointer(prevNameCString))
	if !C.grnxx_db_reorder_table(db.handle, nameCString, prevNameCString) {
		return fmt.Errorf("grnxx_db_reorder_table() failed")
	}
	return nil
}

func (db *DB) GetTable(tableID int) *Table {
	return &Table{C.grnxx_db_get_table(db.handle, C.size_t(tableID))}
}

func (db *DB) FindTable(name string) *Table {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	table := C.grnxx_db_find_table(db.handle, nameCString)
	if table == nil {
		return nil
	}
	return &Table{table}
}

// -- Table --

type Table struct {
	handle *C.grnxx_table
}

func (table *Table) DB() (db *DB) {
	return &DB{C.grnxx_table_db(table.handle)}
}

func (table *Table) Name() string {
	var size C.size_t
	name := C.grnxx_table_name(table.handle, &size)
	return C.GoStringN(name, C.int(size))
}

func (table *Table) NumColumns() int {
	return int(C.grnxx_table_num_columns(table.handle))
}

func (table *Table) KeyColumn() *Column {
	column := C.grnxx_table_key_column(table.handle)
	if column == nil {
		return nil
	}
	return &Column{column}
}

func (table *Table) NumRows() int {
	return int(C.grnxx_table_num_rows(table.handle))
}

func (table *Table) MaxRowID() Int {
	return Int(C.grnxx_table_max_row_id(table.handle))
}

func (table *Table) IsEmpty() bool {
	return bool(C.grnxx_table_is_empty(table.handle))
}

func (table *Table) IsFull() bool {
	return bool(C.grnxx_table_is_full(table.handle))
}

func (table *Table) CreateColumn(
	name string, dataType DataType, options *ColumnOptions) (*Column, error) {
	var internalOptions *C.grnxx_column_options
	if options != nil {
		cString := C.CString(options.ReferenceTableName)
		defer C.free(unsafe.Pointer(cString))
		internalOptions = &C.grnxx_column_options{cString}
	}
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	column := C.grnxx_table_create_column(table.handle, nameCString,
		C.grnxx_data_type(dataType), internalOptions)
	if column == nil {
		return nil, fmt.Errorf("grnxx_table_create_column() failed")
	}
	return &Column{column}, nil
}

func (table *Table) RemoveColumn(name string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	if !C.grnxx_table_remove_column(table.handle, nameCString) {
		return fmt.Errorf("grnxx_table_remove_column() failed")
	}
	return nil
}

func (table *Table) RenameColumn(name string, newName string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	newNameCString := C.CString(newName)
	defer C.free(unsafe.Pointer(newNameCString))
	if !C.grnxx_table_rename_column(table.handle, nameCString, newNameCString) {
		return fmt.Errorf("grnxx_table_rename_column() failed")
	}
	return nil
}

func (table *Table) ReorderColumn(name string, prevName string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	prevNameCString := C.CString(prevName)
	defer C.free(unsafe.Pointer(prevNameCString))
	if !C.grnxx_table_reorder_column(table.handle, nameCString, prevNameCString) {
		return fmt.Errorf("grnxx_table_reorder_column() failed")
	}
	return nil
}

func (table *Table) GetColumn(columnID int) *Column {
	return &Column{C.grnxx_table_get_column(table.handle, C.size_t(columnID))}
}

func (table *Table) FindColumn(name string) *Column {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	column := C.grnxx_table_find_column(table.handle, nameCString)
	if column == nil {
		return nil
	}
	return &Column{column}
}

func (table *Table) SetKeyColumn(name string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	if !C.grnxx_table_set_key_column(table.handle, nameCString) {
		return fmt.Errorf("grnxx_table_set_key_column() failed")
	}
	return nil
}

func (table *Table) UnsetKeyColumn() error {
	if !C.grnxx_table_unset_key_column(table.handle) {
		return fmt.Errorf("grnxx_table_unset_key_column() failed")
	}
	return nil
}

func (table *Table) InsertRow(key Valuer) (Int, error) {
	var rowID C.int64_t
	switch x := key.(type) {
	case nil:
		rowID = C.grnxx_table_insert_row(table.handle, nil)
	case Bool:
		rowID = C.grnxx_table_insert_row(table.handle, unsafe.Pointer(&x))
	case Int:
		rowID = C.grnxx_table_insert_row(table.handle, unsafe.Pointer(&x))
	case Float:
		rowID = C.grnxx_table_insert_row(table.handle, unsafe.Pointer(&x))
	case GeoPoint:
		rowID = C.grnxx_table_insert_row(table.handle, unsafe.Pointer(&x))
	case Text:
		text := x.Convert()
		rowID = C.grnxx_table_insert_row(table.handle, unsafe.Pointer(&text))
	default:
		return NAInt(), fmt.Errorf("unsupported data type")
	}
	if rowID < 0 {
		return Int(rowID), fmt.Errorf("grnxx_table_insert_row() failed")
	}
	return Int(rowID), nil
}

func (table *Table) InsertRows(keys interface{}) ([]Int, error) {
	var rowIDs []Int
	var count int
	switch x := keys.(type) {
	case []Bool:
		if len(x) == 0 {
			return nil, fmt.Errorf("no data")
		}
		rowIDs = make([]Int, len(x))
		count = int(C.grnxx_table_insert_rows(table.handle, C.size_t(len(x)),
			unsafe.Pointer(&x[0]), (*C.int64_t)(unsafe.Pointer(&rowIDs[0]))))
	case []Int:
		if len(x) == 0 {
			return nil, fmt.Errorf("no data")
		}
		rowIDs = make([]Int, len(x))
		count = int(C.grnxx_table_insert_rows(table.handle, C.size_t(len(x)),
			unsafe.Pointer(&x[0]), (*C.int64_t)(unsafe.Pointer(&rowIDs[0]))))
	case []Float:
		if len(x) == 0 {
			return nil, fmt.Errorf("no data")
		}
		rowIDs = make([]Int, len(x))
		count = int(C.grnxx_table_insert_rows(table.handle, C.size_t(len(x)),
			unsafe.Pointer(&x[0]), (*C.int64_t)(unsafe.Pointer(&rowIDs[0]))))
	case []GeoPoint:
		if len(x) == 0 {
			return nil, fmt.Errorf("no data")
		}
		rowIDs = make([]Int, len(x))
		count = int(C.grnxx_table_insert_rows(table.handle, C.size_t(len(x)),
			unsafe.Pointer(&x[0]), (*C.int64_t)(unsafe.Pointer(&rowIDs[0]))))
	case []Text:
		if len(x) == 0 {
			return nil, fmt.Errorf("no data")
		}
		internalKeys := make([]C.grnxx_text, len(x))
		for i := 0; i < len(x); i++ {
			internalKeys[i].data = (*C.char)(unsafe.Pointer(&x[i][0]))
			internalKeys[i].size = C.int64_t(len(x[i]))
		}
		rowIDs = make([]Int, len(x))
		count = int(C.grnxx_table_insert_rows(table.handle, C.size_t(len(x)),
			unsafe.Pointer(&internalKeys[0]), (*C.int64_t)(unsafe.Pointer(&rowIDs[0]))))
	default:
		return nil, fmt.Errorf("unsupported data type")
	}
	if count < len(rowIDs) {
		return rowIDs[:count], fmt.Errorf("grnxx_table_insert_rows() failed")
	}
	return rowIDs, nil
}

func (table *Table) InsertRowAt(rowID Int, key Valuer) error {
	var result C.bool
	switch x := key.(type) {
	case nil:
		result = C.grnxx_table_insert_row_at(
			table.handle, C.int64_t(rowID), nil)
	case Bool:
		result = C.grnxx_table_insert_row_at(
			table.handle, C.int64_t(rowID), unsafe.Pointer(&x))
	case Int:
		result = C.grnxx_table_insert_row_at(
			table.handle, C.int64_t(rowID), unsafe.Pointer(&x))
	case Float:
		result = C.grnxx_table_insert_row_at(
			table.handle, C.int64_t(rowID), unsafe.Pointer(&x))
	case GeoPoint:
		result = C.grnxx_table_insert_row_at(
			table.handle, C.int64_t(rowID), unsafe.Pointer(&x))
	case Text:
		text := x.Convert()
		result = C.grnxx_table_insert_row_at(
			table.handle, C.int64_t(rowID), unsafe.Pointer(&text))
	default:
		return fmt.Errorf("unsupported data type")
	}
	if !result {
		return fmt.Errorf("grnxx_table_insert_row_at() failed")
	}
	return nil
}

func (table *Table) FindOrInsertRow(key Valuer) (Int, bool, error) {
	var inserted C.bool
	var internalRowID C.int64_t
	switch x := key.(type) {
	case nil:
		internalRowID = C.grnxx_table_find_or_insert_row(
			table.handle, nil, &inserted)
	case Bool:
		internalRowID = C.grnxx_table_find_or_insert_row(
			table.handle, unsafe.Pointer(&x), &inserted)
	case Int:
		internalRowID = C.grnxx_table_find_or_insert_row(
			table.handle, unsafe.Pointer(&x), &inserted)
	case Float:
		internalRowID = C.grnxx_table_find_or_insert_row(
			table.handle, unsafe.Pointer(&x), &inserted)
	case GeoPoint:
		internalRowID = C.grnxx_table_find_or_insert_row(
			table.handle, unsafe.Pointer(&x), &inserted)
	case Text:
		text := x.Convert()
		internalRowID = C.grnxx_table_find_or_insert_row(
			table.handle, unsafe.Pointer(&text), &inserted)
	default:
		return NAInt(), false, fmt.Errorf("unsupported data type")
	}
	rowID := Int(internalRowID)
	if rowID.IsNA() {
		return rowID, false, fmt.Errorf("grnxx_table_find_or_insert_row() failed")
	}
	return rowID, bool(inserted), nil
}

func (table *Table) RemoveRow(rowID Int) error {
	if !C.grnxx_table_remove_row(table.handle, C.int64_t(rowID)) {
		return fmt.Errorf("grnxx_table_remove_row() failed")
	}
	return nil
}

func (table *Table) TestRow(rowID Int) bool {
	return bool(C.grnxx_table_test_row(table.handle, C.int64_t(rowID)))
}

func (table *Table) FindRow(key Valuer) Int {
	switch x := key.(type) {
	case Bool:
		return Int(C.grnxx_table_find_row(table.handle, unsafe.Pointer(&x)))
	case Int:
		return Int(C.grnxx_table_find_row(table.handle, unsafe.Pointer(&x)))
	case Float:
		return Int(C.grnxx_table_find_row(table.handle, unsafe.Pointer(&x)))
	case GeoPoint:
		return Int(C.grnxx_table_find_row(table.handle, unsafe.Pointer(&x)))
	case Text:
		text := x.Convert()
		return Int(C.grnxx_table_find_row(table.handle, unsafe.Pointer(&text)))
	}
	return NAInt()
}

func (table *Table) CreateCursor(options *CursorOptions) (*Cursor, error) {
	cursor := C.grnxx_table_create_cursor(
		table.handle, convertCursorOptions(options))
	if cursor == nil {
		return nil, fmt.Errorf("grnxx_table_create_cursor() failed")
	}
	return &Cursor{cursor}, nil
}

// -- Column --

type ColumnOptions struct {
	ReferenceTableName string
}

type Column struct {
	handle *C.grnxx_column
}

func (column *Column) Table() *Table {
	return &Table{C.grnxx_column_table(column.handle)}
}

func (column *Column) Name() string {
	var size C.size_t
	name := C.grnxx_column_name(column.handle, &size)
	return C.GoStringN(name, C.int(size))
}

func (column *Column) DataType() DataType {
	return DataType(C.grnxx_column_data_type(column.handle))
}

func (column *Column) ReferenceTable() *Table {
	referenceTable := C.grnxx_column_reference_table(column.handle)
	if referenceTable == nil {
		return nil
	}
	return &Table{referenceTable}
}

func (column *Column) IsKey() bool {
	return bool(C.grnxx_column_is_key(column.handle))
}

func (column *Column) NumIndexes() int {
	return int(C.grnxx_column_num_indexes(column.handle))
}

func (column *Column) CreateIndex(name string, indexType IndexType) (*Index, error) {
	nameCString := C.CString(name)
	index := C.grnxx_column_create_index(column.handle, nameCString,
		C.grnxx_index_type(indexType))
	defer C.free(unsafe.Pointer(nameCString))
	if index == nil {
		return nil, fmt.Errorf("grnxx_column_create_index() failed")
	}
	return &Index{index}, nil
}

func (column *Column) RemoveIndex(name string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	if !C.grnxx_column_remove_index(column.handle, nameCString) {
		return fmt.Errorf("grnxx_column_remove_index() failed")
	}
	return nil
}

func (column *Column) RenameIndex(name string, newName string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	newNameCString := C.CString(newName)
	defer C.free(unsafe.Pointer(newNameCString))
	if !C.grnxx_column_rename_index(column.handle, nameCString, newNameCString) {
		return fmt.Errorf("grnxx_column_rename_index() failed")
	}
	return nil
}

func (column *Column) ReorderIndex(name string, prevName string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	prevNameCString := C.CString(prevName)
	defer C.free(unsafe.Pointer(prevNameCString))
	if !C.grnxx_column_reorder_index(column.handle, nameCString, prevNameCString) {
		return fmt.Errorf("grnxx_column_reorder_index() failed")
	}
	return nil
}

func (column *Column) GetIndex(indexID int) *Index {
	return &Index{C.grnxx_column_get_index(column.handle, C.size_t(indexID))}
}

func (column *Column) FindIndex(name string) *Index {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	index := C.grnxx_column_find_index(column.handle, nameCString)
	if index == nil {
		return nil
	}
	return &Index{index}
}

func (column *Column) Set(rowID Int, value Valuer) error {
	switch x := value.(type) {
	case nil:
		if !C.grnxx_column_set(column.handle, C.int64_t(rowID), nil) {
			return fmt.Errorf("grnxx_column_set() failed")
		}
	case Bool:
		if !C.grnxx_column_set(column.handle, C.int64_t(rowID), unsafe.Pointer(&x)) {
			return fmt.Errorf("grnxx_column_set() failed")
		}
	case Int:
		if !C.grnxx_column_set(column.handle, C.int64_t(rowID), unsafe.Pointer(&x)) {
			return fmt.Errorf("grnxx_column_set() failed")
		}
	case Float:
		if !C.grnxx_column_set(column.handle, C.int64_t(rowID), unsafe.Pointer(&x)) {
			return fmt.Errorf("grnxx_column_set() failed")
		}
	case GeoPoint:
		if !C.grnxx_column_set(column.handle, C.int64_t(rowID), unsafe.Pointer(&x)) {
			return fmt.Errorf("grnxx_column_set() failed")
		}
	case Text:
		text := x.Convert()
		if !C.grnxx_column_set(column.handle, C.int64_t(rowID), unsafe.Pointer(&text)) {
			return fmt.Errorf("grnxx_column_set() failed")
		}
	default:
		return fmt.Errorf("undefined type")
	}
	return nil
}

func (column *Column) Get(rowID Int, value interface{}) error {
	switch p := value.(type) {
	case *Bool:
		if !C.grnxx_column_get(column.handle, C.int64_t(rowID), unsafe.Pointer(p)) {
			return fmt.Errorf("grnxx_column_get() failed")
		}
	case *Int:
		if !C.grnxx_column_get(column.handle, C.int64_t(rowID), unsafe.Pointer(p)) {
			return fmt.Errorf("grnxx_column_get() failed")
		}
	case *Float:
		if !C.grnxx_column_get(column.handle, C.int64_t(rowID), unsafe.Pointer(p)) {
			return fmt.Errorf("grnxx_column_get() failed")
		}
	case *GeoPoint:
		if !C.grnxx_column_get(column.handle, C.int64_t(rowID), unsafe.Pointer(p)) {
			return fmt.Errorf("grnxx_column_get() failed")
		}
	case *Text:
		var text C.grnxx_text
		if !C.grnxx_column_get(column.handle, C.int64_t(rowID), unsafe.Pointer(&text)) {
			return fmt.Errorf("grnxx_column_get() failed")
		}
		if Int(text.size).IsNA() {
			*p = nil
		} else {
			*p = C.GoBytes(unsafe.Pointer(text.data), C.int(text.size))
		}
	default:
		return fmt.Errorf("undefined type")
	}
	return nil
}

func (column *Column) Contains(value Valuer) bool {
	switch x := value.(type) {
	case nil:
		return bool(C.grnxx_column_contains(column.handle, nil))
	case Bool:
		return bool(C.grnxx_column_contains(column.handle, unsafe.Pointer(&x)))
	case Int:
		return bool(C.grnxx_column_contains(column.handle, unsafe.Pointer(&x)))
	case Float:
		return bool(C.grnxx_column_contains(column.handle, unsafe.Pointer(&x)))
	case GeoPoint:
		return bool(C.grnxx_column_contains(column.handle, unsafe.Pointer(&x)))
	case Text:
		text := x.Convert()
		return bool(C.grnxx_column_contains(column.handle, unsafe.Pointer(&text)))
	}
	return false
}

func (column *Column) FindOne(value Valuer) Int {
	switch x := value.(type) {
	case nil:
		return Int(C.grnxx_column_find_one(column.handle, nil))
	case Bool:
		return Int(C.grnxx_column_find_one(column.handle, unsafe.Pointer(&x)))
	case Int:
		return Int(C.grnxx_column_find_one(column.handle, unsafe.Pointer(&x)))
	case Float:
		return Int(C.grnxx_column_find_one(column.handle, unsafe.Pointer(&x)))
	case GeoPoint:
		return Int(C.grnxx_column_find_one(column.handle, unsafe.Pointer(&x)))
	case Text:
		text := x.Convert()
		return Int(C.grnxx_column_find_one(column.handle, unsafe.Pointer(&text)))
	}
	return NAInt()
}

// -- Index --

type Index struct {
	handle *C.grnxx_index
}

func (index *Index) Column() *Column {
	return &Column{C.grnxx_index_column(index.handle)}
}

func (index *Index) Name() string {
	var size C.size_t
	name := C.grnxx_index_name(index.handle, &size)
	return C.GoStringN(name, C.int(size))
}

func (index *Index) IndexType() IndexType {
	return IndexType(C.grnxx_index_index_type(index.handle))
}

func (index *Index) NumEntries() int {
	return int(C.grnxx_index_num_entries(index.handle))
}

func (index *Index) TestUniqueness() bool {
	return bool(C.grnxx_index_test_uniqueness(index.handle))
}

func (index *Index) Contains(value Valuer) bool {
	switch x := value.(type) {
	case nil:
		return bool(C.grnxx_index_contains(index.handle, nil))
	case Bool:
		return bool(C.grnxx_index_contains(index.handle, unsafe.Pointer(&x)))
	case Int:
		return bool(C.grnxx_index_contains(index.handle, unsafe.Pointer(&x)))
	case Float:
		return bool(C.grnxx_index_contains(index.handle, unsafe.Pointer(&x)))
	case GeoPoint:
		return bool(C.grnxx_index_contains(index.handle, unsafe.Pointer(&x)))
	case Text:
		text := x.Convert()
		return bool(C.grnxx_index_contains(index.handle, unsafe.Pointer(&text)))
	}
	return false
}

func (index *Index) FindOne(value Valuer) Int {
	switch x := value.(type) {
	case nil:
		return Int(C.grnxx_index_find_one(index.handle, nil))
	case Bool:
		return Int(C.grnxx_index_find_one(index.handle, unsafe.Pointer(&x)))
	case Int:
		return Int(C.grnxx_index_find_one(index.handle, unsafe.Pointer(&x)))
	case Float:
		return Int(C.grnxx_index_find_one(index.handle, unsafe.Pointer(&x)))
	case GeoPoint:
		return Int(C.grnxx_index_find_one(index.handle, unsafe.Pointer(&x)))
	case Text:
		text := x.Convert()
		return Int(C.grnxx_index_find_one(index.handle, unsafe.Pointer(&text)))
	}
	return NAInt()
}

func (index *Index) Find(value Valuer, options *CursorOptions) (*Cursor, error) {
	cursorOptions := convertCursorOptions(options)
	var cursor *C.grnxx_cursor
	switch x := value.(type) {
	case nil:
		cursor = C.grnxx_index_find(index.handle, nil, cursorOptions)
	case Bool:
		cursor = C.grnxx_index_find(index.handle, unsafe.Pointer(&x), cursorOptions)
	case Int:
		cursor = C.grnxx_index_find(index.handle, unsafe.Pointer(&x), cursorOptions)
	case Float:
		cursor = C.grnxx_index_find(index.handle, unsafe.Pointer(&x), cursorOptions)
	case GeoPoint:
		cursor = C.grnxx_index_find(index.handle, unsafe.Pointer(&x), cursorOptions)
	case Text:
		text := x.Convert()
		cursor = C.grnxx_index_find(index.handle, unsafe.Pointer(&text), cursorOptions)
	}
	if cursor == nil {
		return nil, fmt.Errorf("grnxx_index_find() failed")
	}
	return &Cursor{cursor}, nil
}

func (index *Index) FindInRange(
	lowerBound Valuer, lowerBoundInclusive bool,
	upperBound Valuer, upperBoundInclusive bool,
	options *CursorOptions) (*Cursor, error) {
	cursorOptions := convertCursorOptions(options)
	var lowerBoundPointer unsafe.Pointer
	switch x := lowerBound.(type) {
	case nil:
		lowerBoundPointer = nil
	case Bool:
		lowerBoundPointer = unsafe.Pointer(&x)
	case Int:
		lowerBoundPointer = unsafe.Pointer(&x)
	case Float:
		lowerBoundPointer = unsafe.Pointer(&x)
	case GeoPoint:
		lowerBoundPointer = unsafe.Pointer(&x)
	case Text:
		text := x.Convert()
		lowerBoundPointer = unsafe.Pointer(&text)
	}
	var upperBoundPointer unsafe.Pointer
	switch x := upperBound.(type) {
	case nil:
		upperBoundPointer = nil
	case Bool:
		upperBoundPointer = unsafe.Pointer(&x)
	case Int:
		upperBoundPointer = unsafe.Pointer(&x)
	case Float:
		upperBoundPointer = unsafe.Pointer(&x)
	case GeoPoint:
		upperBoundPointer = unsafe.Pointer(&x)
	case Text:
		text := x.Convert()
		upperBoundPointer = unsafe.Pointer(&text)
	}
	cursor := C.grnxx_index_find_in_range(
		index.handle, lowerBoundPointer, C.bool(lowerBoundInclusive),
		upperBoundPointer, C.bool(upperBoundInclusive), cursorOptions)
	if cursor == nil {
		return nil, fmt.Errorf("grnxx_index_find_in_range() failed")
	}
	return &Cursor{cursor}, nil
}

func (index *Index) FindStartsWith(
	value Valuer, inclusive bool, options *CursorOptions) (*Cursor, error) {
	cursorOptions := convertCursorOptions(options)
	var cursor *C.grnxx_cursor
	switch x := value.(type) {
	case nil:
		cursor = C.grnxx_index_find_starts_with(
			index.handle, nil, C.bool(inclusive), cursorOptions)
	case Bool:
		cursor = C.grnxx_index_find_starts_with(
			index.handle, unsafe.Pointer(&x), C.bool(inclusive), cursorOptions)
	case Int:
		cursor = C.grnxx_index_find_starts_with(
			index.handle, unsafe.Pointer(&x), C.bool(inclusive), cursorOptions)
	case Float:
		cursor = C.grnxx_index_find_starts_with(
			index.handle, unsafe.Pointer(&x), C.bool(inclusive), cursorOptions)
	case GeoPoint:
		cursor = C.grnxx_index_find_starts_with(
			index.handle, unsafe.Pointer(&x), C.bool(inclusive), cursorOptions)
	case Text:
		text := x.Convert()
		cursor = C.grnxx_index_find_starts_with(
			index.handle, unsafe.Pointer(&text), C.bool(inclusive), cursorOptions)
	}
	if cursor == nil {
		return nil, fmt.Errorf("grnxx_index_find_starts_with() failed")
	}
	return &Cursor{cursor}, nil
}

func (index *Index) FindPrefixes(
	value Valuer, options *CursorOptions) (*Cursor, error) {
	cursorOptions := convertCursorOptions(options)
	var cursor *C.grnxx_cursor
	switch x := value.(type) {
	case nil:
		cursor = C.grnxx_index_find_prefixes(
			index.handle, nil, cursorOptions)
	case Bool:
		cursor = C.grnxx_index_find_prefixes(
			index.handle, unsafe.Pointer(&x), cursorOptions)
	case Int:
		cursor = C.grnxx_index_find_prefixes(
			index.handle, unsafe.Pointer(&x), cursorOptions)
	case Float:
		cursor = C.grnxx_index_find_prefixes(
			index.handle, unsafe.Pointer(&x), cursorOptions)
	case GeoPoint:
		cursor = C.grnxx_index_find_prefixes(
			index.handle, unsafe.Pointer(&x), cursorOptions)
	case Text:
		text := x.Convert()
		cursor = C.grnxx_index_find_prefixes(
			index.handle, unsafe.Pointer(&text), cursorOptions)
	}
	if cursor == nil {
		return nil, fmt.Errorf("grnxx_index_find_prefixes() failed")
	}
	return &Cursor{cursor}, nil
}

// -- Cursor --

// TODO: CursorOptions should be generated with NewCursorOptions().
type CursorOptions struct {
	Offset    int
	Limit     int
	OrderType OrderType
}

func convertCursorOptions(options *CursorOptions) *C.grnxx_cursor_options {
	var cursorOptions C.grnxx_cursor_options
	if options == nil {
		cursorOptions.offset = 0
		cursorOptions.limit = math.MaxInt32
		cursorOptions.order_type = C.GRNXX_REGULAR_ORDER
	} else {
		cursorOptions.offset = C.size_t(options.Offset)
		cursorOptions.limit = C.size_t(options.Limit)
		cursorOptions.order_type = C.grnxx_order_type(options.OrderType)
	}
	return &cursorOptions
}

type Cursor struct {
	handle *C.grnxx_cursor
}

func (cursor *Cursor) Close() error {
	C.grnxx_cursor_close(cursor.handle)
	return nil
}

func (cursor *Cursor) Read(records []Record) int {
	if len(records) == 0 {
		return 0
	}
	return int(C.grnxx_cursor_read(cursor.handle,
		(*C.grnxx_record)(unsafe.Pointer(&records[0])), C.size_t(len(records))))
}

// -- Expression --

type Expression struct {
	handle *C.grnxx_expression
}

func ParseExpression(table *Table, query string) (*Expression, error) {
	queryCString := C.CString(query)
	defer C.free(unsafe.Pointer(queryCString))
	expression := C.grnxx_expression_parse(table.handle, queryCString)
	if expression == nil {
		return nil, fmt.Errorf("grnxx_expression_parse() failed")
	}
	return &Expression{expression}, nil
}

func (this *Expression) Close() {
	C.grnxx_expression_close(this.handle)
}

func (this *Expression) Table() *Table {
	return &Table{C.grnxx_expression_table(this.handle)}
}

func (this *Expression) DataType() DataType {
	return DataType(C.grnxx_expression_data_type(this.handle))
}

func (this *Expression) IsRowID() bool {
	return bool(C.grnxx_expression_is_row_id(this.handle))
}

func (this *Expression) IsScore() bool {
	return bool(C.grnxx_expression_is_score(this.handle))
}

func (this *Expression) BlockSize() int {
	return int(C.grnxx_expression_block_size(this.handle))
}

func (this *Expression) Filter(records *[]Record) error {
	if len(*records) == 0 {
		return nil
	}
	internalRecords := (*C.grnxx_record)(unsafe.Pointer(&(*records)[0]))
	size := C.size_t(len(*records))
	if !C.grnxx_expression_filter(this.handle, internalRecords,
		&size, C.size_t(0), C.size_t(math.MaxInt32)) {
		return fmt.Errorf("grnxx_expression_filter() failed")
	}
	*records = (*records)[:int(size)]
	return nil
}

func (this *Expression) FilterEx(
	records *[]Record, offset int, limit int) error {
	if len(*records) == 0 {
		return nil
	}
	internalRecords := (*C.grnxx_record)(unsafe.Pointer(&(*records)[0]))
	size := C.size_t(len(*records))
	if !C.grnxx_expression_filter(this.handle, internalRecords,
		&size, C.size_t(offset), C.size_t(limit)) {
		return fmt.Errorf("grnxx_expression_filter() failed")
	}
	*records = (*records)[:int(size)]
	return nil
}

func (this *Expression) Adjust(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	internalRecords := (*C.grnxx_record)(unsafe.Pointer(&records[0]))
	size := C.size_t(len(records))
	if !C.grnxx_expression_adjust(this.handle, internalRecords, size) {
		return fmt.Errorf("grnxx_expression_adjust() failed")
	}
	return nil
}

func (this *Expression) Evaluate(records []Record, values interface{}) error {
	if len(records) == 0 {
		return nil
	}
	internalRecords := (*C.grnxx_record)(unsafe.Pointer(&records[0]))
	size := C.size_t(len(records))
	var result C.bool
	switch x := values.(type) {
	case []Bool:
		result = C.grnxx_expression_evaluate(
			this.handle, internalRecords, size, unsafe.Pointer(&x[0]))
	case []Int:
		result = C.grnxx_expression_evaluate(
			this.handle, internalRecords, size, unsafe.Pointer(&x[0]))
	case []Float:
		result = C.grnxx_expression_evaluate(
			this.handle, internalRecords, size, unsafe.Pointer(&x[0]))
	case []GeoPoint:
		result = C.grnxx_expression_evaluate(
			this.handle, internalRecords, size, unsafe.Pointer(&x[0]))
	case []Text:
		internalValues := make([]C.grnxx_text, len(records))
		result = C.grnxx_expression_evaluate(
			this.handle, internalRecords, size, unsafe.Pointer(&internalValues[0]))
		for i := 0; i < len(records); i++ {
			// TODO: Deep-copy should not be done?
			x[i] = C.GoBytes(unsafe.Pointer(internalValues[i].data),
				C.int(internalValues[i].size))
		}
	default:
		return fmt.Errorf("unsupported data type")
	}
	if !result {
		return fmt.Errorf("grnxx_expression_evaluate() failed")
	}
	return nil
}

// -- ExpressionBuilder --

type ExpressionBuilder struct {
	handle *C.grnxx_expression_builder
}

func CreateExpressionBuilder(table *Table) (*ExpressionBuilder, error) {
	builder := C.grnxx_expression_builder_create(table.handle)
	if builder == nil {
		return nil, fmt.Errorf("grnxx_expression_builder_create() failed")
	}
	return &ExpressionBuilder{builder}, nil
}

func (this *ExpressionBuilder) Close() {
	C.grnxx_expression_builder_close(this.handle)
}

func (this *ExpressionBuilder) Table() *Table {
	return &Table{C.grnxx_expression_builder_table(this.handle)}
}

func (this *ExpressionBuilder) PushConstant(value Valuer) error {
	var result C.bool
	switch x := value.(type) {
	case Bool:
		result = C.grnxx_expression_builder_push_constant(
			this.handle, C.GRNXX_BOOL, unsafe.Pointer(&x))
	case Int:
		result = C.grnxx_expression_builder_push_constant(
			this.handle, C.GRNXX_INT, unsafe.Pointer(&x))
	case Float:
		result = C.grnxx_expression_builder_push_constant(
			this.handle, C.GRNXX_FLOAT, unsafe.Pointer(&x))
	case GeoPoint:
		result = C.grnxx_expression_builder_push_constant(
			this.handle, C.GRNXX_GEO_POINT, unsafe.Pointer(&x))
	case Text:
		text := x.Convert()
		result = C.grnxx_expression_builder_push_constant(
			this.handle, C.GRNXX_TEXT, unsafe.Pointer(&text))
	default:
		return fmt.Errorf("unsupported data type")
	}
	if !result {
		return fmt.Errorf("grnxx_expression_builder_push_constant() failed")
	}
	return nil
}

func (this *ExpressionBuilder) PushRowID() error {
	if !C.grnxx_expression_builder_push_row_id(this.handle) {
		fmt.Errorf("grnxx_expression_builder_push_row_id() failed")
	}
	return nil
}

func (this *ExpressionBuilder) PushScore() error {
	if !C.grnxx_expression_builder_push_score(this.handle) {
		fmt.Errorf("grnxx_expression_builder_push_score() failed")
	}
	return nil
}

func (this *ExpressionBuilder) PushColumn(name string) error {
	nameCString := C.CString(name)
	defer C.free(unsafe.Pointer(nameCString))
	if !C.grnxx_expression_builder_push_column(this.handle, nameCString) {
		fmt.Errorf("grnxx_expression_builder_push_column() failed")
	}
	return nil
}

func (this *ExpressionBuilder) PushOperator(operatorType OperatorType) error {
	if !C.grnxx_expression_builder_push_operator(
		this.handle, C.grnxx_operator_type(operatorType)) {
		fmt.Errorf("grnxx_expression_builder_push_operator() failed")
	}
	return nil
}

func (this *ExpressionBuilder) BeginSubexpression() error {
	if !C.grnxx_expression_builder_begin_subexpression(this.handle) {
		return fmt.Errorf("grnxx_expression_builder_begin_subexpression() failed")
	}
	return nil
}

func (this *ExpressionBuilder) EndSubexpression() error {
	if !C.grnxx_expression_builder_end_subexpression(this.handle) {
		return fmt.Errorf("grnxx_expression_builder_end_subexpression() failed")
	}
	return nil
}

func (this *ExpressionBuilder) Clear() {
	C.grnxx_expression_builder_clear(this.handle)
}

func (this *ExpressionBuilder) Release() (*Expression, error) {
	expression := C.grnxx_expression_builder_release(this.handle)
	if expression == nil {
		return nil, fmt.Errorf("grnxx_expression_builder_release() failed")
	}
	return &Expression{expression}, nil
}

// -- Sorter --

type SorterOrder struct {
	Expression *Expression
	OrderType  OrderType
}

func convertSorterOrders(orders []SorterOrder) []C.grnxx_sorter_order {
	internalOrders := make([]C.grnxx_sorter_order, len(orders))
	for i := 0; i < len(orders); i++ {
		internalOrders[i].expression = orders[i].Expression.handle
		internalOrders[i].order_type = C.grnxx_order_type(orders[i].OrderType)
	}
	return internalOrders
}

type SorterOptions struct {
	Offset int
	Limit  int
}

func convertSorterOptions(options *SorterOptions) *C.grnxx_sorter_options {
	if options == nil {
		return nil
	}
	var internalOptions C.grnxx_sorter_options
	internalOptions.offset = C.size_t(options.Offset)
	internalOptions.limit = C.size_t(options.Limit)
	return &internalOptions
}

type Sorter struct {
	handle *C.grnxx_sorter
}

func CreateSorter(orders []SorterOrder, options *SorterOptions) (*Sorter, error) {
	if len(orders) == 0 {
		return nil, fmt.Errorf("no orders")
	}
	internalOrders := convertSorterOrders(orders)
	sorter := C.grnxx_sorter_create(
		&internalOrders[0], C.size_t(len(orders)), convertSorterOptions(options))
	if sorter == nil {
		return nil, fmt.Errorf("grnxx_sorter_create() failed")
	}
	return &Sorter{sorter}, nil
}

func (this *Sorter) Close() error {
	C.grnxx_sorter_close(this.handle)
	return nil
}

// -- Merger --

type MergerOptions struct {
	LogicalOperatorType MergerOperatorType
	ScoreOperatorType   MergerOperatorType
	MissingScore        Float
	Offset              int
	Limit               int
}

func convertMergerOptions(options *MergerOptions) *C.grnxx_merger_options {
	if options == nil {
		return nil
	}
	var internalOptions C.grnxx_merger_options
	internalOptions.logical_operator_type =
		C.grnxx_merger_operator_type(options.LogicalOperatorType)
	internalOptions.score_operator_type =
		C.grnxx_merger_operator_type(options.ScoreOperatorType)
	internalOptions.missing_score = C.double(options.MissingScore)
	internalOptions.offset = C.size_t(options.Offset)
	internalOptions.limit = C.size_t(options.Limit)
	return &internalOptions
}

type Merger struct {
	handle *C.grnxx_merger
}

func CreateMerger(options *MergerOptions) (*Merger, error) {
	merger := C.grnxx_merger_create(convertMergerOptions(options))
	if merger == nil {
		return nil, fmt.Errorf("grnxx_merger_create() failed")
	}
	return &Merger{merger}, nil
}

func (this *Merger) Close() error {
	C.grnxx_merger_close(this.handle)
	return nil
}

// -- Pipeline --

type Pipeline struct {
	handle *C.grnxx_pipeline
}

func (this *Pipeline) Close() error {
	C.grnxx_pipeline_close(this.handle)
	return nil
}

func (this *Pipeline) Table() *Table {
	return &Table{C.grnxx_pipeline_table(this.handle)}
}

func (this *Pipeline) Flush() ([]Record, error) {
	var ptr *C.grnxx_record
	var size C.size_t
	if !C.grnxx_pipeline_flush(this.handle, &ptr, &size) {
		return nil, fmt.Errorf("grnxx_pipeline_flush() failed")
	}
	defer C.free(unsafe.Pointer(ptr))
	header := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(ptr)),
		Len:  int(size),
		Cap:  int(size),
	}
	internalRecords := *(*[]C.grnxx_record)(unsafe.Pointer(&header))
	records := make([]Record, int(size))
	for i := 0; i < len(records); i++ {
		records[i].RowID = Int(internalRecords[i].row_id)
		records[i].Score = Float(internalRecords[i].score)
	}
	return records, nil
}

// -- PipelineBuilder --

type PipelineOptions struct {
}

func convertPipelineOptions(options *PipelineOptions) *C.grnxx_pipeline_options {
	return nil
}

type PipelineBuilder struct {
	handle *C.grnxx_pipeline_builder
}

func CreatePipelineBuilder(table *Table) (*PipelineBuilder, error) {
	builder := C.grnxx_pipeline_builder_create(table.handle)
	if builder == nil {
		return nil, fmt.Errorf("grnxx_pipeline_builder_create() failed")
	}
	return &PipelineBuilder{builder}, nil
}

func (this *PipelineBuilder) Close() error {
	C.grnxx_pipeline_builder_close(this.handle)
	return nil
}

func (this *PipelineBuilder) Table() *Table {
	return &Table{C.grnxx_pipeline_builder_table(this.handle)}
}

func (this *PipelineBuilder) PushCursor(cursor *Cursor) error {
	if !C.grnxx_pipeline_builder_push_cursor(this.handle, cursor.handle) {
		return fmt.Errorf("grnxx_pipeline_builder_push_cursor() failed")
	}
	return nil
}

func (this *PipelineBuilder) PushFilter(
	expression *Expression, offset int, limit int) error {
	if !C.grnxx_pipeline_builder_push_filter(
		this.handle, expression.handle, C.size_t(offset), C.size_t(limit)) {
		return fmt.Errorf("grnxx_pipeline_builder_push_filter() failed")
	}
	return nil
}

func (this *PipelineBuilder) PushAdjuster(expression *Expression) error {
	if !C.grnxx_pipeline_builder_push_adjuster(this.handle, expression.handle) {
		return fmt.Errorf("grnxx_pipeline_builder_push_adjuster() failed")
	}
	return nil
}

func (this *PipelineBuilder) PushSorter(sorter *Sorter) error {
	if !C.grnxx_pipeline_builder_push_sorter(this.handle, sorter.handle) {
		return fmt.Errorf("grnxx_pipeline_builder_push_sorter() failed")
	}
	return nil
}

func (this *PipelineBuilder) PushMerger(options *MergerOptions) error {
	internalOptions := convertMergerOptions(options)
	if !C.grnxx_pipeline_builder_push_merger(this.handle, internalOptions) {
		return fmt.Errorf("grnxx_pipeline_builder_push_merger() failed")
	}
	return nil
}

func (this *PipelineBuilder) Clear() error {
	C.grnxx_pipeline_builder_clear(this.handle)
	return nil
}

func (this *PipelineBuilder) Release(options *PipelineOptions) (*Pipeline, error) {
	internalOptions := convertPipelineOptions(options)
	pipeline := C.grnxx_pipeline_builder_release(this.handle, internalOptions)
	if pipeline == nil {
		return nil, fmt.Errorf("grnxx_pipeline_builder_release() failed")
	}
	return &Pipeline{pipeline}, nil
}
