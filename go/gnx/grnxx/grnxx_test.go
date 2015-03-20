package grnxx

import "reflect"
import "testing"

func TestPackage(t *testing.T) {
	if Package() != "grnxx" {
		t.Fatalf("Package name is wrong")
	}
}

func TestVersion(t *testing.T) {
	if Version() == "" {
		t.Fatalf("Version is not available")
	}
}

func TestEmptyDB(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	if db == nil {
		t.Fatalf("CreateDB() returned nil")
	}
	numTables := db.NumTables()
	if numTables != 0 {
		t.Fatalf("Empty DB has tables: numTables = %v", numTables)
	}
	error = db.Close()
	if error != nil {
		t.Fatalf("DB.Close() failed: error = %v", error)
	}
}

func TestEmptyTable(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	numTables := db.NumTables()
	if numTables != 1 {
		t.Fatalf("DB.NumTables() returned a wrong value: numTables = %v",
			numTables)
	}
	if table.DB().handle != db.handle {
		t.Fatalf("Table.DB() returned a wrong DB")
	}
	name := table.Name()
	if name != "Table" {
		t.Fatalf("Table.Name() returned a wrong name: name = %v", name)
	}
	numColumns := table.NumColumns()
	if numColumns != 0 {
		t.Fatalf("Empty table has columns: numColumns = %v", numColumns)
	}
	keyColumn := table.KeyColumn()
	if keyColumn != nil {
		t.Fatalf("Empty column has key column")
	}
	numRows := table.NumRows()
	if numRows != 0 {
		t.Fatalf("Empty table has rows: numRows = %v", numRows)
	}
	maxRowID := table.MaxRowID()
	if !maxRowID.IsNA() {
		t.Fatalf("Empty table has valid max. row ID: maxRowID = %v", maxRowID)
	}
	if !table.IsEmpty() {
		t.Fatalf("Empty table's IsEmpty() returned false")
	}
	if !table.IsFull() {
		t.Fatalf("Empty table's IsFull() returned false")
	}
	error = db.RemoveTable("Table")
	if error != nil {
		t.Fatalf("DB.RemoveTable() failed: error = %v", error)
	}
}

func TestTableRename(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	error = db.RenameTable("Table", "FirstTable")
	if error != nil {
		t.Fatalf("DB.RenameTable() failed: error = %v", error)
	}
	name := table.Name()
	if name != "FirstTable" {
		t.Fatalf("Table name is wrong: name = %v", name)
	}
	table2, error := db.CreateTable("FirstTable")
	if error == nil {
		t.Fatalf("DB.CreateTable() could not detect name collision")
	}
	table2, error = db.CreateTable("SecondTable")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	name = table2.Name()
	if name != "SecondTable" {
		t.Fatalf("Table name is wrong: name = %v", name)
	}
	error = db.RenameTable("FirstTable", "SecondTable")
	if error == nil {
		t.Fatalf("DB.RenameTable() could not detect name collision")
	}
	error = db.RenameTable("FirstTable", "Table")
	if error != nil {
		t.Fatalf("DB.RenameTable() failed: error = %v", error)
	}
}

func TestTableReordering(t *testing.T) {
	// TODO
}

func TestEmptyColumn(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	numColumns := table.NumColumns()
	if numColumns != 1 {
		t.Fatalf("Table.NumColumns() returned a wrong value: numColumns = %v",
			numColumns)
	}
	if column.Table().handle != table.handle {
		t.Fatalf("Column.Table() returned a wrong table")
	}
	name := column.Name()
	if name != "Column" {
		t.Fatalf("Column.Name() returned a wrong name: name = %v", name)
	}
	dataType := column.DataType()
	if dataType != INT {
		t.Fatalf("Column.DataType() returned a wrong data type: dataType = %v",
			dataType)
	}
	referenceTable := column.ReferenceTable()
	if referenceTable != nil {
		t.Fatalf("Empty column has a reference table: referenceTable.Name() = %v",
			referenceTable.Name())
	}
	if column.IsKey() {
		t.Fatalf("Empty column is key")
	}
	numIndexes := column.NumIndexes()
	if numIndexes != 0 {
		t.Fatalf("Empty column has indexes: numIndexes = %v", numIndexes)
	}
	table.RemoveColumn("Column")
	if error != nil {
		t.Fatalf("Table.RemoveColumn() failed: error = %v", error)
	}
}

func TestColumnRename(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	error = table.RenameColumn("Column", "FirstColumn")
	if error != nil {
		t.Fatalf("Table.RenameColumn() failed: error = %v", error)
	}
	name := column.Name()
	if name != "FirstColumn" {
		t.Fatalf("Column name is wrong: name = %v", name)
	}
	column2, error := table.CreateColumn("FirstColumn", INT, nil)
	if error == nil {
		t.Fatalf("Table.CreateColumn() could not detect name collision")
	}
	column2, error = table.CreateColumn("SecondColumn", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	name = column2.Name()
	if name != "SecondColumn" {
		t.Fatalf("Column name is wrong: name = %v", name)
	}
	error = table.RenameColumn("FirstColumn", "SecondColumn")
	if error == nil {
		t.Fatalf("Table.RenameColumn() could not detect name collision")
	}
	error = table.RenameColumn("FirstColumn", "Column")
	if error != nil {
		t.Fatalf("Table.RenameColumn() failed: error = %v", error)
	}
}

func TestColumnReordering(t *testing.T) {
	// TODO
}

func TestEmptyIndex(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	index, error := column.CreateIndex("Index", TREE_INDEX)
	if error != nil {
		t.Fatalf("Column.CreateIndex() failed: error = %v", error)
	}
	numIndexes := column.NumIndexes()
	if numIndexes != 1 {
		t.Fatalf("Column.NumIndexes() returned a wrong value: numIndexes = %v",
			numIndexes)
	}
	if index.Column().handle != column.handle {
		t.Fatalf("Index.Column() returned a wrong column")
	}
	name := index.Name()
	if name != "Index" {
		t.Fatalf("Index.Name() returned a wrong name: name = %v", name)
	}
	indexType := index.IndexType()
	if indexType != TREE_INDEX {
		t.Fatalf("Index.IndexType() returned a wrong index type: indexType = %v",
			indexType)
	}
	numEntries := index.NumEntries()
	if numEntries != 0 {
		t.Fatalf("Empty index has entries: numEntries = %v", numEntries)
	}
	if !index.TestUniqueness() {
		t.Fatalf("Empty index's TestUniqueness() returned true")
	}
	column.RemoveIndex("Index")
	if error != nil {
		t.Fatalf("Column.RemoveIndex() failed: error = %v", error)
	}
}

func TestIndexRename(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	index, error := column.CreateIndex("Index", TREE_INDEX)
	if error != nil {
		t.Fatalf("Column.CreateIndex() failed: error = %v", error)
	}

	error = column.RenameIndex("Index", "FirstIndex")
	if error != nil {
		t.Fatalf("Table.RenameIndex() failed: error = %v", error)
	}
	name := index.Name()
	if name != "FirstIndex" {
		t.Fatalf("Index name is wrong: name = %v", name)
	}
	index2, error := column.CreateIndex("FirstIndex", TREE_INDEX)
	if error == nil {
		t.Fatalf("Table.CreateIndex() could not detect name collision")
	}
	index2, error = column.CreateIndex("SecondIndex", TREE_INDEX)
	if error != nil {
		t.Fatalf("Table.CreateIndex() failed: error = %v", error)
	}
	name = index2.Name()
	if name != "SecondIndex" {
		t.Fatalf("Index name is wrong: name = %v", name)
	}
	error = column.RenameIndex("FirstIndex", "SecondIndex")
	if error == nil {
		t.Fatalf("Table.RenameIndex() could not detect name collision")
	}
	error = column.RenameIndex("FirstIndex", "Index")
	if error != nil {
		t.Fatalf("Table.RenameIndex() failed: error = %v", error)
	}
}

func TestIndexReordering(t *testing.T) {
	// TODO
}

func TestInsertRow(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	if rowID != 0 {
		t.Fatalf("Table.InsertRow() returned a wrong value: rowID = %v", rowID)
	}
	numRows := table.NumRows()
	if numRows != 1 {
		t.Fatalf("Table.NumRows() returned a wrong value: numRows = %v", numRows)
	}
	maxRowID := table.MaxRowID()
	if maxRowID != rowID {
		t.Fatalf("Table.MaxRowID() returned a wrong value: maxRowID = %v", maxRowID)
	}
	if table.IsEmpty() {
		t.Fatalf("Non-empty table's IsEmpty() returned true")
	}
	if !table.IsFull() {
		t.Fatalf("Full table's IsFull() returned false")
	}
	rowID, error = table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	if rowID != 1 {
		t.Fatalf("Table.InsertRow() returned a wrong value: rowID = %v", rowID)
	}
	error = db.RemoveTable("Table")
	if error != nil {
		t.Fatalf("DB.RemoveTable() failed: error = %v", error)
	}

	table, error = db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", TEXT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	error = table.SetKeyColumn("Column")
	if error != nil {
		t.Fatalf("Table.SetKeyColumn() failed: error = %v", error)
	}
	rowID, error = table.InsertRow(Text("Hello"))
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	if rowID != 0 {
		t.Fatalf("Table.InsertRow() returned a wrong value: rowID = %v", rowID)
	}
	var value Text
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !reflect.DeepEqual(value, Text("Hello")) {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	rowID = table.FindRow(Text("Hello"))
	if rowID != 0 {
		t.Fatalf("Table.FindRow() returned a wrong value: rowID = %v", rowID)
	}
}

func TestTableInsertRows(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()

	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	rowIDs, error := table.InsertRows(make([]Int, 3))
	if error != nil {
		t.Fatalf("Table.InsertRows() failed: error = %v", error)
	}
	if len(rowIDs) != 3 {
		t.Fatalf("Table.InsertRows() failed: len(rowIDs) = %v", len(rowIDs))
	}
	for i := 0; i < 3; i++ {
		if rowIDs[i] != Int(i) {
			t.Fatalf("Table.InsertRows() failed: i = %v, rowIDs[i] = %v",
				i, rowIDs[i])
		}
	}
	error = db.RemoveTable("Table")
	if error != nil {
		t.Fatalf("DB.RemoveTable() failed: error = %v", error)
	}

	table, error = db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", TEXT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	error = table.SetKeyColumn("Column")
	if error != nil {
		t.Fatalf("Table.SetKeyColumn() failed: error = %v", error)
	}
	keys := []Text{Text("Hello"), Text("World"), Text("!")}
	rowIDs, error = table.InsertRows(keys)
	if error != nil {
		t.Fatalf("Table.InsertRows() failed: error = %v", error)
	}
	if len(rowIDs) != 3 {
		t.Fatalf("Table.InsertRows() failed: len(rowIDs) = %v", len(rowIDs))
	}
	for i := 0; i < 3; i++ {
		if rowIDs[i] != Int(i) {
			t.Fatalf("Table.InsertRows() failed: i = %v, rowIDs[i] = %v",
				i, rowIDs[i])
		}
		var value Text
		error = column.Get(rowIDs[i], &value)
		if error != nil {
			t.Fatalf("Column.Get() failed: error = %v", error)
		}
		if !reflect.DeepEqual(value, keys[i]) {
			t.Fatalf("Column.Get() returned a wrong value: "+
				"i = %v, key = %v, value = %v", i, keys[i], value)
		}
	}
}

func TestTableInsertRowAt(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	error = table.InsertRowAt(Int(100), nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	numRows := table.NumRows()
	if numRows != 1 {
		t.Fatalf("Table.NumRows() returned a wrong value: numRows = %v", numRows)
	}
	maxRowID := table.MaxRowID()
	if maxRowID != 100 {
		t.Fatalf("Table.MaxRowID() returned a wrong value: maxRowID = %v", maxRowID)
	}
	if table.IsEmpty() {
		t.Fatalf("Non-empty table's IsEmpty() returned true")
	}
	if table.IsFull() {
		t.Fatalf("Non-full table's IsFull() returned true")
	}
	if table.TestRow(0) {
		t.Fatalf("Table.TestRow() returned true for an invalid row ID")
	}
	if !table.TestRow(100) {
		t.Fatalf("Table.TestRow() returned false for a valid row ID")
	}
}

func TestTableFindOrInsertRow(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	_, error = table.CreateColumn("Column", TEXT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	error = table.SetKeyColumn("Column")
	if error != nil {
		t.Fatalf("Table.SetKeyColumn() failed: error = %v", error)
	}
	rowID, inserted, error := table.FindOrInsertRow(Text("Hello"))
	if error != nil {
		t.Fatalf("Table.FindOrInsertRow() failed: error = %v", error)
	}
	if (rowID != 0) || !inserted {
		t.Fatalf("Table.FindOrInsertRow() returned a wrong value: "+
			"rowID = %v, inserted = %v", rowID, inserted)
	}
	rowID, inserted, error = table.FindOrInsertRow(Text("Hello"))
	if error != nil {
		t.Fatalf("Table.FindOrInsertRow() failed: error = %v", error)
	}
	if (rowID != 0) || inserted {
		t.Fatalf("Table.FindOrInsertRow() returned a wrong value: "+
			"rowID = %v, inserted = %v", rowID, inserted)
	}
	rowID, inserted, error = table.FindOrInsertRow(Text("World"))
	if error != nil {
		t.Fatalf("Table.FindOrInsertRow() failed: error = %v", error)
	}
	if (rowID != 1) || !inserted {
		t.Fatalf("Table.FindOrInsertRow() returned a wrong value: "+
			"rowID = %v, inserted = %v", rowID, inserted)
	}
}

func TestTableRemoveRow(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	error = table.RemoveRow(rowID)
	if error != nil {
		t.Fatalf("Table.RemoveRow() failed: error = %v", error)
	}
	numRows := table.NumRows()
	if numRows != 0 {
		t.Fatalf("Table.NumRows() returned a wrong value: numRows = %v", numRows)
	}
	maxRowID := table.MaxRowID()
	if !maxRowID.IsNA() {
		t.Fatalf("Table.MaxRowID() returned a wrong value: maxRowID = %v", maxRowID)
	}
	if !table.IsEmpty() {
		t.Fatalf("Empty table's IsEmpty() returned false")
	}
	if !table.IsFull() {
		t.Fatalf("Empty table's IsFull() returned false")
	}
}

func TestTableCreateCursor(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	rowIDs := make([]Int, 100)
	for i := 0; i < 100; i++ {
		rowIDs[i], error = table.InsertRow(nil)
		if error != nil {
			t.Fatalf("Table.InsertRow() failed: error = %v", error)
		}
	}
	cursor, error := table.CreateCursor(nil)
	if error != nil {
		t.Fatalf("Table.CreateCursor() failed: error = %v", error)
	}
	defer cursor.Close()
	records := make([]Record, 200)
	count := cursor.Read(records)
	if count != 100 {
		t.Fatalf("Cursor.Read() could not read all records: count = %v", count)
	}
	for i := 0; i < 100; i++ {
		if (rowIDs[i] != records[i].RowID) || (records[i].Score != 0.0) {
			t.Fatalf("Cursor.Read() returned a wrong record: RowID = %v, Score = %v",
				records[i].RowID, records[i].Score)
		}
	}
	cursorOptions := CursorOptions{25, 50, REVERSE_ORDER}
	cursor, error = table.CreateCursor(&cursorOptions)
	if error != nil {
		t.Fatalf("Table.CreateCursor() failed: error = %v", error)
	}
	defer cursor.Close()
	count = cursor.Read(records)
	if count != 50 {
		t.Fatalf("Cursor.Read() could not read all records: count = %v", count)
	}
	for i := 0; i < 50; i++ {
		j := 100 - 25 - i - 1
		if (rowIDs[j] != records[i].RowID) || (records[i].Score != 0.0) {
			t.Fatalf("Cursor.Read() returned a wrong record: RowID = %v, Score = %v",
				records[i].RowID, records[i].Score)
		}
	}
}

func TestBoolColumn(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", BOOL, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	var value Bool
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}

	VALUE := TRUE
	error = column.Set(rowID, VALUE)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if value != VALUE {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(VALUE) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID := column.FindOne(VALUE)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}

	error = column.Set(rowID, nil)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(nil) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID = column.FindOne(nil)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}
}

func TestIntColumn(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	var value Int
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}

	VALUE := Int(123)
	error = column.Set(rowID, VALUE)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if value != VALUE {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(VALUE) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID := column.FindOne(VALUE)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}

	error = column.Set(rowID, nil)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(nil) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID = column.FindOne(nil)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}
}

func TestFloatColumn(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", FLOAT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	var value Float
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}

	VALUE := Float(1.25)
	error = column.Set(rowID, VALUE)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if value != VALUE {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(VALUE) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID := column.FindOne(VALUE)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}

	error = column.Set(rowID, nil)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(nil) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID = column.FindOne(nil)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}
}

func TestGeoPointColumn(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", GEO_POINT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	var value GeoPoint
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}

	VALUE := GeoPoint{123, 456}
	error = column.Set(rowID, VALUE)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if value != VALUE {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(VALUE) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID := column.FindOne(VALUE)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}

	error = column.Set(rowID, nil)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(nil) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID = column.FindOne(nil)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}
}

func TestTextColumn(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", TEXT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	var value Text
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}

	VALUE := Text("Hello")
	error = column.Set(rowID, VALUE)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !reflect.DeepEqual(value, VALUE) {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(VALUE) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID := column.FindOne(VALUE)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}

	error = column.Set(rowID, nil)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(nil) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID = column.FindOne(nil)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v, rowID = %v",
			foundRowID, rowID)
	}
}

func TestReferenceColumn(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, &ColumnOptions{"Table"})
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	var value Int
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}

	VALUE := Int(123)
	error = column.Set(rowID, VALUE)
	if error == nil {
		t.Fatalf("Column.Set() succeeded")
	}
	VALUE = Int(0)
	error = column.Set(rowID, VALUE)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if value != VALUE {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(VALUE) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID := column.FindOne(VALUE)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}

	error = column.Set(rowID, nil)
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = column.Get(rowID, &value)
	if error != nil {
		t.Fatalf("Column.Get() failed: error = %v", error)
	}
	if !value.IsNA() {
		t.Fatalf("Column.Get() returned a wrong value: value = %v", value)
	}
	if !column.Contains(nil) {
		t.Fatalf("Column.Contains() failed")
	}
	foundRowID = column.FindOne(nil)
	if foundRowID != rowID {
		t.Fatalf("Column.FindOne() failed: foundRowID = %v", foundRowID)
	}
}

func TestIntIndex(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	index, error := column.CreateIndex("Index", TREE_INDEX)
	if error != nil {
		t.Fatalf("Column.CreateIndex() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	error = column.Set(rowID, Int(123))
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	error = table.SetKeyColumn("Column")
	if error != nil {
		t.Fatalf("Table.SetKeyColumn() failed: error = %v", error)
	}
	rowID, error = table.InsertRow(Int(456))
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	if !index.Contains(Int(456)) {
		t.Fatalf("Index.Contains() failed")
	}
	foundRowID := index.FindOne(Int(456))
	if foundRowID != rowID {
		t.Fatalf("Index.FindOne() failed: foundRowID = %v, rowID = %v",
			foundRowID, rowID)
	}
	cursor, error := index.Find(Int(456), nil)
	if error != nil {
		t.Fatalf("Index.Find() failed: error = %v", error)
	}
	defer cursor.Close()
	records := make([]Record, 10)
	count := cursor.Read(records)
	if count != 1 {
		t.Fatalf("Cursor.Read() could not read a record: count = %v", count)
	}
	if records[0].RowID != rowID {
		t.Fatalf("Cursor.Read() returned a wrong record: RowID = %v, Score = %v",
			records[0].RowID, records[0].Score)
	}
	cursorOptions := CursorOptions{0, 2, REVERSE_ORDER}
	cursor, error = index.FindInRange(Int(123), true, Int(456), true,
		&cursorOptions)
	if error != nil {
		t.Fatalf("Index.FindInRange() failed: error = %v", error)
	}
	defer cursor.Close()
	count = cursor.Read(records)
	if count != 2 {
		t.Fatalf("Cursor.Read() could not read all records: count = %v", count)
	}
	if records[0].RowID != 1 {
		t.Fatalf("Cursor.Read() returned a wrong record: RowID = %v, Score = %v",
			records[0].RowID, records[0].Score)
	}
	if records[1].RowID != 0 {
		t.Fatalf("Cursor.Read() returned a wrong record: RowID = %v, Score = %v",
			records[1].RowID, records[1].Score)
	}
}

func TestFloatIndex(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", FLOAT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	index, error := column.CreateIndex("Index", HASH_INDEX)
	if error != nil {
		t.Fatalf("Column.CreateIndex() failed: error = %v", error)
	}
	rowID, error := table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	error = column.Set(rowID, Float(1.25))
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	rowID, error = table.InsertRow(nil)
	if error != nil {
		t.Fatalf("Table.InsertRow() failed: error = %v", error)
	}
	error = column.Set(rowID, Float(4.75))
	if error != nil {
		t.Fatalf("Column.Set() failed: error = %v", error)
	}
	if !index.Contains(Float(4.75)) {
		t.Fatalf("Index.Contains() failed")
	}
	foundRowID := index.FindOne(Float(4.75))
	if foundRowID != rowID {
		t.Fatalf("Index.FindOne() failed: foundRowID = %v, rowID = %v",
			foundRowID, rowID)
	}
	cursor, error := index.Find(Float(4.75), nil)
	if error != nil {
		t.Fatalf("Index.Find() failed: error = %v", error)
	}
	defer cursor.Close()
	records := make([]Record, 10)
	count := cursor.Read(records)
	if count != 1 {
		t.Fatalf("Cursor.Read() could not read a record: count = %v", count)
	}
	if records[0].RowID != rowID {
		t.Fatalf("Cursor.Read() returned a wrong record: RowID = %v, Score = %v",
			records[0].RowID, records[0].Score)
	}
	cursorOptions := CursorOptions{0, 2, REVERSE_ORDER}
	cursor, error = index.FindInRange(Float(1.25), true, Float(4.75), true,
		&cursorOptions)
	if error == nil {
		t.Fatalf("Index.FindInRange() Succeeded for hash index")
	}
}

func TestTextIndex(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", TEXT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	index, error := column.CreateIndex("Index", TREE_INDEX)
	if error != nil {
		t.Fatalf("Column.CreateIndex() failed: error = %v", error)
	}
	VALUES := []Text{Text("He"), Text("Hello"), Text("World")}
	for i := 0; i < len(VALUES); i++ {
		rowID, error := table.InsertRow(nil)
		if error != nil {
			t.Fatalf("Table.InsertRow() failed: error = %v", error)
		}
		if rowID != Int(i) {
			t.Fatalf("Table.InsertRow() returned a wrong row ID: rowID = %v", rowID)
		}
		error = column.Set(rowID, VALUES[i])
		if error != nil {
			t.Fatalf("Column.Set() failed: error = %v", error)
		}
	}
	for i := 0; i < len(VALUES); i++ {
		if !index.Contains(VALUES[i]) {
			t.Fatalf("Index.Contains() failed: i = %v", i)
		}
		rowID := index.FindOne(VALUES[i])
		if rowID != Int(i) {
			t.Fatalf("Index.FindOne() failed: rowID = %v, i = %v", rowID, i)
		}
	}
	cursor, error := index.Find(VALUES[0], nil)
	if error != nil {
		t.Fatalf("Index.Find() failed: error = %v", error)
	}
	defer cursor.Close()
	records := make([]Record, 10)
	count := cursor.Read(records)
	if count != 1 {
		t.Fatalf("Cursor.Read() failed: count = %v", count)
	}
	cursor, error = index.FindInRange(nil, false, nil, false, nil)
	if error != nil {
		t.Fatalf("Index.FindInRange() failed: error = %v", error)
	}
	defer cursor.Close()
	count = cursor.Read(records)
	if count != 3 {
		t.Fatalf("Cursor.Read() failed: count = %v", count)
	}
	cursor, error = index.FindStartsWith(Text("He"), true, nil)
	if error != nil {
		t.Fatalf("Index.FindInRange() failed: error = %v", error)
	}
	defer cursor.Close()
	count = cursor.Read(records)
	if count != 2 {
		t.Fatalf("Cursor.Read() failed: count = %v", count)
	}
	cursor, error = index.FindPrefixes(Text("Hello"), nil)
	if error != nil {
		t.Fatalf("Index.FindPrefixes() failed: error = %v", error)
	}
	defer cursor.Close()
	count = cursor.Read(records)
	if count != 2 {
		t.Fatalf("Cursor.Read() failed: count = %v", count)
	}
}

func TestRowIDExpression(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	builder, error := CreateExpressionBuilder(table)
	if error != nil {
		t.Fatalf("CreateExpressionBuilder() failed: error = %v", error)
	}
	defer builder.Close()
	error = builder.PushRowID()
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushRowID() failed: error = %v", error)
	}
	expression, error := builder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	defer expression.Close()
	if expression.Table().handle != table.handle {
		t.Fatalf("Expression.Table() returned a wrong table")
	}
	if expression.DataType() != INT {
		t.Fatalf("Expression.DataType() returned a wrong data type: type = %v",
			expression.DataType())
	}
	if !expression.IsRowID() {
		t.Fatalf("Expression.IsRowID() failed")
	}
	if expression.IsScore() {
		t.Fatalf("Expression.IsScore() failed")
	}
}

func TestScoreExpression(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	builder, error := CreateExpressionBuilder(table)
	if error != nil {
		t.Fatalf("CreateExpressionBuilder() failed: error = %v", error)
	}
	defer builder.Close()
	error = builder.PushScore()
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushScore() failed: error = %v", error)
	}
	expression, error := builder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	defer expression.Close()
	if expression.Table().handle != table.handle {
		t.Fatalf("Expression.Table() returned a wrong table")
	}
	if expression.DataType() != FLOAT {
		t.Fatalf("Expression.DataType() returned a wrong data type: type = %v",
			expression.DataType())
	}
	if expression.IsRowID() {
		t.Fatalf("Expression.IsRowID() failed")
	}
	if !expression.IsScore() {
		t.Fatalf("Expression.IsScore() failed")
	}
}

func TestExpressionFilter(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", TEXT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	VALUES := []Text{Text("He"), Text("Hello"), Text("World")}
	for i := 0; i < len(VALUES); i++ {
		rowID, error := table.InsertRow(nil)
		if error != nil {
			t.Fatalf("Table.InsertRow() failed: error = %v", error)
		}
		error = column.Set(rowID, VALUES[i])
		if error != nil {
			t.Fatalf("Column.Set() failed: error = %v", error)
		}
	}
	builder, error := CreateExpressionBuilder(table)
	if error != nil {
		t.Fatalf("CreateExpressionBuilder() failed: error = %v", error)
	}
	defer builder.Close()
	error = builder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	error = builder.PushConstant(Text("Hello"))
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushConstant() failed: error = %v", error)
	}
	error = builder.PushOperator(LESS_EQUAL)
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushOperator() failed: error = %v", error)
	}
	expression, error := builder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	defer expression.Close()
	cursor, error := table.CreateCursor(nil)
	if error != nil {
		t.Fatalf("Table.CreateCursor() failed: error = %v", error)
	}
	defer cursor.Close()
	records := make([]Record, 10)
	count := cursor.Read(records)
	records = records[:count]
	error = expression.Filter(&records)
	if error != nil {
		t.Fatalf("Expression.Filter() failed: error = %v", error)
	}
	if len(records) != 2 {
		t.Fatalf("Expression.Filter() failed: len(records) = %v", len(records))
	}
}

func TestExpressionAdjust(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", FLOAT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	VALUES := []Float{1.5, 2.0, 2.5}
	for i := 0; i < len(VALUES); i++ {
		rowID, error := table.InsertRow(nil)
		if error != nil {
			t.Fatalf("Table.InsertRow() failed: error = %v", error)
		}
		error = column.Set(rowID, VALUES[i])
		if error != nil {
			t.Fatalf("Column.Set() failed: error = %v", error)
		}
	}
	builder, error := CreateExpressionBuilder(table)
	if error != nil {
		t.Fatalf("CreateExpressionBuilder() failed: error = %v", error)
	}
	defer builder.Close()
	error = builder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	error = builder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	error = builder.PushOperator(MULTIPLICATION)
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushOperator() failed: error = %v", error)
	}
	expression, error := builder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	defer expression.Close()
	cursor, error := table.CreateCursor(nil)
	if error != nil {
		t.Fatalf("Table.CreateCursor() failed: error = %v", error)
	}
	defer cursor.Close()
	records := make([]Record, 10)
	count := cursor.Read(records)
	if count != 3 {
		t.Fatalf("Cursor.Read() failed: count = %v", count)
	}
	records = records[:count]
	error = expression.Adjust(records)
	if error != nil {
		t.Fatalf("Expression.Adjust() failed: error = %v", error)
	}
	for i := 0; i < count; i++ {
		if records[i].Score != (VALUES[i] * VALUES[i]) {
			t.Fatalf("Expression.Adjust() failed: "+
				"i = %v, RowID = %v, Score = %v, VALUE = %v",
				i, records[i].RowID, records[i].Score, VALUES[i])
		}
	}
}

func TestExpressionEvaluate(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	VALUES := []Int{123, 456, 789}
	for i := 0; i < len(VALUES); i++ {
		rowID, error := table.InsertRow(nil)
		if error != nil {
			t.Fatalf("Table.InsertRow() failed: error = %v", error)
		}
		error = column.Set(rowID, VALUES[i])
		if error != nil {
			t.Fatalf("Column.Set() failed: error = %v", error)
		}
	}
	builder, error := CreateExpressionBuilder(table)
	if error != nil {
		t.Fatalf("CreateExpressionBuilder() failed: error = %v", error)
	}
	defer builder.Close()
	error = builder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	error = builder.PushConstant(Int(100))
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushConstant() failed: error = %v", error)
	}
	error = builder.PushOperator(PLUS)
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushOperator() failed: error = %v", error)
	}
	expression, error := builder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	defer expression.Close()
	cursor, error := table.CreateCursor(nil)
	if error != nil {
		t.Fatalf("Table.CreateCursor() failed: error = %v", error)
	}
	defer cursor.Close()
	records := make([]Record, 10)
	count := cursor.Read(records)
	if count != 3 {
		t.Fatalf("Cursor.Read() failed: count = %v", count)
	}
	records = records[:count]
	values := make([]Int, count)
	error = expression.Evaluate(records, values)
	if error != nil {
		t.Fatalf("Expression.Evaluate() failed: error = %v", error)
	}
	for i := 0; i < count; i++ {
		if values[i] != (VALUES[i] + 100) {
			t.Fatalf("Expression.Evaluate() failed: "+
				"i = %v, VALUE = %v, value = %v", i, VALUES[i], values[i])
		}
	}
}

func TestExpressionParser(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", INT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	VALUES := []Int{123, 456, 789}
	for i := 0; i < len(VALUES); i++ {
		rowID, error := table.InsertRow(nil)
		if error != nil {
			t.Fatalf("Table.InsertRow() failed: error = %v", error)
		}
		error = column.Set(rowID, VALUES[i])
		if error != nil {
			t.Fatalf("Column.Set() failed: error = %v", error)
		}
	}
	expression, error := ParseExpression(table, "Column + 100")
	defer expression.Close()
	cursor, error := table.CreateCursor(nil)
	if error != nil {
		t.Fatalf("Table.CreateCursor() failed: error = %v", error)
	}
	defer cursor.Close()
	records := make([]Record, 10)
	count := cursor.Read(records)
	if count != 3 {
		t.Fatalf("Cursor.Read() failed: count = %v", count)
	}
	records = records[:count]
	values := make([]Int, count)
	error = expression.Evaluate(records, values)
	if error != nil {
		t.Fatalf("Expression.Evaluate() failed: error = %v", error)
	}
	for i := 0; i < count; i++ {
		if values[i] != (VALUES[i] + 100) {
			t.Fatalf("Expression.Evaluate() failed: "+
				"i = %v, VALUE = %v, value = %v", i, VALUES[i], values[i])
		}
	}
}

func TestPipelineFilterAndAdjuster(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", FLOAT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	VALUES := []Float{1.5, 2.0, 2.5}
	for i := 0; i < len(VALUES); i++ {
		rowID, error := table.InsertRow(nil)
		if error != nil {
			t.Fatalf("Table.InsertRow() failed: error = %v", error)
		}
		error = column.Set(rowID, VALUES[i])
		if error != nil {
			t.Fatalf("Column.Set() failed: error = %v", error)
		}
	}

	pipelineBuilder, error := CreatePipelineBuilder(table)
	if error != nil {
		t.Fatalf("CreatePipelineBuilder() failed: error = %v", error)
	}
	defer pipelineBuilder.Close()
	cursor, error := table.CreateCursor(nil)
	if error != nil {
		t.Fatalf("Table.CreateCursor() failed: error = %v", error)
	}
	error = pipelineBuilder.PushCursor(cursor)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushCursor() failed: error = %v", error)
	}

	expressionBuilder, error := CreateExpressionBuilder(table)
	if error != nil {
		t.Fatalf("CreateExpressionBuilder() failed: error = %v", error)
	}
	defer expressionBuilder.Close()
	error = expressionBuilder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	error = expressionBuilder.PushConstant(Float(2.0))
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushConstant() failed: error = %v", error)
	}
	error = expressionBuilder.PushOperator(GREATER_EQUAL)
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushOperator() failed: error = %v", error)
	}
	expression, error := expressionBuilder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	error = pipelineBuilder.PushFilter(expression, 0, 100)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushFilter() failed: error = %v", error)
	}

	error = expressionBuilder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	error = expressionBuilder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	error = expressionBuilder.PushOperator(MULTIPLICATION)
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushOperator() failed: error = %v", error)
	}
	expression, error = expressionBuilder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	error = pipelineBuilder.PushAdjuster(expression)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushAdjuster() failed: error = %v", error)
	}

	pipeline, error := pipelineBuilder.Release(nil)
	if error != nil {
		t.Fatalf("PipelineBuilder.Release() failed: error = %v", error)
	}
	defer pipeline.Close()
	records, error := pipeline.Flush()
	if error != nil {
		t.Fatalf("Pipeline.Flush() failed: error = %v", error)
	}
	if len(records) != 2 {
		t.Fatalf("Pipeline.Flush() failed: len(records) = %v", len(records))
	}
	if (records[0].RowID != 1) || (records[0].Score != (VALUES[1] * VALUES[1])) {
		t.Fatalf("Pipeline.Flush() failed: i = %v, RowID = %v, Score = %v",
			0, records[0].RowID, records[0].Score)
	}
	if (records[1].RowID != 2) || (records[1].Score != (VALUES[2] * VALUES[2])) {
		t.Fatalf("Pipeline.Flush() failed: i = %v, RowID = %v, Score = %v",
			1, records[1].RowID, records[1].Score)
	}
}

func TestPipelineSorterAndMerger(t *testing.T) {
	db, error := CreateDB()
	if error != nil {
		t.Fatalf("CreateDB() failed: error = %v", error)
	}
	defer db.Close()
	table, error := db.CreateTable("Table")
	if error != nil {
		t.Fatalf("DB.CreateTable() failed: error = %v", error)
	}
	column, error := table.CreateColumn("Column", FLOAT, nil)
	if error != nil {
		t.Fatalf("Table.CreateColumn() failed: error = %v", error)
	}
	index, error := column.CreateIndex("Index", TREE_INDEX)
	if error != nil {
		t.Fatalf("Table.CreateIndex() failed: error = %v", error)
	}
	VALUES := []Float{4.5, 1.5, 6.0, 0.0, 3.0}
	for i := 0; i < len(VALUES); i++ {
		rowID, error := table.InsertRow(nil)
		if error != nil {
			t.Fatalf("Table.InsertRow() failed: error = %v", error)
		}
		error = column.Set(rowID, VALUES[i])
		if error != nil {
			t.Fatalf("Column.Set() failed: error = %v", error)
		}
	}

	pipelineBuilder, error := CreatePipelineBuilder(table)
	if error != nil {
		t.Fatalf("CreatePipelineBuilder() failed: error = %v", error)
	}
	defer pipelineBuilder.Close()
	expressionBuilder, error := CreateExpressionBuilder(table)
	if error != nil {
		t.Fatalf("CreateExpressionBuilder() failed: error = %v", error)
	}
	defer expressionBuilder.Close()

	cursor, error := index.FindInRange(nil, false, Float(4.5), true, nil)
	if error != nil {
		t.Fatalf("index.FindInRange() failed: error = %v", error)
	}
	error = pipelineBuilder.PushCursor(cursor)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushCursor() failed: error = %v", error)
	}
	error = expressionBuilder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	expression, error := expressionBuilder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	error = pipelineBuilder.PushAdjuster(expression)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushAdjuster() failed: error = %v", error)
	}

	cursor, error = index.FindInRange(Float(1.5), true, nil, false, nil)
	if error != nil {
		t.Fatalf("index.FindInRange() failed: error = %v", error)
	}
	error = pipelineBuilder.PushCursor(cursor)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushCursor() failed: error = %v", error)
	}
	error = expressionBuilder.PushColumn("Column")
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushColumn() failed: error = %v", error)
	}
	expression, error = expressionBuilder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	error = pipelineBuilder.PushAdjuster(expression)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushAdjuster() failed: error = %v", error)
	}

	var mergerOptions MergerOptions
	mergerOptions.LogicalOperatorType = MERGER_AND
	mergerOptions.ScoreOperatorType = MERGER_PLUS
	mergerOptions.Limit = 10
	error = pipelineBuilder.PushMerger(&mergerOptions)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushMerger() failed: error = %v", error)
	}

	error = expressionBuilder.PushRowID()
	if error != nil {
		t.Fatalf("ExpressionBuilder.PushRowID() failed: error = %v", error)
	}
	expression, error = expressionBuilder.Release()
	if error != nil {
		t.Fatalf("ExpressionBuilder.Release() failed: error = %v", error)
	}
	orders := make([]SorterOrder, 1)
	orders[0].Expression = expression
	orders[0].OrderType = REGULAR_ORDER
	sorter, error := CreateSorter(orders, nil)
	if error != nil {
		t.Fatalf("CreateSorter() failed: error = %v", error)
	}
	error = pipelineBuilder.PushSorter(sorter)
	if error != nil {
		t.Fatalf("PipelineBuilder.PushSorter() failed: error = %v", error)
	}

	pipeline, error := pipelineBuilder.Release(nil)
	if error != nil {
		t.Fatalf("PipelineBuilder.Release() failed: error = %v", error)
	}
	defer pipeline.Close()
	records, error := pipeline.Flush()
	if error != nil {
		t.Fatalf("Pipeline.Flush() failed: error = %v", error)
	}
	if len(records) != 3 {
		t.Fatalf("Pipeline.Flush() failed: len(records) = %v", len(records))
	}
	if (records[0].RowID != 0) || (records[0].Score != (VALUES[0] + VALUES[0])) {
		t.Fatalf("Pipeline.Flush() failed: i = %v, RowID = %v, Score = %v",
			0, records[0].RowID, records[0].Score)
	}
	if (records[1].RowID != 1) || (records[1].Score != (VALUES[1] + VALUES[1])) {
		t.Fatalf("Pipeline.Flush() failed: i = %v, RowID = %v, Score = %v",
			1, records[1].RowID, records[1].Score)
	}
	if (records[2].RowID != 4) || (records[2].Score != (VALUES[4] + VALUES[4])) {
		t.Fatalf("Pipeline.Flush() failed: i = %v, RowID = %v, Score = %v",
			2, records[2].RowID, records[2].Score)
	}
}
