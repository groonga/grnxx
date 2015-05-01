package gnx

import (
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func createTempGrnDB(tb testing.TB) (string, string, *GrnDB) {
	dirPath, err := ioutil.TempDir("", "grn_test")
	if err != nil {
		tb.Fatalf("ioutil.TempDir() failed: %v", err)
	}
	dbPath := dirPath + "/db"
	db, err := CreateGrnDB(dbPath)
	if err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("CreateGrnDB() failed: %v", err)
	}
	return dirPath, dbPath, db
}

func removeTempGrnDB(tb testing.TB, dirPath string, db *GrnDB) {
	if err := db.Close(); err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("GrnDB.Close() failed: %v", err)
	}
	if err := os.RemoveAll(dirPath); err != nil {
		tb.Fatalf("os.RemoveAll() failed: %v", err)
	}
}

func createTempGrnTable(tb testing.TB, name string, options *TableOptions) (
	string, string, *GrnDB, *GrnTable) {
	dirPath, dbPath, db := createTempGrnDB(tb)
	table, err := db.CreateTable(name, options)
	if err != nil {
		removeTempGrnDB(tb, dirPath, db)
		tb.Fatalf("GrnDB.CreateTable() failed: %v", err)
	}
	return dirPath, dbPath, db, table
}

func createTempGrnColumn(tb testing.TB, tableName string,
	tableOptions *TableOptions, columnName string, valueType string,
	columnOptions *ColumnOptions) (
	string, string, *GrnDB, *GrnTable, *GrnColumn) {
	dirPath, dbPath, db, table := createTempGrnTable(tb, tableName, tableOptions)
	column, err := table.CreateColumn(columnName, valueType, columnOptions)
	if err != nil {
		removeTempGrnDB(tb, dirPath, db)
		tb.Fatalf("GrnDB.CreateTable() failed: %v", err)
	}
	return dirPath, dbPath, db, table, column
}

func TestCreateGrnDB(t *testing.T) {
	dirPath, _, db := createTempGrnDB(t)
	removeTempGrnDB(t, dirPath, db)
}

func TestOpenGrnDB(t *testing.T) {
	dirPath, dbPath, db := createTempGrnDB(t)
	db2, err := OpenGrnDB(dbPath)
	if err != nil {
		t.Fatalf("OpenGrnDB() failed: %v", err)
	}
	db2.Close()
	removeTempGrnDB(t, dirPath, db)
}

func testGrnDBCreateTableWithKey(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, _ := createTempGrnTable(t, "Table", options)
	removeTempGrnDB(t, dirPath, db)
}

func testGrnDBCreateTableWithValue(t *testing.T, valueType string) {
	options := NewTableOptions()
	options.ValueType = valueType
	dirPath, _, db, _ := createTempGrnTable(t, "Table", options)
	removeTempGrnDB(t, dirPath, db)
}

func testGrnDBCreateTableRef(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, _ := createTempGrnTable(t, "Table", options)
	defer removeTempGrnDB(t, dirPath, db)

	options.KeyType = "Table"
	_, err := db.CreateTable("Table2", options)
	if err != nil {
		t.Fatalf("GrnDB.CreateTable() failed: %v", err)
	}
}

func TestGrnDBCreateTable(t *testing.T) {
	dirPath, _, db, _ := createTempGrnTable(t, "Table", nil)
	removeTempGrnDB(t, dirPath, db)

	testGrnDBCreateTableWithKey(t, "Bool")
	testGrnDBCreateTableWithKey(t, "Int")
	testGrnDBCreateTableWithKey(t, "Float")
	testGrnDBCreateTableWithKey(t, "GeoPoint")
	testGrnDBCreateTableWithKey(t, "Text")

	testGrnDBCreateTableWithValue(t, "Bool")
	testGrnDBCreateTableWithValue(t, "Int")
	testGrnDBCreateTableWithValue(t, "Float")
	testGrnDBCreateTableWithValue(t, "GeoPoint")

	testGrnDBCreateTableRef(t, "Bool")
	testGrnDBCreateTableRef(t, "Int")
	testGrnDBCreateTableRef(t, "Float")
	testGrnDBCreateTableRef(t, "GeoPoint")
	testGrnDBCreateTableRef(t, "Text")
}

func generateRandomKey(keyType string) interface{} {
	switch keyType {
	case "Bool":
		if (rand.Int() & 1) == 1 {
			return True
		} else {
			return False
		}
	case "Int":
		return Int(rand.Int63())
	case "Float":
		return Float(rand.Float64())
	case "GeoPoint":
		const (
			MinLatitude  = 73531000
			MaxLatitude  = 164006000
			MinLongitude = 439451000
			MaxLongitude = 554351000
		)
		latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
		longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
		return GeoPoint{int32(latitude), int32(longitude)}
	case "Text":
		return Text(strconv.Itoa(rand.Int()))
	default:
		return nil
	}
}

func testGrnTableInsertRow(t *testing.T, keyType string) {
	options := NewTableOptions()
	if keyType != "" {
		options.TableType = PatTable
	}
	options.KeyType = keyType
	dirPath, _, db, table := createTempGrnTable(t, "Table", options)
	defer removeTempGrnDB(t, dirPath, db)

	count := 0
	for i := 0; i < 100; i++ {
		inserted, _, err := table.InsertRow(generateRandomKey(keyType))
		if err != nil {
			t.Fatalf("GrnTable.InsertRow() failed: %v", err)
		}
		if inserted {
			count++
		}
	}
	t.Logf("keyType = <%s>, count = %d", keyType, count)
}

func TestGrnTableInsertRow(t *testing.T) {
	testGrnTableInsertRow(t, "")
	testGrnTableInsertRow(t, "Bool")
	testGrnTableInsertRow(t, "Int")
	testGrnTableInsertRow(t, "Float")
	testGrnTableInsertRow(t, "GeoPoint")
	testGrnTableInsertRow(t, "Text")
}

func testGrnTableCreateScalarColumn(t *testing.T, valueType string) {
	dirPath, _, db, table, _ := createTempGrnColumn(t, "Table", nil, "Value", valueType, nil)
	defer removeTempGrnDB(t, dirPath, db)

	if column, err := table.FindColumn("_id"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("_id: %+v", column)
	}
	if column, err := table.FindColumn("Value"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value: %+v", column)
	}
}

func testGrnTableCreateVectorColumn(t *testing.T, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, _ := createTempGrnColumn(t, "Table", nil, "Value", valueType, options)
	defer removeTempGrnDB(t, dirPath, db)

	if column, err := table.FindColumn("_id"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("_id: %+v", column)
	}
	if column, err := table.FindColumn("Value"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value: %+v", column)
	}
}

func testGrnTableCreateScalarRefColumn(t *testing.T, keyType string) {
	options := NewTableOptions()
	options.TableType = PatTable
	options.KeyType = keyType
	dirPath, _, db, table, _ := createTempGrnColumn(t, "Table", options, "Value", "Table", nil)
	defer removeTempGrnDB(t, dirPath, db)

	if column, err := table.FindColumn("Value"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value: %+v", column)
	}
	if column, err := table.FindColumn("Value._id"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value._id: %+v", column)
	}
	if column, err := table.FindColumn("Value._key"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value._key: %+v", column)
	}
}

func testGrnTableCreateVectorRefColumn(t *testing.T, keyType string) {
	tableOptions := NewTableOptions()
	tableOptions.TableType = PatTable
	tableOptions.KeyType = keyType
	columnOptions := NewColumnOptions()
	columnOptions.ColumnType = VectorColumn
	dirPath, _, db, table, _ := createTempGrnColumn(t, "Table", tableOptions, "Value", "Table", columnOptions)
	defer removeTempGrnDB(t, dirPath, db)

	if column, err := table.FindColumn("Value"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value: %+v", column)
	}
	if column, err := table.FindColumn("Value._id"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value._id: %+v", column)
	}
	if column, err := table.FindColumn("Value._key"); err != nil {
		t.Fatalf("Table.FindColumn() failed: %v", err)
	} else {
		t.Logf("Value._key: %+v", column)
	}
}

func TestGrnTableCreateColumn(t *testing.T) {
	testGrnTableCreateScalarColumn(t, "Bool")
	testGrnTableCreateScalarColumn(t, "Int")
	testGrnTableCreateScalarColumn(t, "Float")
	testGrnTableCreateScalarColumn(t, "GeoPoint")
	testGrnTableCreateScalarColumn(t, "Text")

	testGrnTableCreateVectorColumn(t, "Bool")
	testGrnTableCreateVectorColumn(t, "Int")
	testGrnTableCreateVectorColumn(t, "Float")
	testGrnTableCreateVectorColumn(t, "GeoPoint")
	testGrnTableCreateVectorColumn(t, "Text")

	testGrnTableCreateScalarRefColumn(t, "Bool")
	testGrnTableCreateScalarRefColumn(t, "Int")
	testGrnTableCreateScalarRefColumn(t, "Float")
	testGrnTableCreateScalarRefColumn(t, "GeoPoint")
	testGrnTableCreateScalarRefColumn(t, "Text")

	testGrnTableCreateVectorRefColumn(t, "Bool")
	testGrnTableCreateVectorRefColumn(t, "Int")
	testGrnTableCreateVectorRefColumn(t, "Float")
	testGrnTableCreateVectorRefColumn(t, "GeoPoint")
	testGrnTableCreateVectorRefColumn(t, "Text")
}

func generateRandomScalarValue(valueType string) interface{} {
	switch valueType {
	case "Bool":
		if (rand.Int() & 1) == 1 {
			return True
		} else {
			return False
		}
	case "Int":
		return Int(rand.Int63())
	case "Float":
		return Float(rand.Float64())
	case "GeoPoint":
		const (
			MinLatitude  = 73531000
			MaxLatitude  = 164006000
			MinLongitude = 439451000
			MaxLongitude = 554351000
		)
		latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
		longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
		return GeoPoint{int32(latitude), int32(longitude)}
	case "Text":
		return Text(strconv.Itoa(rand.Int()))
	default:
		return nil
	}
}

func testGrnColumnSetScalarValue(t *testing.T, valueType string) {
	dirPath, _, db, table, column := createTempGrnColumn(t, "Table", nil, "Value", valueType, nil)
	defer removeTempGrnDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("GrnTable.InsertRow() failed: %v", err)
		}
		if err := column.SetValue(id, generateRandomScalarValue(valueType)); err != nil {
			t.Fatalf("GrnColumn.SetValue() failed: %v", err)
		}
	}

	bytes, _ := db.Query("select Table --limit 3")
	t.Logf("valueType = <%s>, result = %s", valueType, string(bytes))
}

func generateRandomVectorValue(valueType string) interface{} {
	size := rand.Int() % 10
	switch valueType {
	case "Bool":
		value := make([]Bool, size)
		for i := 0; i < size; i++ {
			if (rand.Int() & 1) == 1 {
				value[i] = True
			}
		}
		return value
	case "Int":
		value := make([]Int, size)
		for i := 0; i < size; i++ {
			value[i] = Int(rand.Int63())
		}
		return value
	case "Float":
		value := make([]Float, size)
		for i := 0; i < size; i++ {
			value[i] = Float(rand.Float64())
		}
		return value
	case "GeoPoint":
		const (
			MinLatitude  = 73531000
			MaxLatitude  = 164006000
			MinLongitude = 439451000
			MaxLongitude = 554351000
		)
		value := make([]GeoPoint, size)
		for i := 0; i < size; i++ {
			latitude := MinLatitude + rand.Intn(MaxLatitude-MinLatitude+1)
			longitude := MinLongitude + rand.Intn(MaxLongitude-MinLongitude+1)
			value[i] = GeoPoint{int32(latitude), int32(longitude)}
		}
		return value
	case "Text":
		value := make([]Text, size)
		for i := 0; i < size; i++ {
			value[i] = Text(strconv.Itoa(rand.Int()))
		}
		return value
	default:
		return nil
	}
}

func testGrnColumnSetVectorValue(t *testing.T, valueType string) {
	options := NewColumnOptions()
	options.ColumnType = VectorColumn
	dirPath, _, db, table, column := createTempGrnColumn(t, "Table", nil, "Value", valueType, options)
	defer removeTempGrnDB(t, dirPath, db)

	for i := 0; i < 100; i++ {
		_, id, err := table.InsertRow(nil)
		if err != nil {
			t.Fatalf("GrnTable.InsertRow() failed: %v", err)
		}
		if err := column.SetValue(id, generateRandomVectorValue(valueType)); err != nil {
			t.Fatalf("GrnColumn.SetValue() failed: %v", err)
		}
	}

	bytes, _ := db.Query("select Table --limit 3")
	t.Logf("valueType = <%s>, result = %s", valueType, string(bytes))
}

func TestGrnColumnSetValue(t *testing.T) {
	testGrnColumnSetScalarValue(t, "Bool")
	testGrnColumnSetScalarValue(t, "Int")
	testGrnColumnSetScalarValue(t, "Float")
	testGrnColumnSetScalarValue(t, "GeoPoint")
	testGrnColumnSetScalarValue(t, "Text")

	testGrnColumnSetVectorValue(t, "Bool")
	testGrnColumnSetVectorValue(t, "Int")
	testGrnColumnSetVectorValue(t, "Float")
	testGrnColumnSetVectorValue(t, "GeoPoint")
	testGrnColumnSetVectorValue(t, "Text")
}
