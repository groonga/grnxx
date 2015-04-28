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
		return GeoPoint{ int32(latitude), int32(longitude) }
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
