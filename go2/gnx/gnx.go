package gnx

/*
#cgo pkg-config: groonga
#include <groonga.h>
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json" // FIXME
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

// -- Data types --

type Bool uint8
type Int int64
type Float float64
//type GeoPoint struct {
//	Latitude  int32
//	Longitude int32
//}
type Text []byte
type BoolVector []Bool
type IntVector []Int
type FloatVector []Float
//type GeoPointVector []GeoPoint
type TextVector []Text

const (
	FALSE = Bool(0)
	TRUE  = Bool(3)
)

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
//func NAGeoPoint() GeoPoint {
//	return GeoPoint{math.MinInt32, math.MinInt32}
//}
func NAText() Text {
	return nil
}
func NABoolVector() BoolVector {
	return nil
}
func NAIntVector() IntVector {
	return nil
}
func NAFloatVector() FloatVector {
	return nil
}
//func NAGeoPointVector() GeoPointVector {
//	return nil
//}
func NATextVector() TextVector {
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
//func (this GeoPoint) IsNA() bool {
//	return this.Latitude == math.MinInt32
//}
func (this Text) IsNA() bool {
	return this == nil
}
func (this BoolVector) IsNA() bool {
	return this == nil
}
func (this IntVector) IsNA() bool {
	return this == nil
}
func (this FloatVector) IsNA() bool {
	return this == nil
}
//func (this GeoPointVector) IsNA() bool {
//	return this == nil
//}
func (this TextVector) IsNA() bool {
	return this == nil
}

// -- GroongaDB --

type GroongaDB struct {
	ctx *C.grn_ctx
}

var groongaInitCount = 0

func DisableGroongaInitCount() {
	groongaInitCount = -1
}

func initGroonga() error {
	switch groongaInitCount {
	case -1: // Disabled.
		return nil
	case 0:
		if rc := C.grn_init(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_init() failed: rc = %d", rc)
		}
	}
	groongaInitCount++
	return nil
}

func finGroonga() error {
	switch groongaInitCount {
	case -1: // Disabled.
		return nil
	case 0:
		return fmt.Errorf("Groonga is not initialized yet")
	case 1:
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_fin() failed: rc = %d", rc)
		}
	}
	groongaInitCount--
	return nil
}

func CreateGroongaDB(path string) (*GroongaDB, error) {
	if err := initGroonga(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(0)
	if ctx == nil {
		finGroonga()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if db := C.grn_db_create(ctx, cPath, nil); db == nil {
		C.grn_ctx_close(ctx)
		finGroonga()
		message := C.GoString(&ctx.errbuf[0])
		return nil, fmt.Errorf("grn_db_create() failed: err = %s", message)
	}
	return &GroongaDB{ctx}, nil
}

// Create a DB in a temporary directory.
//
// Example:
// db, dir, err := CreateTempGroonggnx.aDB("", "gnx")
// if err != nil {
//   log.Fatalln(err)
// }
// defer os.RemoveAll(dir)
// defer db.Close()
// ...
func createTempGroongaDB(dir, prefix string) (*GroongaDB, string, error) {
	tempDir, err := ioutil.TempDir(dir, prefix)
	if err != nil {
		return nil, "", err
	}
	db, err := CreateGroongaDB(tempDir + "/db")
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, "", err
	}
	return db, tempDir, nil
}

func OpenGroongaDB(path string) (*GroongaDB, error) {
	if err := initGroonga(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(0)
	if ctx == nil {
		finGroonga()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if db := C.grn_db_open(ctx, cPath); db == nil {
		C.grn_ctx_close(ctx)
		finGroonga()
		message := C.GoString(&ctx.errbuf[0])
		return nil, fmt.Errorf("grn_db_create() failed: err = %s", message)
	}
	return &GroongaDB{ctx}, nil
}

func (db *GroongaDB) Close() error {
	if db.ctx == nil {
		return nil
	}
	if rc := C.grn_ctx_close(db.ctx); rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_ctx_close() failed: rc = %d", rc)
	}
	db.ctx = nil
	finGroonga()
	return nil
}

func (db *GroongaDB) Send(command string) error {
	cCommand := C.CString(command)
	defer C.free(unsafe.Pointer(cCommand))
	rc := C.grn_ctx_send(db.ctx, cCommand, C.uint(len(command)), 0)
	switch {
	case rc != C.GRN_SUCCESS:
		return fmt.Errorf(
			"grn_ctx_send() failed: rc = %d, err = %s",
			rc, C.GoString(&db.ctx.errbuf[0]))
	case db.ctx.rc != C.GRN_SUCCESS:
		return fmt.Errorf(
			"grn_ctx_send() failed: ctx.rc = %d, err = %s",
			db.ctx.rc, C.GoString(&db.ctx.errbuf[0]))
	}
	return nil
}

func (db *GroongaDB) Recv() ([]byte, error) {
	var resultBuffer *C.char
	var resultLength C.uint
	var flags C.int
	rc := C.grn_ctx_recv(db.ctx, &resultBuffer, &resultLength, &flags)
	switch {
	case rc != C.GRN_SUCCESS:
		return nil, fmt.Errorf(
			"grn_ctx_recv() failed: rc = %d, err = %s",
			rc, C.GoString(&db.ctx.errbuf[0]))
	case db.ctx.rc != C.GRN_SUCCESS:
		return nil, fmt.Errorf(
			"grn_ctx_recv() failed: ctx.rc = %d, err = %s",
			db.ctx.rc, C.GoString(&db.ctx.errbuf[0]))
	}
	result := C.GoBytes(unsafe.Pointer(resultBuffer), C.int(resultLength))
	return result, nil
}

func (db *GroongaDB) Query(command string) ([]byte, error) {
	err := db.Send(command)
	if err != nil {
		_, _ = db.Recv()
		return nil, err
	}
	return db.Recv()
}

func (db *GroongaDB) load(
	tableName string, columnNames []string, records [][]Valuer) (int, error) {
	jsonRecords, err := json.Marshal(records)
	if err != nil {
		return 0, err
	}
	command := fmt.Sprintf("load --table '%s' --columns '%s' --values '%s'",
		tableName, strings.Join(columnNames, ","), string(jsonRecords))
	bytes, err := db.Query(command)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(bytes))
}

func (db *GroongaDB) loadMap(
	tableName string, recordMaps []map[string]Valuer) (int, error) {
	jsonRecords, err := json.Marshal(recordMaps)
	if err != nil {
		return 0, err
	}
	command := fmt.Sprintf("load --table '%s' --values '%s'",
		tableName, string(jsonRecords))
	bytes, err := db.Query(command)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(bytes))
}

func (db *GroongaDB) loadC(
	tableName string, columnNames []string,
	columnarRecords []interface{}) (int, error) {
	numValues := -1
	for _, columnarValues := range columnarRecords {
		var thisLen int
		switch values := columnarValues.(type) {
		case []Bool:
			thisLen = len(values)
		case []Int:
			thisLen = len(values)
		case []Float:
			thisLen = len(values)
		case []Text:
			thisLen = len(values)
		case []BoolVector:
			thisLen = len(values)
		case []IntVector:
			thisLen = len(values)
		case []FloatVector:
			thisLen = len(values)
		case []TextVector:
			thisLen = len(values)
		}
		if numValues == -1 {
			numValues = thisLen
		} else if thisLen != numValues {
			return 0, fmt.Errorf("length conflict: numValues = %d, thisLen = %d",
				numValues, thisLen)
		}
	}
	if numValues == -1 {
		numValues = 0
	}

	// TODO: Use cgo for JSON encoding.
	records := make([][]Valuer, numValues)
	for i := 0; i < numValues; i++ {
		records[i] = make([]Valuer, len(columnNames))
	}
	for i, columnarValues := range columnarRecords {
		switch values := columnarValues.(type) {
		case []Bool:
			for j, value := range values {
				records[j][i] = value
			}
		case []Int:
			for j, value := range values {
				records[j][i] = value
			}
		case []Float:
			for j, value := range values {
				records[j][i] = value
			}
		case []Text:
			for j, value := range values {
				records[j][i] = value
			}
		case []BoolVector:
			for j, value := range values {
				records[j][i] = value
			}
		case []IntVector:
			for j, value := range values {
				records[j][i] = value
			}
		case []FloatVector:
			for j, value := range values {
				records[j][i] = value
			}
		case []TextVector:
			for j, value := range values {
				records[j][i] = value
			}
		}
	}
	return db.load(tableName, columnNames, records)
}

func (db *GroongaDB) loadCMap(
	tableName string, columnarRecordsMap map[string]interface{}) (int, error) {
	columnNames := make([]string, len(columnarRecordsMap))
	columnarRecords := make([]interface{}, len(columnarRecordsMap))
	i := 0
	for columnName, columnarValues := range columnarRecordsMap {
		columnNames[i] = columnName
		columnarRecords[i] = columnarValues
		i++
	}
	return db.loadC(tableName, columnNames, columnarRecords)
}

// -- DB --

type DB struct {
	groongaDBs []*GroongaDB
}

func CreateDB(path string, n int) (*DB, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid parameter: n = %d", n)
	}
	groongaDBs := make([]*GroongaDB, n)
	for i := 0; i < n; i++ {
		dbPath := path
		if i != 0 {
			dbPath += strconv.Itoa(i)
		}
		groongaDB, err := CreateGroongaDB(dbPath)
		if err != nil {
			for j := 0; j < i; j++ {
				groongaDBs[j].Close()
			}
			return nil, err
		}
		groongaDBs[i] = groongaDB
	}
	return &DB{groongaDBs}, nil
}

func CreateTempDB(dir, prefix string, n int) (*DB, string, error) {
	if n <= 0 {
		return nil, "", fmt.Errorf("invalid parameter: n = %d", n)
	}
	tempDir, err := ioutil.TempDir(dir, prefix)
	if err != nil {
		return nil, "", err
	}
	db, err := CreateDB(tempDir+"/db", n)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, "", err
	}
	return db, tempDir, nil
}

func OpenDB(path string) (*DB, error) {
	var groongaDBs []*GroongaDB
	for i := 0; ; i++ {
		dbPath := path
		if i != 0 {
			dbPath += strconv.Itoa(i)
		}
		groongaDB, err := OpenGroongaDB(dbPath)
		if err != nil {
			if i == 0 {
				return nil, err
			}
			break
		}
		groongaDBs = append(groongaDBs, groongaDB)
	}
	return &DB{groongaDBs}, nil
}

func (db *DB) Close() error {
	for i := 0; i < len(db.groongaDBs); i++ {
		db.groongaDBs[i].Close()
	}
	return nil
}

func (db *DB) GroongaQuery(i int, command string) ([]byte, error) {
	if (i < 0) || (i >= len(db.groongaDBs)) {
		return nil, fmt.Errorf("invalid DB: i = %d", i)
	}
	return db.groongaDBs[i].Query(command)
}

func (db *DB) checkTableName(tableName string) error {
	for _, c := range tableName {
		if (c != '_') && ((c < 'A') && (c > 'Z')) && ((c < 'a') && (c > 'Z')) {
			return fmt.Errorf("Invalid rune in table name")
		}
	}
	return nil
}

func (db *DB) checkColumnName(columnName string) error {
	for _, c := range columnName {
		if (c != '_') && ((c < 'A') && (c > 'Z')) && ((c < 'a') && (c > 'Z')) {
			return fmt.Errorf("Invalid rune in column name")
		}
	}
	return nil
}

func (db *DB) loadN(
	tableName string, columnNames []string, records [][]Valuer) (int, error) {
	return 0, nil
}

func (db *DB) loadMapN(
	tableName string, records []map[string]Valuer) (int, error) {
	return 0, nil
}

func (db *DB) loadCN(
	tableName string, columnNames []string, records []interface{}) (int, error) {
	return 0, nil
}

func (db *DB) loadCMapN(
	tableName string, records map[string]interface{}) (int, error) {
	return 0, nil
}

func (db *DB) Load(
	tableName string, columnNames []string, records [][]Valuer) (int, error) {
	// Check arguments.
	if err := db.checkTableName(tableName); err != nil {
		return 0, err
	}
	for _, columnName := range columnNames {
		if err := db.checkColumnName(columnName); err != nil {
			return 0, err
		}
	}
	for _, record := range records {
		if len(record) != len(columnNames) {
			return 0, fmt.Errorf(
				"length conflict: len(columnNames) = %d, len(record) = %d",
				len(columnNames), len(record))
		}
	}

	if len(db.groongaDBs) == 1 {
		return db.groongaDBs[0].load(tableName, columnNames, records)
	} else {
		return db.loadN(tableName, columnNames, records)
	}
}

func (db *DB) LoadMap(
	tableName string, recordMaps []map[string]Valuer) (int, error) {
	// Check arguments.
	if err := db.checkTableName(tableName); err != nil {
		return 0, err
	}
	for _, recordMap := range recordMaps {
		for columnName, _ := range recordMap {
			if err := db.checkColumnName(columnName); err != nil {
				return 0, err
			}
		}
	}

	if len(db.groongaDBs) == 1 {
		return db.groongaDBs[0].loadMap(tableName, recordMaps)
	} else {
		return db.loadMapN(tableName, recordMaps)
	}
}

func (db *DB) LoadC(
	tableName string, columnNames []string,
	columnarRecords []interface{}) (int, error) {
	// Check arguments.
	if err := db.checkTableName(tableName); err != nil {
		return 0, err
	}
	for _, columnName := range columnNames {
		if err := db.checkColumnName(columnName); err != nil {
			return 0, err
		}
	}
	if len(columnNames) != len(columnarRecords) {
		return 0, fmt.Errorf(
			"length conflict: len(columnNames) = %d, len(columnarRecords) = %d",
			len(columnNames), len(columnarRecords))
	}

	if len(db.groongaDBs) == 1 {
		return db.groongaDBs[0].loadC(tableName, columnNames, columnarRecords)
	} else {
		return db.loadCN(tableName, columnNames, columnarRecords)
	}
}

func (db *DB) LoadCMap(
	tableName string, columnarRecordsMap map[string]interface{}) (int, error) {
	// Check arguments.
	if err := db.checkTableName(tableName); err != nil {
		return 0, err
	}
	for columnName, _ := range columnarRecordsMap {
		if err := db.checkColumnName(columnName); err != nil {
			return 0, err
		}
	}

	if len(db.groongaDBs) == 1 {
		return db.groongaDBs[0].loadCMap(tableName, columnarRecordsMap)
	} else {
		return db.loadCMapN(tableName, columnarRecordsMap)
	}
}
