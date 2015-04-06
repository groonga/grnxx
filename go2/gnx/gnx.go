package gnx

/*
#cgo CXXFLAGS: -std=c++11
#cgo LDFLAGS: -lgrnxx -lstdc++
#cgo pkg-config: groonga
#include <groonga.h>
#include <stdlib.h>
#include "gnx.h"
*/
import "C"

import (
	"encoding/binary"
	"encoding/json" // FIXME
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"math"
	"math/rand"
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

// -- Common --

func countColumnarRecords(columnarRecords []interface{}) (int, error) {
	numRecords := -1
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
		default:
			return 0, fmt.Errorf("unsupported data type")
		}
		if numRecords == -1 {
			numRecords = thisLen
		} else if thisLen != numRecords {
			return 0, fmt.Errorf("length conflict: numRecords = %d, thisLen = %d",
				numRecords, thisLen)
		}
	}
	if numRecords <= 0 {
		return 0, fmt.Errorf("no input")
	}
	return numRecords, nil
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
//	 log.Fatalln(err)
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

func (db *GroongaDB) checkCommandName(name string) error {
	if name == "" {
		return fmt.Errorf("empty command name")
	}
	for _, r := range name {
		if (r != '_') || ((r < 'a') && (r > 'z')) {
			return fmt.Errorf("invalid rune in command name")
		}
	}
	return nil
}

func (db *GroongaDB) checkOptionKey(name string) error {
	if name == "" {
		return fmt.Errorf("empty option key")
	}
	for _, r := range name {
		if (r != '_') || ((r < 'a') && (r > 'z')) {
			return fmt.Errorf("invalid rune in option key")
		}
	}
	return nil
}

func (db *GroongaDB) SendEx(name string, options map[string]string) error {
	if err := db.checkCommandName(name); err != nil {
		return err
	}
	parts := []string{name}
	for key, value := range options {
		if err := db.checkOptionKey(key); err != nil {
			return err
		}
		value = strings.Replace(value, "\\", "\\\\", -1)
		value = strings.Replace(value, "'", "\\'", -1)
		parts = append(parts, fmt.Sprintf("--%s '%s'", key, value))
	}
	command := strings.Join(parts, " ")
	return db.Send(command)
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

func (db *GroongaDB) QueryEx(
	name string, options map[string]string) ([]byte, error) {
	err := db.SendEx(name, options)
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
	numRecords, err := countColumnarRecords(columnarRecords)
	if err != nil {
		return 0, err
	}

	// TODO: Use cgo for JSON encoding.
	records := make([][]Valuer, numRecords)
	for i := 0; i < numRecords; i++ {
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
		default:
			return 0, fmt.Errorf("unsupported data type")
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

func (db *DB) GroongaQueryEx(i int, name string, options map[string]string) (
	[]byte, error) {
	if (i < 0) || (i >= len(db.groongaDBs)) {
		return nil, fmt.Errorf("invalid DB: i = %d", i)
	}
	return db.groongaDBs[i].QueryEx(name, options)
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

func (db *DB) hashInt(value Int) int {
	hasher := fnv.New32a()
	binary.Write(hasher, binary.LittleEndian, value)
	return int(hasher.Sum32())
}

func (db *DB) hashFloat(value Float) int {
	hasher := fnv.New32a()
	binary.Write(hasher, binary.LittleEndian, value)
	return int(hasher.Sum32())
}

func (db *DB) hashText(value Text) int {
	hasher := fnv.New32a()
	hasher.Write([]byte(value))
	return int(hasher.Sum32())
}

func (db *DB) selectGroongaDB(key Valuer) (int, error) {
	switch value := key.(type) {
	case nil:
		return rand.Int() % len(db.groongaDBs), nil
	case Int:
		return db.hashInt(value) % len(db.groongaDBs), nil
	case Float:
		return db.hashFloat(value) % len(db.groongaDBs), nil
	case Text:
		return db.hashText(value) % len(db.groongaDBs), nil
	}
	return -1, fmt.Errorf("unsupported key type")
}

func (db *DB) load(
	tableName string, columnNames []string, records [][]Valuer) (int, error) {
	idID := -1
	keyID := -1
	for i, columnName := range columnNames {
		switch columnName {
		case "_id":
			if idID != -1 {
				return 0, fmt.Errorf("_id appears more than once")
			}
			idID = i
		case "_key":
			if keyID != -1 {
				return 0, fmt.Errorf("_key appears more than once")
			}
			keyID = i
		}
	}
	if (idID != -1) && (keyID != -1) {
		return 0, fmt.Errorf("both _id and _key appear")
	}

	recordsPerDBs := make([][][]Valuer, len(db.groongaDBs))
	switch {
	case idID != -1:
		for _, record := range records {
			rowID := record[idID].(Int)
			dbID := int(rowID - 1) % len(db.groongaDBs)
			dummyRecord := make([]Valuer, len(record))
			copy(dummyRecord, record)
			dummyRecord[idID] = Int((int(rowID - 1) / len(db.groongaDBs)) + 1)
			recordsPerDBs[dbID] = append(recordsPerDBs[dbID], dummyRecord)
		}
	case keyID != -1:
		for _, record := range records {
			dbID, err := db.selectGroongaDB(record[keyID])
			if err != nil {
				return 0, err
			}
			recordsPerDBs[dbID] = append(recordsPerDBs[dbID], record)
		}
	default:
		for _, record := range records {
			dbID, _ := db.selectGroongaDB(nil)
			recordsPerDBs[dbID] = append(recordsPerDBs[dbID], record)
		}
	}

	// TODO: Parallel processing.
	total := 0
	for i, recordsPerDB := range recordsPerDBs {
		if len(recordsPerDB) != 0 {
			count, err := db.groongaDBs[i].load(tableName, columnNames, recordsPerDB)
			if err != nil {
				return total, err
			}
			total += count
		}
	}
	return total, nil
}

func (db *DB) loadMap(
	tableName string, recordMaps []map[string]Valuer) (int, error) {
	recordMapsPerDBs := make([][]map[string]Valuer, len(db.groongaDBs))
	for _, recordMap := range recordMaps {
		rowIDValue, hasRowID := recordMap["_id"]
		keyValue, hasKey := recordMap["_key"]
		if hasRowID && hasKey {
			return 0, fmt.Errorf("both _id and _key appear")
		}
		switch {
		case hasRowID:
			rowID := rowIDValue.(Int)
			dbID := int(rowID - 1) % len(db.groongaDBs)
			dummyRecordMap := make(map[string]Valuer)
			for key, value := range recordMap {
				if key == "_id" {
					dummyRecordMap[key] = Int((int(rowID - 1) / len(db.groongaDBs)) + 1)
				} else {
					dummyRecordMap[key] = value
				}
			}
			recordMapsPerDBs[dbID] = append(recordMapsPerDBs[dbID], dummyRecordMap)
		case hasKey:
			dbID, err := db.selectGroongaDB(keyValue)
			if err != nil {
				return 0, err
			}
			recordMapsPerDBs[dbID] = append(recordMapsPerDBs[dbID], recordMap)
		default:
			dbID, _ := db.selectGroongaDB(nil)
			recordMapsPerDBs[dbID] = append(recordMapsPerDBs[dbID], recordMap)
		}
	}

	// TODO: Parallel processing.
	total := 0
	for i, recordMapsPerDB := range recordMapsPerDBs {
		if len(recordMapsPerDB) != 0 {
			count, err := db.groongaDBs[i].loadMap(tableName, recordMapsPerDB)
			if err != nil {
				return total, err
			}
			total += count
		}
	}
	return total, nil
}

func (db *DB) loadC(
	tableName string, columnNames []string,
	columnarRecords []interface{}) (int, error) {
	numRecords, err := countColumnarRecords(columnarRecords)
	if err != nil {
		return 0, err
	}

	idID := -1
	keyID := -1
	for i, columnName := range columnNames {
		switch columnName {
		case "_id":
			if idID != -1 {
				return 0, fmt.Errorf("_id appears more than once")
			}
			idID = i
		case "_key":
			if keyID != -1 {
				return 0, fmt.Errorf("_key appears more than once")
			}
			keyID = i
		}
	}
	if (idID != -1) && (keyID != -1) {
		return 0, fmt.Errorf("both _id and _key appear")
	}

	dbIDs := make([]int, numRecords)
	numRecordsPerDBs := make([]int, len(db.groongaDBs))
	switch {
	case idID != -1:
		rowIDs := columnarRecords[idID].([]Int)
		dummyRowIDs := make([]Int, len(rowIDs))
		for i := 0; i < numRecords; i++ {
			dbIDs[i] = int(rowIDs[i] - 1) % len(db.groongaDBs)
			dummyRowIDs[i] = Int((int(rowIDs[i] - 1) / len(db.groongaDBs)) + 1)
			numRecordsPerDBs[dbIDs[i]]++
		}
		dummyColumnarRecords := make([]interface{}, len(columnarRecords))
		copy(dummyColumnarRecords, columnarRecords)
		dummyColumnarRecords[idID] = dummyRowIDs
		columnarRecords = dummyColumnarRecords
	case keyID != -1:
		switch keys := columnarRecords[keyID].(type) {
		case []Int:
			for i := 0; i < numRecords; i++ {
				dbIDs[i] = db.hashInt(keys[i]) % len(db.groongaDBs)
				numRecordsPerDBs[dbIDs[i]]++
			}
		case []Float:
			for i := 0; i < numRecords; i++ {
				dbIDs[i] = db.hashFloat(keys[i]) % len(db.groongaDBs)
				numRecordsPerDBs[dbIDs[i]]++
			}
		case []Text:
			for i := 0; i < numRecords; i++ {
				dbIDs[i] = db.hashText(keys[i]) % len(db.groongaDBs)
				numRecordsPerDBs[dbIDs[i]]++
			}
		default:
			return 0, fmt.Errorf("unsupported key type")
		}
	default:
		for i := 0; i < numRecords; i++ {
			dbIDs[i] = rand.Int() % len(db.groongaDBs)
			numRecordsPerDBs[dbIDs[i]]++
		}
	}

	columnarRecordsPerDBs := make([][]interface{}, len(db.groongaDBs))
	for i := 0; i < len(db.groongaDBs); i++ {
		columnarRecordsPerDBs[i] = make([]interface{}, len(columnarRecords))
	}
	for i, columnarValues := range columnarRecords {
		switch values := columnarValues.(type) {
		case []Bool:
			valuesPerDBs := make([][]Bool, len(db.groongaDBs))
			for j, value := range values {
				dbID := dbIDs[j]
				valuesPerDBs[dbID] = append(valuesPerDBs[dbID], value)
			}
			for j := 0; j < len(db.groongaDBs); j++ {
				columnarRecordsPerDBs[j][i] = valuesPerDBs[j]
			}
		case []Int:
			valuesPerDBs := make([][]Int, len(db.groongaDBs))
			for j, value := range values {
				dbID := dbIDs[j]
				valuesPerDBs[dbID] = append(valuesPerDBs[dbID], value)
			}
			for j := 0; j < len(db.groongaDBs); j++ {
				columnarRecordsPerDBs[j][i] = valuesPerDBs[j]
			}
		case []Float:
			valuesPerDBs := make([][]Float, len(db.groongaDBs))
			for j, value := range values {
				dbID := dbIDs[j]
				valuesPerDBs[dbID] = append(valuesPerDBs[dbID], value)
			}
			for j := 0; j < len(db.groongaDBs); j++ {
				columnarRecordsPerDBs[j][i] = valuesPerDBs[j]
			}
		case []Text:
			valuesPerDBs := make([][]Text, len(db.groongaDBs))
			for j, value := range values {
				dbID := dbIDs[j]
				valuesPerDBs[dbID] = append(valuesPerDBs[dbID], value)
			}
			for j := 0; j < len(db.groongaDBs); j++ {
				columnarRecordsPerDBs[j][i] = valuesPerDBs[j]
			}
		case []BoolVector:
			valuesPerDBs := make([][]BoolVector, len(db.groongaDBs))
			for j, value := range values {
				dbID := dbIDs[j]
				valuesPerDBs[dbID] = append(valuesPerDBs[dbID], value)
			}
			for j := 0; j < len(db.groongaDBs); j++ {
				columnarRecordsPerDBs[j][i] = valuesPerDBs[j]
			}
		case []IntVector:
			valuesPerDBs := make([][]IntVector, len(db.groongaDBs))
			for j, value := range values {
				dbID := dbIDs[j]
				valuesPerDBs[dbID] = append(valuesPerDBs[dbID], value)
			}
			for j := 0; j < len(db.groongaDBs); j++ {
				columnarRecordsPerDBs[j][i] = valuesPerDBs[j]
			}
		case []FloatVector:
			valuesPerDBs := make([][]FloatVector, len(db.groongaDBs))
			for j, value := range values {
				dbID := dbIDs[j]
				valuesPerDBs[dbID] = append(valuesPerDBs[dbID], value)
			}
			for j := 0; j < len(db.groongaDBs); j++ {
				columnarRecordsPerDBs[j][i] = valuesPerDBs[j]
			}
		case []TextVector:
			valuesPerDBs := make([][]TextVector, len(db.groongaDBs))
			for j, value := range values {
				dbID := dbIDs[j]
				valuesPerDBs[dbID] = append(valuesPerDBs[dbID], value)
			}
			for j := 0; j < len(db.groongaDBs); j++ {
				columnarRecordsPerDBs[j][i] = valuesPerDBs[j]
			}
		default:
			return 0, fmt.Errorf("unsupported data type")
		}
	}

	// TODO: Parallel processing.
	total := 0
	for i, columnarRecordsPerDB := range columnarRecordsPerDBs {
		if numRecordsPerDBs[i] != 0 {
			count, err := db.groongaDBs[i].loadC(
				tableName, columnNames, columnarRecordsPerDB)
			if err != nil {
				return total, err
			}
			total += count
		}
	}
	return total, nil
}

func (db *DB) loadCMap(
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

func (db *DB) Load(
	tableName string, columnNames []string, records [][]Valuer) (int, error) {
	// Check arguments.
	if err := db.checkTableName(tableName); err != nil {
		return 0, err
	}
	if (len(columnNames) == 0) || (len(records) == 0) {
		return 0, fmt.Errorf("no input")
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
		return db.load(tableName, columnNames, records)
	}
}

func (db *DB) LoadMap(
	tableName string, recordMaps []map[string]Valuer) (int, error) {
	// Check arguments.
	if err := db.checkTableName(tableName); err != nil {
		return 0, err
	}
	if len(recordMaps) == 0 {
		return 0, fmt.Errorf("no input")
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
		return db.loadMap(tableName, recordMaps)
	}
}

func (db *DB) LoadC(
	tableName string, columnNames []string,
	columnarRecords []interface{}) (int, error) {
	// Check arguments.
	if err := db.checkTableName(tableName); err != nil {
		return 0, err
	}
	if len(columnNames) == 0 {
		return 0, fmt.Errorf("no input")
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
		return db.loadC(tableName, columnNames, columnarRecords)
	}
}

func (db *DB) LoadCMap(
	tableName string, columnarRecordsMap map[string]interface{}) (int, error) {
	// Check arguments.
	if err := db.checkTableName(tableName); err != nil {
		return 0, err
	}
	if len(columnarRecordsMap) == 0 {
		return 0, fmt.Errorf("no input")
	}
	for columnName, _ := range columnarRecordsMap {
		if err := db.checkColumnName(columnName); err != nil {
			return 0, err
		}
	}

	if len(db.groongaDBs) == 1 {
		return db.groongaDBs[0].loadCMap(tableName, columnarRecordsMap)
	} else {
		return db.loadCMap(tableName, columnarRecordsMap)
	}
}

func (db *DB) InsertRow(tableName string, key Valuer) (bool, Int, error) {
	dbID, err := db.selectGroongaDB(key)
	if err != nil {
		return false, NAInt(), err
	}
	groongaDB := db.groongaDBs[dbID]

	var inserted C.gnx_bool
	var rowID C.gnx_int
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))
	switch value := key.(type) {
	case nil:
		inserted = C.gnx_insert_row(
			groongaDB.ctx, cTableName, C.GNX_NA, nil, &rowID)
	case Int:
		inserted = C.gnx_insert_row(
			groongaDB.ctx, cTableName, C.GNX_INT, unsafe.Pointer(&value), &rowID)
	case Float:
		inserted = C.gnx_insert_row(
			groongaDB.ctx, cTableName, C.GNX_FLOAT, unsafe.Pointer(&value), &rowID)
//	case GeoPoint:
	case Text:
		cValue := C.CString(string(value))
		defer C.free(unsafe.Pointer(cValue))
		text := C.gnx_text{cValue, C.gnx_int(len(value))}
		inserted = C.gnx_insert_row(
			groongaDB.ctx, cTableName, C.GNX_TEXT, unsafe.Pointer(&text), &rowID)
	default:
		return false, NAInt(), fmt.Errorf("unsupported key type")
	}
	if inserted == C.GNX_NA_BOOL {
		return false, NAInt(), fmt.Errorf("gnx_insert_row() failed")
	}
	rowID = ((rowID - 1) * C.gnx_int(len(db.groongaDBs))) + C.gnx_int(dbID) + 1
	return inserted == C.GNX_TRUE, Int(rowID), err
}

func (db *DB) SetValue(tableName string, columnName string, rowID Int,
	value Valuer) error {
	dbID := int(rowID - 1) % len(db.groongaDBs)
	rowID = ((rowID - 1) / Int(len(db.groongaDBs))) + 1
	groongaDB := db.groongaDBs[dbID]

	var ok C.gnx_bool
	cTableName := C.CString(tableName)
	defer C.free(unsafe.Pointer(cTableName))
	cColumnName := C.CString(columnName)
	defer C.free(unsafe.Pointer(cColumnName))
	switch v := value.(type) {
	case nil:
		ok = C.gnx_set_value(groongaDB.ctx, cTableName, cColumnName,
			C.gnx_int(rowID), C.GNX_NA, nil)
	case Int:
		ok = C.gnx_set_value(groongaDB.ctx, cTableName, cColumnName,
			C.gnx_int(rowID), C.GNX_INT, unsafe.Pointer(&v))
	case Float:
		ok = C.gnx_set_value(groongaDB.ctx, cTableName, cColumnName,
			C.gnx_int(rowID), C.GNX_FLOAT, unsafe.Pointer(&v))
//	case GeoPoint:
	case Text:
		cValue := C.CString(string(v))
		defer C.free(unsafe.Pointer(cValue))
		text := C.gnx_text{cValue, C.gnx_int(len(v))}
		ok = C.gnx_set_value(groongaDB.ctx, cTableName, cColumnName,
			C.gnx_int(rowID), C.GNX_TEXT, unsafe.Pointer(&text))
	default:
		return fmt.Errorf("unsupported value type")
	}
	if ok != C.GNX_TRUE {
		return fmt.Errorf("gnx_set_value() failed")
	}
	return nil
}
