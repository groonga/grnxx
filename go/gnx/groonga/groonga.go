package groonga

/*
#cgo pkg-config: groonga
#include <groonga.h>
#include <stdlib.h>
*/
import "C"

import "encoding/json"
import "fmt"
import "io/ioutil"
import "os"
import "strconv"
import "strings"
import "time"
import "unicode"
import "unsafe"

var initCount = 0

func DisableInitCount() {
	initCount = -1
}

func Init() error {
	switch initCount {
	case -1: // Disabled.
		return nil
	case 0:
		if rc := C.grn_init(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_init() failed: rc = %d", rc)
		}
	}
	initCount++
	return nil
}

func Fin() error {
	switch initCount {
	case -1: // Disabled.
		return nil
	case 0:
		return fmt.Errorf("Groonga is not initialized yet")
	case 1:
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_fin() failed: rc = %d", rc)
		}
	}
	initCount--
	return nil
}

type DB struct {
	ctx *C.grn_ctx
}

func CreateDB(path string) (*DB, error) {
	if err := Init(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(0)
	if ctx == nil {
		Fin()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if db := C.grn_db_create(ctx, cPath, nil); db == nil {
		C.grn_ctx_close(ctx)
		Fin()
		message := C.GoString(&ctx.errbuf[0])
		return nil, fmt.Errorf("grn_db_create() failed: err = %s", message)
	}
	return &DB{ctx}, nil
}

// Create a DB in a temporary directory.
//
// Example:
// db, dir, err := CreateTempDB("", "gnx")
// if err != nil {
//   log.Fatalln(err)
// }
// defer os.RemoveAll(dir)
// ...
func CreateTempDB(dir, prefix string) (*DB, string, error) {
	tempDir, err := ioutil.TempDir(dir, prefix)
	if err != nil {
		return nil, "", err
	}
	db, err := CreateDB(tempDir + "/db")
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, "", err
	}
	return db, tempDir, nil
}

func OpenDB(path string) (*DB, error) {
	if err := Init(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(0)
	if ctx == nil {
		Fin()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if db := C.grn_db_open(ctx, cPath); db == nil {
		C.grn_ctx_close(ctx)
		Fin()
		message := C.GoString(&ctx.errbuf[0])
		return nil, fmt.Errorf("grn_db_create() failed: err = %s", message)
	}
	return &DB{ctx}, nil
}

func (db *DB) Close() error {
	if db.ctx == nil {
		return nil
	}
	if rc := C.grn_ctx_close(db.ctx); rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_ctx_close() failed: rc = %d", rc)
	}
	db.ctx = nil
	Fin() // may fail
	return nil
}

func (db *DB) Send(command string) error {
	cCommand := C.CString(command)
	defer C.free(unsafe.Pointer(cCommand))
	rc := C.grn_ctx_send(db.ctx, cCommand, C.uint(len(command)), 0)
	if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
		message := C.GoString(&db.ctx.errbuf[0])
		return fmt.Errorf(
			"grn_ctx_send() failed: rc = %d, ctx.rc = %d, err = %s",
			rc, db.ctx.rc, message)
	}
	return nil
}

func (db *DB) Recv() ([]byte, error) {
	var resultBuffer *C.char
	var resultLength C.uint
	var flags C.int
	rc := C.grn_ctx_recv(db.ctx, &resultBuffer, &resultLength, &flags)
	if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
		message := C.GoString(&db.ctx.errbuf[0])
		return nil, fmt.Errorf(
			"grn_ctx_recv() failed: rc = %d, ctx.rc = %d, err = %s",
			rc, db.ctx.rc, message)
	}
	result := C.GoBytes(unsafe.Pointer(resultBuffer), C.int(resultLength))
	return result, nil
}

func (db *DB) Query(command string) ([]byte, error) {
	if err := db.Send(command); err != nil {
		db.Recv()
		return nil, err
	}
	return db.Recv()
}

func (db *DB) SendEx(name string, options map[string]string) error {
	if (len(name) == 0) || (strings.IndexFunc(name, unicode.IsSpace) != -1) {
		return fmt.Errorf("invalid command name")
	}
	command := name
	for key, value := range options {
		if (len(key) == 0) || (strings.IndexFunc(key, unicode.IsSpace) != -1) {
			return fmt.Errorf("invalid option key")
		}
		value = strings.Replace(value, "\\", "\\\\", -1)
		value = strings.Replace(value, "'", "\\'", -1)
		command += fmt.Sprintf(" --%s '%s'", key, value)
	}
	return db.Send(command)
}

func (db *DB) QueryEx(name string, options map[string]string) (
	[]byte, error) {
	if err := db.SendEx(name, options); err != nil {
		return nil, err
	}
	return db.Recv()
}

func (db *DB) boolQueryEx(name string, options map[string]string) (
	bool, error) {
	bytes, err := db.QueryEx(name, options)
	if err != nil {
		return false, err
	}
	return string(bytes) == "true", nil
}

func (db *DB) intQueryEx(name string, options map[string]string) (
	int, error) {
	bytes, err := db.QueryEx(name, options)
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(string(bytes))
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (db *DB) CacheLimit(max int) (int, error) {
	if max < 0 {
		return db.intQueryEx("cache_limit", nil)
	}
	return db.intQueryEx("cache_limit", map[string]string{
		"max": strconv.Itoa(max)})
}

func (db *DB) Check(objName string) ([]byte, error) {
	return db.QueryEx("check", map[string]string{"obj": objName})
}

func (db *DB) CreateColumn(options map[string]string) (bool, error) {
	return db.boolQueryEx("column_create", options)
}

func parseStringOrNil(value interface{}) string {
	if result, ok := value.(string); ok {
		return result
	}
	return ""
}

type Column struct {
	ID     int
	Name   string
	Path   string
	Type   string
	Flags  string
	Domain string
	Range  string
	Source string
}

func (db *DB) ColumnList(tableName string) ([]Column, error) {
	bytes, err := db.QueryEx("column_list", map[string]string{
		"table": tableName})
	if err != nil {
		return nil, err
	}
	var result [][]interface{}
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	headers := make([]string, len(result[0]))
	for i := 0; i < len(headers); i++ {
		headers[i] = result[0][i].([]interface{})[0].(string)
	}
	columns := make([]Column, len(result)-1)
	for i := 0; i < len(columns); i++ {
		values := result[i+1]
		for j := 0; j < len(values); j++ {
			switch headers[j] {
			case "id":
				columns[i].ID = int(values[j].(float64))
			case "name":
				columns[i].Name = parseStringOrNil(values[j])
			case "path":
				columns[i].Path = parseStringOrNil(values[j])
			case "type":
				columns[i].Type = parseStringOrNil(values[j])
			case "flags":
				columns[i].Flags = parseStringOrNil(values[j])
			case "domain":
				columns[i].Domain = parseStringOrNil(values[j])
			case "range":
				columns[i].Range = parseStringOrNil(values[j])
			case "source":
				columns[i].Source = parseStringOrNil(values[j])
			}
		}
	}
	return columns, nil
}

func (db *DB) RemoveColumn(tableName, columnName string) (bool, error) {
	return db.boolQueryEx("column_remove",
		map[string]string{"table": tableName, "name": columnName})
}

func (db *DB) RenameColumn(tableName, columnName, newColumnName string) (
	bool, error) {
	return db.boolQueryEx("column_rename", map[string]string{
		"table": tableName, "name": columnName, "new_name": newColumnName})
}

func (db *DB) DefineSelector(options map[string]string) (bool, error) {
	return db.boolQueryEx("define_selector", options)
}

func (db *DB) Defrag(objName string, threshold int) (int, error) {
	return db.intQueryEx("defrag", map[string]string{
		"objname": objName, "threshold": strconv.Itoa(threshold)})
}

func (db *DB) Delete(options map[string]string) (bool, error) {
	return db.boolQueryEx("delete", options)
}

func (db *DB) Dump(tableNames []string) ([]byte, error) {
	if len(tableNames) == 0 {
		return db.QueryEx("dump", nil)
	}
	joinedTableNames := strings.Join(tableNames, ",")
	return db.QueryEx("dump", map[string]string{"tables": joinedTableNames})
}

func (db *DB) Load(options map[string]string) (int, error) {
	return db.intQueryEx("load", options)
}

func (db *DB) ClearLock(options map[string]string) (bool, error) {
	return db.boolQueryEx("lock_clear", options)
}

func (db *DB) SetLogLevel(level string) (bool, error) {
	return db.boolQueryEx("log_level", map[string]string{"level": level})
}

func (db *DB) PutLog(level, message string) (bool, error) {
	return db.boolQueryEx("log_put", map[string]string{
		"level": level, "message": message})
}

func (db *DB) ReopenLog() (bool, error) {
	return db.boolQueryEx("log_reopen", nil)
}

func (db *DB) LogicalCount(options map[string]string) (int, error) {
	return db.intQueryEx("logical_count", options)
}

func (db *DB) Normalize(normalizer, target string, flags []string) (
	string, error) {
	options := map[string]string{"normalizer": normalizer, "string": target}
	if len(flags) != 0 {
		options["flags"] = strings.Join(flags, "|")
	}
	bytes, err := db.QueryEx("normalize", options)
	if err != nil {
		return "", err
	}
	var result map[string]interface{}
	if err = json.Unmarshal(bytes, &result); err != nil {
		return "", err
	}
	return result["normalized"].(string), nil
}

func (db *DB) NormalizerList() ([]string, error) {
	bytes, err := db.QueryEx("normalizer_list", nil)
	if err != nil {
		return nil, err
	}
	var result []map[string]interface{}
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	normalizers := make([]string, len(result))
	for i, normalizer := range result {
		normalizers[i] = normalizer["name"].(string)
	}
	return normalizers, nil
}

func (db *DB) Quit() error {
	_, err := db.QueryEx("log_reopen", nil)
	return err
}

// TODO: range_filter

func (db *DB) Register(path string) (bool, error) {
	return db.boolQueryEx("register", map[string]string{"path": path})
}

// TODO
func (db *DB) CalcelRequest(id string) ([]byte, error) {
	return db.QueryEx("request_cancel", map[string]string{"id": id})
}

// TODO
func (db *DB) EvalRuby(script string) ([]byte, error) {
	return db.QueryEx("ruby_eval", map[string]string{"script": script})
}

// TODO
func (db *DB) LoadRuby(path string) ([]byte, error) {
	return db.QueryEx("ruby_load", map[string]string{"path": path})
}

type OutputColumn struct {
	Name string
	Type string
	// []bool, [](u)int8/16/32/64, []float64, []time.Time, []string, or ???.
	// TODO: Support GeoPoint.
	Values interface{}
}

func (output *OutputColumn) Count() int {
	switch values := output.Values.(type) {
		case []bool:
			return len(values)
		case []int8:
			return len(values)
		case []int32:
			return len(values)
		case []int64:
			return len(values)
		case []uint8:
			return len(values)
		case []uint16:
			return len(values)
		case []uint32:
			return len(values)
		case []uint64:
			return len(values)
		case []float64:
			return len(values)
		case []time.Time:
			return len(values)
		case []string:
			return len(values)
		default:
			return 0
	}
}

func (db *DB) Select(options map[string]string) ([]OutputColumn, error) {
	bytes, err := db.QueryEx("select", options)
	if err != nil {
		return nil, err
	}
	var result [][][]interface{}
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	headers := result[0][1]
	outputColumns := make([]OutputColumn, len(headers))
	for i := 0; i < len(headers); i++ {
		header := headers[i].([]interface{})
		outputColumns[i].Name = header[0].(string)
		outputColumns[i].Type = header[1].(string)
	}
	records := result[0][2:]
	for i := 0; i < len(outputColumns); i++ {
		switch outputColumns[i].Type {
		case "Bool":
			values := make([]bool, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = records[j][i].(bool)
			}
			outputColumns[i].Values = values
		case "Int8":
			values := make([]int8, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = int8(records[j][i].(float64))
			}
			outputColumns[i].Values = values
		case "Int16":
			values := make([]int16, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = int16(records[j][i].(float64))
			}
			outputColumns[i].Values = values
		case "Int32":
			values := make([]int32, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = int32(records[j][i].(float64))
			}
			outputColumns[i].Values = values
		case "Int64":
			values := make([]int64, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = int64(records[j][i].(float64))
			}
			outputColumns[i].Values = values
		case "UInt8":
			values := make([]uint8, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = uint8(records[j][i].(float64))
			}
			outputColumns[i].Values = values
		case "UInt16":
			values := make([]uint16, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = uint16(records[j][i].(float64))
			}
			outputColumns[i].Values = values
		case "UInt32":
			values := make([]uint32, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = uint32(records[j][i].(float64))
			}
			outputColumns[i].Values = values
		case "UInt64":
			values := make([]uint64, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = uint64(records[j][i].(float64))
			}
			outputColumns[i].Values = values
		case "Float":
			values := make([]float64, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = records[j][i].(float64)
			}
			outputColumns[i].Values = values
		case "Time":
			values := make([]time.Time, len(records))
			for j := 0; j < len(records); j++ {
				microSecondsSinceEpoch := int64(records[j][i].(float64) * 1000000.0)
				values[j] = time.Unix(microSecondsSinceEpoch/1000000,
					microSecondsSinceEpoch%1000000)
			}
			outputColumns[i].Values = values
		case "ShortText", "Text", "LongText":
			values := make([]string, len(records))
			for j := 0; j < len(records); j++ {
				values[j] = records[j][i].(string)
			}
			outputColumns[i].Values = values
		case "TokyoGeoPoint": // TODO
			return nil, fmt.Errorf("not supported type")
		case "WGS84GeoPoint": // TODO
			return nil, fmt.Errorf("not supported type")
		}
	}
	return outputColumns, nil
}

func (db *DB) Shutdown() (bool, error) {
	return db.boolQueryEx("shutdown", nil)
}

func (db *DB) Status() (map[string]interface{}, error) {
	bytes, err := db.QueryEx("status", nil)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	return result, nil
}

type Candidate struct {
	Query string
	Score float64
}

func (db *DB) Suggest(options map[string]string) (
	map[string][]Candidate, error) {
	bytes, err := db.QueryEx("suggest", options)
	if err != nil {
		return nil, err
	}
	var result map[string][][]interface{}
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	suggestResult := make(map[string][]Candidate, len(result))
	for key, value := range result {
		var candidates []Candidate
		if len(value) > 2 {
			value = value[2:]
			candidates = make([]Candidate, len(value))
			for i := 0; i < len(value); i++ {
				candidates[i].Query = value[i][0].(string)
				candidates[i].Score = value[i][1].(float64)
			}
		}
		suggestResult[key] = candidates
	}
	return suggestResult, nil
}

func (db *DB) CreateTable(options map[string]string) (bool, error) {
	bytes, err := db.QueryEx("table_create", options)
	if err != nil {
		return false, err
	}
	return string(bytes) == "true", nil
}

type Table struct {
	ID               int
	Name             string
	Path             string
	Flags            string
	Domain           string
	Range            string
	DefaultTokenizer string
	Normalizer       string
}

func (db *DB) TableList() ([]Table, error) {
	bytes, err := db.QueryEx("table_list", nil)
	if err != nil {
		return nil, err
	}
	var result [][]interface{}
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	headers := make([]string, len(result[0]))
	for i := 0; i < len(headers); i++ {
		headers[i] = result[0][i].([]interface{})[0].(string)
	}
	tables := make([]Table, len(result)-1)
	for i := 0; i < len(tables); i++ {
		values := result[i+1]
		for j := 0; j < len(values); j++ {
			switch headers[j] {
			case "id":
				tables[i].ID = int(values[j].(float64))
			case "name":
				tables[i].Name = parseStringOrNil(values[j])
			case "path":
				tables[i].Path = parseStringOrNil(values[j])
			case "flags":
				tables[i].Flags = parseStringOrNil(values[j])
			case "domain":
				tables[i].Domain = parseStringOrNil(values[j])
			case "range":
				tables[i].Range = parseStringOrNil(values[j])
			case "default_tokenizer":
				tables[i].DefaultTokenizer = parseStringOrNil(values[j])
			case "normalizer":
				tables[i].Normalizer = parseStringOrNil(values[j])
			}
		}
	}
	return tables, nil
}

func (db *DB) RemoveTable(tableName string) (bool, error) {
	return db.boolQueryEx("table_remove", map[string]string{"name": tableName})
}

type Token struct {
	Value    string
	Position int
}

func (db *DB) TableTokenize(options map[string]string) ([]Token, error) {
	bytes, err := db.QueryEx("table_tokenize", options)
	if err != nil {
		return nil, err
	}
	var result []Token
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (db *DB) Tokenize(options map[string]string) ([]Token, error) {
	bytes, err := db.QueryEx("tokenize", options)
	if err != nil {
		return nil, err
	}
	var result []Token
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (db *DB) TokenizerList() ([]string, error) {
	bytes, err := db.QueryEx("tokenizer_list", nil)
	if err != nil {
		return nil, err
	}
	var result []map[string]string
	if err = json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	tokenizers := make([]string, len(result))
	for i := 0; i < len(result); i++ {
		tokenizers[i] = result[i]["name"]
	}
	return tokenizers, nil
}

func (db *DB) Truncate(targetName string) (bool, error) {
	return db.boolQueryEx("truncate", map[string]string{
		"target_name": targetName})
}

func (db *DB) tokenizeCommand(s string) (string, string, bool, error) {
	s = strings.TrimLeftFunc(s, unicode.IsSpace)
	if len(s) == 0 {
		return "", "", false, nil
	}
	switch s[0] {
	case '#':
		return "", "", false, nil
	case '\'', '"':
		quote := s[0]
		pos := 1
		hasBackslash := false
		for pos < len(s) {
			if s[pos] == quote {
				break
			} else if s[pos] == '\\' {
				hasBackslash = true
				pos++
			}
			pos++
		}
		if pos >= len(s) {
			return "", "", false, fmt.Errorf("quote missing")
		}
		token := s[1:pos]
		if hasBackslash {
			bytes := make([]byte, len(token))
			count := 0
			for i := 0; i < len(token); i++ {
				if token[i] == '\\' {
					i++
				}
				bytes[count] = token[i]
				count++
			}
			token = string(bytes[:count])
		}
		return token, s[pos+1:], true, nil
	default:
		pos := strings.IndexFunc(s, unicode.IsSpace)
		if pos == -1 {
			pos = len(s)
		}
		return s[0:pos], s[pos:], true, nil
	}
}

func (db *DB) tokenizeCommandAll(command string) ([]string, error) {
	var tokens []string
	s := command
	for {
		token, rest, ok, err := db.tokenizeCommand(s)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		tokens = append(tokens, token)
		s = rest
	}
	return tokens, nil
}

var commandOptionKeys = map[string][]string{
	"cache_limit":   {"max"},
	"check":         {"obj"},
	"column_create": {"table", "name", "flags", "type", "source"},
	"column_list":   {"table"},
	"column_remove": {"table", "name"},
	"column_rename": {"table", "name", "new_name"},
	"define_selector": {
		"name", "table", "match_columns", "query", "filter", "scorer", "sortby",
		"output_columns", "offset", "limit", "drilldown", "drilldown_sortby",
		"drilldown_output_columns", "drilldown_offset", "drilldown_limit"},
	"defrag":     {"objname", "threshold"},
	"delete":     {"table", "key", "id", "filter"},
	"dump":       {"tables"},
	"load":       {"values", "table", "columns", "ifexists", "input_type"},
	"lock_clear": {"target_name"},
	"log_level":  {"level"},
	"log_put":    {"level", "message"},
	"log_reopen": {},
	"logical_count": {
		"logical_table", "shared_key", "min", "min_border", "max", "max_border",
		"filter"},
	"normalize":       {"normalizer", "string", "flags"},
	"normalizer_list": {},
	"quit":            {},
	//	"range_filter": {},
	"register":       {"path"},
	"request_cancel": {"id"},
	"ruby_eval":      {"script"},
	"ruby_load":      {"path"},
	"select": {
		"table", "match_columns", "query", "filter", "scorer", "sortby",
		"output_columns", "offset", "limit", "drilldown", "drilldown_sortby",
		"drilldown_output_columns", "drilldown_offset", "drilldown_limit",
		"cache", "match_escalation_threshold", "query_expansion", "query_flags",
		"query_expander", "adjuster", "drilldown_calc_types",
		"drilldown_calc_target"},
	"shutdown": {},
	"status":   {},
	"suggest": {"types", "table", "column", "query", "sortby", "output_columns",
		"offset", "limit", "frequency_threshold",
		"conditional_probability_threshold", "prefix_search"},
	"table_create": {
		"name", "flag", "key_type", "value_type", "default_tokenizer",
		"normalizer", "token_filters"},
	"table_list":     {},
	"table_remove":   {"name"},
	"table_tokenize": {"table", "string", "flags", "mode"},
	"tokenize": {
		"tokenizer", "string", "normalizer", "flags", "mode", "token_filters"},
	"tokenizer_list": {},
	"truncate":       {"target_name"}}

func (db *DB) parseCommandOptions(name string, tokens []string) (
	map[string]string, error) {
	args := make([]string, 0)
	options := make(map[string]string)
	for i := 0; i < len(tokens); i++ {
		if strings.HasPrefix(tokens[i], "--") {
			key := tokens[i][2:]
			i++
			if i >= len(tokens) {
				return nil, fmt.Errorf("option argument missing")
			}
			options[key] = tokens[i]
		} else {
			args = append(args, tokens[i])
		}
	}
	keys := commandOptionKeys[name]
	end := len(keys)
	if end > len(args) {
		end = len(args)
	}
	for i := 0; i < end; i++ {
		options[keys[i]] = args[i]
	}
	return options, nil
}

func (db *DB) ParseCommand(command string) (string, map[string]string, error) {
	tokens, err := db.tokenizeCommandAll(command)
	if err != nil {
		return "", nil, err
	}
	if len(tokens) == 0 {
		return "", nil, nil
	}
	// Parse tokens.
	name := tokens[0]
	options, err := db.parseCommandOptions(name, tokens[1:])
	if err != nil {
		return "", nil, err
	}
	return name, options, nil
}
