package gnx

import "./grnxx"
import "./groonga"
import "encoding/binary"
import "encoding/json"
import "fmt"
import "hash/fnv"
import "io/ioutil"
import "math"
import "math/rand"
import "os"
import "runtime"
import "strconv"
import "strings"
import "time"
import "unicode"

type DB struct {
	groongaDBs []*groonga.DB
	grnxxDB *grnxx.DB
}

func CreateDB(path string, n int) (*DB, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid parameter: n = %d", n)
	}
	groongaDBs := make([]*groonga.DB, n)
	for i := 0; i < n; i++ {
		dbPath := path
		if i != 0 {
			dbPath += strconv.Itoa(i)
		}
		groongaDB, err := groonga.CreateDB(dbPath)
		if err != nil {
			for j := 0; j < i; j++ {
				groongaDBs[j].Close()
			}
			return nil, err
		}
		groongaDBs[i] = groongaDB
	}
	return &DB{groongaDBs, nil}, nil
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
	var groongaDBs []*groonga.DB
	for i := 0; ; i++ {
		dbPath := path
		if i != 0 {
			dbPath += strconv.Itoa(i)
		}
		groongaDB, err := groonga.OpenDB(dbPath)
		if err != nil {
			if i == 0 {
				return nil, err
			}
			break
		}
		groongaDBs = append(groongaDBs, groongaDB)
	}
	return &DB{groongaDBs, nil}, nil
}

func (db *DB) Close() error {
	for i := 0; i < len(db.groongaDBs); i++ {
		db.groongaDBs[i].Close()
	}
	if db.grnxxDB != nil {
		db.grnxxDB.Close()
	}
	return nil
}

func (db *DB) gnxLoad(command string) ([]byte, error) {
	_, options, err := db.ParseCommand(command)
	if err != nil {
		return nil, err
	}
	columnsOption, ok := options["columns"]
	if !ok {
		return nil, fmt.Errorf("columns option missing")
	}
	idPos := -1
	keyPos := -1
	var columns []string
	columnNames := strings.Split(columnsOption, ",")
	for i := 0; i < len(columnNames); i++ {
		columnName := strings.TrimFunc(columnNames[i], unicode.IsSpace)
		if len(columnName) != 0 {
			switch columnName {
			case "_id":
				if idPos != -1 {
					return nil, fmt.Errorf("_id appeared more than once")
				}
				idPos = len(columns)
			case "_key":
				if keyPos != -1 {
					return nil, fmt.Errorf("_key appeared more than once")
				}
				keyPos = len(columns)
			}
			columns = append(columns, columnName)
		}
	}
	if (idPos != -1) && (keyPos != -1) {
		return nil, fmt.Errorf("both _id and _key appeared")
	}

	valuesOption, ok := options["values"]
	if !ok {
		return nil, fmt.Errorf("values option missing")
	}
	var mergedValues [][]interface{}
	if err = json.Unmarshal([]byte(valuesOption), &mergedValues); err != nil {
		return nil, err
	}
	values := make([][][]interface{}, len(db.groongaDBs))
	switch {
	case idPos != -1:
		// NOTE: Groonga does not support array-style values with _id.
		for i := 0; i < len(mergedValues); i++ {
			id := int(mergedValues[i][idPos].(float64))
			if id <= 0 {
				return nil, fmt.Errorf("invalid ID: id = %d", id)
			}
			mergedValues[i][idPos] = ((id - 1) / len(values)) + 1
			db := (id - 1) % len(values)
			values[db] = append(values[db], mergedValues[i])
		}
	case keyPos != -1:
		for i := 0; i < len(mergedValues); i++ {
			switch value := mergedValues[i][keyPos].(type) {
			case float64:
				hasher := fnv.New32a()
				err := binary.Write(hasher, binary.LittleEndian, value)
				if err != nil {
					return nil, err
				}
				db := int(hasher.Sum32()) % len(values)
				values[db] = append(values[db], mergedValues[i])
			case string:
				hasher := fnv.New32a()
				if _, err := hasher.Write([]byte(value)); err != nil {
					return nil, err
				}
				db := int(hasher.Sum32()) % len(values)
				values[db] = append(values[db], mergedValues[i])
			default:
				return nil, fmt.Errorf("unsupported key type")
			}
		}
	default:
		for i := 0; i < len(mergedValues); i++ {
			db := rand.Int() % len(values)
			values[db] = append(values[db], mergedValues[i])
		}
	}

	if runtime.GOMAXPROCS(0) == 1 {
		total := 0
		for i := 0; i < len(db.groongaDBs); i++ {
			if len(values[i]) != 0 {
				bytes, err := json.Marshal(values[i])
				if err != nil {
					return nil, err
				}
				options["values"] = string(bytes)
//				fmt.Println("options:", options)
				count, err := db.groongaDBs[i].Load(options)
				if err != nil {
					return nil, err
				}
				total += count
			}
		}
		return []byte(strconv.Itoa(total)), nil
	} else {
		var channels []chan int
		for i := 0; i < len(db.groongaDBs); i++ {
			if len(values[i]) != 0 {
				channel := make(chan int)
				go func(channel chan int, db *groonga.DB, options map[string]string,
					values [][]interface{}) {
					newOptions := make(map[string]string)
					for key, value := range options {
						newOptions[key] = value
					}
					bytes, err := json.Marshal(values)
					if err != nil {
						channel <- 0
						return
					}
					newOptions["values"] = string(bytes)
//					fmt.Println("options:", newOptions)
					count, err := db.Load(newOptions)
					if err != nil {
						channel <- 0
						return
					}
					channel <- count
					close(channel)
					return
				}(channel, db.groongaDBs[i], options, values[i])
				channels = append(channels, channel)
			}
		}
		count := 0
		for _, channel := range channels {
			count += <-channel
		}
		return []byte(strconv.Itoa(count)), nil
	}
}

type Output struct {
	Name string
	Type string
	Values interface{}
}

func (db *DB) groongaSelect(options map[string]string) ([]byte, error) {
	options["cache"] = "no"
	outputColumns, err := db.groongaDBs[0].Select(options)
	if err != nil {
		return nil, err
	}
	if len(outputColumns) == 0 {
		return nil, fmt.Errorf("no output columns")
	}
	for i := 0; i < len(outputColumns); i++ {
		if outputColumns[i].Name == "_id" {
			values := outputColumns[i].Values.([]uint32)
			for j := 0; j < len(values); j++ {
				values[j] = uint32(((int(values[j]) - 1) * len(db.groongaDBs)) + 1)
			}
		}
	}
	for i := 1; i < len(db.groongaDBs); i++ {
		result, err := db.groongaDBs[i].Select(options)
		if err != nil {
			return nil, err
		}
		for j := 0; j < len(result); j++ {
			switch newValues := result[j].Values.(type) {
			case []bool:
				values := outputColumns[j].Values.([]bool)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []int8:
				values := outputColumns[j].Values.([]int8)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []int16:
				values := outputColumns[j].Values.([]int16)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []int32:
				values := outputColumns[j].Values.([]int32)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []int64:
				values := outputColumns[j].Values.([]int64)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []uint8:
				values := outputColumns[j].Values.([]uint8)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []uint16:
				values := outputColumns[j].Values.([]uint16)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []uint32:
				if result[j].Name == "_id" {
					for k := 0; k < len(newValues); k++ {
						newValues[k] =
							uint32(((int(newValues[k]) - 1) * len(db.groongaDBs)) + i + 1)
					}
				}
				values := outputColumns[j].Values.([]uint32)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []uint64:
				values := outputColumns[j].Values.([]uint64)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []float64:
				values := outputColumns[j].Values.([]float64)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []time.Time:
				values := outputColumns[j].Values.([]time.Time)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			case []string:
				values := outputColumns[j].Values.([]string)
				values = append(values, newValues...)
				outputColumns[j].Values = values
			}
		}
	}

	bytes, err := json.Marshal(outputColumns)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

//func (db *DB) groongaSelect(options map[string]string) ([]byte, error) {
//	options["cache"] = "no"
//	count := 0
//	results := make([][][][]interface{}, len(db.groongaDBs))
//	for i := 0; i < len(results); i++ {
//		bytes, err := db.groongaDBs[i].QueryEx("select", options)
//		if err != nil {
//			return nil, err
//		}
//		if err = json.Unmarshal(bytes, &results[i]); err != nil {
//			return nil, err
//		}
//		count += len(results[i][0]) - 2
//	}
//	var ids []int
//	for i := 0; i < len(results[0][0][1]); i++ {
//		name := results[0][0][1][i].([]interface{})[0].(string)
//		if name == "_id" {
//			ids = append(ids, i)
//		}
//	}
//	mergedResult := make([][]interface{}, 1)
//	mergedResult[0] = make([]interface{}, count+2)
//	mergedResult[0][0] = []int{count}
//	mergedResult[0][1] = results[0][0][1]
//	output := mergedResult[0][2:]
//	for i := 0; i < len(results); i++ {
//		input := results[i][0][2:]
//		for j := 0; j < len(input); j++ {
//			output[j] = input[j]
//			for _, idPos := range ids {
//				id := int(input[j][idPos].(float64))
//				input[j][idPos] = ((id - 1) * len(results)) + i + 1
//			}
//		}
//		output = output[len(input):]
//	}
//	bytes, err := json.Marshal(mergedResult)
//	if err != nil {
//		return nil, err
//	}
//	return bytes, nil
//}

func (db *DB) grnxxSelect(options map[string]string) ([]byte, error) {
//	fmt.Println("grnxxSelect: options:", options)
	if db.grnxxDB == nil {
		return nil, fmt.Errorf("grnxx not available")
	}

	table := db.grnxxDB.FindTable(options["table"])
	if table == nil {
		return nil, fmt.Errorf("table not found: name = %s", options["table"])
	}
	builder, err := grnxx.CreatePipelineBuilder(table)
	if err != nil {
		return nil, err
	}
	defer builder.Close()
	cursor, err := table.CreateCursor(nil)
	if err != nil {
		return nil, err
	}
	builder.PushCursor(cursor)

	filterString, ok := options["grnxx_filter"]
	if !ok {
		filterString = "TRUE"
	}
	offset := 0
	offsetString, ok := options["grnxx_offset"]
	if ok {
		offset, err = strconv.Atoi(offsetString)
		if err != nil {
			return nil, err
		}
	}
	limit := math.MaxInt32
	limitString, ok := options["grnxx_limit"]
	if ok {
		limit, err = strconv.Atoi(limitString)
		if err != nil {
			return nil, err
		}
	}
	filter, err := grnxx.ParseExpression(table, filterString)
	if err != nil {
		return nil, err
	}
	if err = builder.PushFilter(filter, offset, limit); err != nil {
		return nil, err
	}

	pipeline, err := builder.Release(nil)
	if err != nil {
		return nil, err
	}
	defer pipeline.Close()
	records, err := pipeline.Flush()
	if err != nil {
		return nil, err
	}
//	fmt.Println("records:", records)

	var outputs []Output
	optionValue, ok := options["grnxx_output_columns"]
	if ok {
		values := strings.Split(optionValue, ",")
		for _, value := range values {
			value = strings.TrimFunc(value, unicode.IsSpace)
			if len(value) != 0 {
				outputs = append(outputs, Output{Name: value})
			}
		}
	} else {
		outputs = append(outputs, Output{Name: "_id"})
		numColumns := table.NumColumns()
		for i := 0; i < numColumns; i++ {
			outputs = append(outputs, Output{Name: table.GetColumn(i).Name()})
		}
	}
//	fmt.Println("outputs:", outputs)

	for i := 0; i < len(outputs); i++ {
		expression, err := grnxx.ParseExpression(table, outputs[i].Name)
		if err != nil {
			return nil, err
		}
		defer expression.Close()

		switch expression.DataType() {
		case grnxx.BOOL:
			outputs[i].Type = "Bool"
			values := make([]grnxx.Bool, len(records))
			if err = expression.Evaluate(records, values); err != nil {
				return nil, err
			}
			outputs[i].Values = values
		case grnxx.INT:
			outputs[i].Type = "Int"
			values := make([]grnxx.Int, len(records))
			if err = expression.Evaluate(records, values); err != nil {
				return nil, err
			}
			outputs[i].Values = values
		case grnxx.FLOAT:
			outputs[i].Type = "Float"
			values := make([]grnxx.Float, len(records))
			if err = expression.Evaluate(records, values); err != nil {
				return nil, err
			}
			outputs[i].Values = values
		case grnxx.TEXT:
			outputs[i].Type = "Text"
			values := make([]grnxx.Text, len(records))
			if err = expression.Evaluate(records, values); err != nil {
				return nil, err
			}
			outputValues := make([]string, len(records))
			for i := 0; i < len(records); i++ {
				outputValues[i] = string(values[i])
			}
			outputs[i].Values = outputValues
		default:
			return nil, fmt.Errorf("unsupported data type")
		}
	}
//	fmt.Println("outputs:", outputs)

	bytes, err := json.Marshal(outputs)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (db *DB) complexSelect(groongaOptions, grnxxOptions map[string]string) (
	[]byte, error) {
//	fmt.Println("groongaOptions: groongaOptions")
//	fmt.Println("grnxxOptions: grnxxOptions")
	if db.grnxxDB == nil {
		return nil, fmt.Errorf("grnxx not available")
	}

	groongaOptions["cache"] = "no"
	groongaOptions["output_columns"] = "_id,_score"
	var records []grnxx.Record
	for i := 0; i < len(db.groongaDBs); i++ {
		outputColumns, err := db.groongaDBs[i].Select(groongaOptions)
		if err != nil {
			return nil, err
		}
		rowIDs := outputColumns[0].Values.([]uint32)
		scores := outputColumns[1].Values.([]int32)
		newRecords := make([]grnxx.Record, len(rowIDs))
		for j := 0; j < len(rowIDs); j++ {
			newRecords[j].RowID =
				grnxx.Int(((int(rowIDs[j]) - 1) * len(db.groongaDBs)) + i + 1)
			newRecords[j].Score = grnxx.Float(scores[j])
		}
		records = append(records, newRecords...)
	}

	table := db.grnxxDB.FindTable(groongaOptions["table"])
	if table == nil {
		return nil, fmt.Errorf("table not found: name = %s",
			groongaOptions["table"])
	}

	filterString, ok := grnxxOptions["grnxx_filter"]
	if !ok {
		filterString = "TRUE"
	}
	offset := 0
	offsetString, ok := grnxxOptions["grnxx_offset"]
	if ok {
		var err error
		offset, err = strconv.Atoi(offsetString)
		if err != nil {
			return nil, err
		}
	}
	limit := 10
	limitString, ok := grnxxOptions["grnxx_limit"]
	if ok {
		var err error
		limit, err = strconv.Atoi(limitString)
		if err != nil {
			return nil, err
		}
	}
	filter, err := grnxx.ParseExpression(table, filterString)
	if err != nil {
		return nil, err
	}
	defer filter.Close()
	filter.FilterEx(&records, offset, limit)
//	fmt.Println("records:", records)

	var outputs []Output
	optionValue, ok := grnxxOptions["grnxx_output_columns"]
	if ok {
		values := strings.Split(optionValue, ",")
		for _, value := range values {
			value = strings.TrimFunc(value, unicode.IsSpace)
			if len(value) != 0 {
				outputs = append(outputs, Output{Name: value})
			}
		}
	} else {
		outputs = append(outputs, Output{Name: "_id"})
		numColumns := table.NumColumns()
		for i := 0; i < numColumns; i++ {
			outputs = append(outputs, Output{Name: table.GetColumn(i).Name()})
		}
	}
//	fmt.Println("outputs:", outputs)

	for i := 0; i < len(outputs); i++ {
		expression, err := grnxx.ParseExpression(table, outputs[i].Name)
		if err != nil {
			return nil, err
		}
		defer expression.Close()

		switch expression.DataType() {
		case grnxx.BOOL:
			outputs[i].Type = "Bool"
			values := make([]grnxx.Bool, len(records))
			if err = expression.Evaluate(records, values); err != nil {
				return nil, err
			}
			outputs[i].Values = values
		case grnxx.INT:
			outputs[i].Type = "Int"
			values := make([]grnxx.Int, len(records))
			if err = expression.Evaluate(records, values); err != nil {
				return nil, err
			}
			outputs[i].Values = values
		case grnxx.FLOAT:
			outputs[i].Type = "Float"
			values := make([]grnxx.Float, len(records))
			if err = expression.Evaluate(records, values); err != nil {
				return nil, err
			}
			outputs[i].Values = values
		case grnxx.TEXT:
			outputs[i].Type = "Text"
			values := make([]grnxx.Text, len(records))
			if err = expression.Evaluate(records, values); err != nil {
				return nil, err
			}
			outputValues := make([]string, len(records))
			for i := 0; i < len(records); i++ {
				outputValues[i] = string(values[i])
			}
			outputs[i].Values = outputValues
		default:
			return nil, fmt.Errorf("unsupported data type")
		}
	}
//	fmt.Println("outputs:", outputs)

	bytes, err := json.Marshal(outputs)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (db *DB) gnxSelect(command string) ([]byte, error) {
	_, options, err := db.ParseCommand(command)
	if err != nil {
		return nil, err
	}
	groongaOptions := make(map[string]string)
	grnxxOptions := make(map[string]string)
	for key, value := range options {
		if strings.HasPrefix(key, "grnxx_") {
			grnxxOptions[key] = value
		} else {
			groongaOptions[key] = value
		}
	}
	if _, ok := groongaOptions["table"]; !ok {
		return nil, fmt.Errorf("table name missing")
	}
	if len(grnxxOptions) == 0 {
		return db.groongaSelect(groongaOptions)
	} else if len(groongaOptions) == 1 {
		grnxxOptions["table"] = groongaOptions["table"]
		return db.grnxxSelect(grnxxOptions)
	} else {
		return db.complexSelect(groongaOptions, grnxxOptions)
	}
}

func (db *DB) gnxSnapshot(command string) ([]byte, error) {
	_, options, err := db.ParseCommand(command)
	if err != nil {
		return nil, err
	}

	tableName, ok := options["table"]
	if !ok {
		return nil, fmt.Errorf("table name missing")
	}
	columnName, ok := options["column"]
	if !ok {
		return nil, fmt.Errorf("column name missing")
	}
//	fmt.Println("tableName:", tableName)
//	fmt.Println("columnName:", columnName)

	idColumns := make([]groonga.OutputColumn, len(db.groongaDBs))
	valueColumns := make([]groonga.OutputColumn, len(db.groongaDBs))
	for i := 0; i < len(db.groongaDBs); i++ {
		outputColumns, err := db.groongaDBs[i].Select(map[string]string{
			"table": tableName, "output_columns": fmt.Sprintf("_id,%s", columnName),
			"limit": "-1", "cache": "no"})
		if err != nil {
			return nil, err
		}
		if len(outputColumns) != 2 {
			return nil, fmt.Errorf("column not found: name = %s", columnName)
		}
		if outputColumns[1].Name != columnName {
			return nil, fmt.Errorf("column mismatch: actual = %s, request = %s",
				outputColumns[1].Name, columnName)
		}
		idColumns[i] = outputColumns[0]
		valueColumns[i] = outputColumns[1]
	}
//	fmt.Println("idColumns:", idColumns)
//	fmt.Println("valueColumns:", valueColumns)

	var dataType grnxx.DataType
	switch valueColumns[0].Type {
	case "Bool":
		dataType = grnxx.BOOL
	case "Int8", "Int16", "Int32", "Int64",
		"UInt8", "UInt16", "UInt32", "UInt64":
		dataType = grnxx.INT
	case "Float":
		dataType = grnxx.FLOAT
	case "Time":
		dataType = grnxx.INT
	case "ShortText", "Text", "LongText":
		dataType = grnxx.TEXT
	default:
		return nil, fmt.Errorf("unsupported data type")
	}

	if db.grnxxDB == nil {
		if db.grnxxDB, err = grnxx.CreateDB(); err != nil {
			return nil, err
		}
	}
	table := db.grnxxDB.FindTable(tableName)
	if table == nil {
		if table, err = db.grnxxDB.CreateTable(tableName); err != nil {
			return nil, err
		}
	}
	column := table.FindColumn(columnName)
	if column == nil {
		if column, err = table.CreateColumn(columnName, dataType, nil); err != nil {
			return nil, err
		}
	}

	for i := 0; i < len(idColumns); i++ {
		ids := idColumns[i].Values.([]uint32)
		for j := 0; j < len(ids); j++ {
			rowID := grnxx.Int(int(ids[j] - 1) * len(idColumns) + i + 1)
			if !table.TestRow(rowID) {
				if err := table.InsertRowAt(rowID, nil); err != nil {
					return nil, err
				}
			}
			switch values := valueColumns[i].Values.(type) {
			case []bool:
				if values[j] {
					err = column.Set(rowID, grnxx.TRUE)
				} else {
					err = column.Set(rowID, grnxx.FALSE)
				}
			case []int8:
				err = column.Set(rowID, grnxx.Int(values[j]))
			case []int16:
				err = column.Set(rowID, grnxx.Int(values[j]))
			case []int32:
				err = column.Set(rowID, grnxx.Int(values[j]))
			case []int64:
				err = column.Set(rowID, grnxx.Int(values[j]))
			case []uint8:
				err = column.Set(rowID, grnxx.Int(values[j]))
			case []uint16:
				err = column.Set(rowID, grnxx.Int(values[j]))
			case []uint32:
				err = column.Set(rowID, grnxx.Int(values[j]))
			case []uint64:
				err = column.Set(rowID, grnxx.Int(values[j]))
			case []float64:
				err = column.Set(rowID, grnxx.Float(values[j]))
			case []time.Time:
				err = column.Set(rowID, grnxx.Int(values[j].UnixNano() / 1000))
			case []string:
				err = column.Set(rowID, grnxx.Text(values[j]))
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return []byte("true"), nil
}

func (db *DB) Query(command string) ([]byte, error) {
	name, _, ok, err := db.tokenizeCommand(command)
	if err != nil {
		return nil, err
	}
	if !ok {
		return []byte{}, nil
	}
	switch name {
	case "gnx_load":
		return db.gnxLoad(command)
	case "gnx_select":
		return db.gnxSelect(command)
	case "gnx_snapshot":
		return db.gnxSnapshot(command)
	default:
		// Query the command to all DBs.
		if name == "select" {
			command += " --cache no"
		}
		results := make([][]byte, len(db.groongaDBs))
		count := 1
		for i := 0; i < len(results); i++ {
			bytes, err := db.groongaDBs[i].Query(command)
			if err != nil {
				return nil, err
			}
			results[i] = bytes
			count += len(bytes) + 1
		}
		bytes := make([]byte, 0, count)
		bytes = append(bytes, '[')
		bytes = append(bytes, results[0]...)
		for i := 1; i < len(results); i++ {
			bytes = append(bytes, ',')
			bytes = append(bytes, results[i]...)
		}
		bytes = append(bytes, ']')
		return bytes, nil
	}
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
	"defrag":   {"objname", "threshold"},
	"delete":   {"table", "key", "id", "filter"},
	"dump":     {"tables"},
	"gnx_load": {"values", "table", "columns", "ifexists", "input_type"},
	"gnx_select": {
		"table", "match_columns", "query", "filter", "scorer", "sortby",
		"output_columns", "offset", "limit", "drilldown", "drilldown_sortby",
		"drilldown_output_columns", "drilldown_offset", "drilldown_limit",
		"cache", "match_escalation_threshold", "query_expansion", "query_flags",
		"query_expander", "adjuster", "drilldown_calc_types",
		"drilldown_calc_target",
		"grnxx_filter", "grnxx_output_columns", "grnxx_offset", "grnxx_limit"},
	"gnx_snapshot": {"table", "column"},
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
