package main

import "./gnx"

import "bufio"
import "flag"
import "fmt"
import "log"
import "math/rand"
import "os"
import "runtime"
import "runtime/pprof"
import "strconv"
import "strings"
import "time"
import "unicode"

var flagMode = flag.String("mode", "benchmark", "mode (run or print)")
var flagKey = flag.String("key", "", "key data type")
var flagColumns = flag.String("columns", "Bool,Int16,Float,Text",
	"comma-separated column data types")
var flagRows = flag.Int("rows", 1000000, "number of rows")
var flagBlock = flag.Int("block", 10, "number of rows per command")
var flagPartition = flag.Int("partition", 1, "number of groonga DBs")
var flagCPU = flag.Int("cpu", 1, "number of CPU cores")

var tableName = "Table"
var columnTypes []string
var columnNames []string

func generateCommands() []string {
	var commands []string
	if *flagKey == "" {
		commands = append(commands,
			fmt.Sprintf("table_create %s TABLE_NO_KEY", tableName))
	} else {
		commands = append(commands,
			fmt.Sprintf("table_create %s TABLE_PAT_KEY %s", tableName, *flagKey))
		columnTypes = append(columnTypes, *flagKey)
		columnNames = append(columnNames, "_key")
	}

	if *flagColumns != "" {
		tokens := strings.Split(*flagColumns, ",")
		for _, token := range tokens {
			columnType := strings.TrimFunc(token, unicode.IsSpace)
			if columnType != "" {
				columnID := len(columnTypes)
				columnTypes = append(columnTypes, columnType)
				columnName := fmt.Sprintf("Col%d", columnID)
				columnNames = append(columnNames, columnName)
				commands = append(commands,
					fmt.Sprintf("column_create %s %s COLUMN_SCALAR %s",
						tableName, columnName, columnType))
				switch columnType {
				case "Text", "LongText":
					const tokenizerOption = "--default_tokenizer TokenBigram"
					commands = append(commands, fmt.Sprintf(
						"table_create Idx%d TABLE_PAT_KEY ShortText %s",
						columnID, tokenizerOption))
					commands = append(commands, fmt.Sprintf(
						"column_create Idx%d Idx COLUMN_INDEX|WITH_POSITION %s Col%d",
						columnID, tableName, columnID))
				}
			}
		}
	}
	return commands
}

func generateSource(columnType string) interface{} {
	switch columnType {
	case "Bool":
		values := make([]gnx.Bool, *flagRows)
		for i := 0; i < len(values); i++ {
			if (rand.Int() % 2) == 1 {
				values[i] = gnx.TRUE
			} else {
				values[i] = gnx.FALSE
			}
		}
		return values
	case "Int8":
		values := make([]gnx.Int, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Int(rand.Int() % 128)
		}
		return values
	case "Int16":
		values := make([]gnx.Int, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Int(rand.Int() % 32768)
		}
		return values
	case "Int32":
		values := make([]gnx.Int, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Int(rand.Int31())
		}
		return values
	case "Int64":
		values := make([]gnx.Int, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Int(rand.Int31())
		}
		return values

		return rand.Int63()
	case "UInt8":
		values := make([]gnx.Int, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Int(rand.Int() % 256)
		}
		return values
	case "UInt16":
		values := make([]gnx.Int, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Int(rand.Int() % 65536)
		}
		return values
	case "UInt32":
		values := make([]gnx.Int, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Int(rand.Uint32())
		}
		return values
	case "UInt64":
		values := make([]gnx.Int, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Int(rand.Int63())
		}
		return values
	case "Float":
		values := make([]gnx.Float, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Float(rand.Float64())
		}
		return values
	case "ShortText":
		values := make([]gnx.Text, *flagRows)
		for i := 0; i < len(values); i++ {
			values[i] = gnx.Text(strconv.Itoa(int(rand.Int31())))
		}
		return values
	case "Text", "LongText":
		values := make([]gnx.Text, *flagRows)
		scanner := bufio.NewScanner(os.Stdin)
		for i := 0; i < len(values); i++ {
			if scanner.Scan() {
				values[i] = gnx.Text(scanner.Text())
			} else {
				values[i] = gnx.Text(strconv.Itoa(int(rand.Int31())))
			}
		}
		return values
	default:
		log.Fatalln("unsupported value type: name =", columnType)
		return nil
	}
}

func generateSources() []interface{} {
	var sources []interface{}
	for _, columnType := range columnTypes {
		sources = append(sources, generateSource(columnType))
	}
	return sources
}

func benchmarkLoad(commands []string, sources []interface{}) {
	fmt.Println("benchmarkLoad()")

	var recordsList [][][]gnx.Valuer
	for i := 0; i < *flagRows; i += *flagBlock {
		var records [][]gnx.Valuer
		for j := 0; j < *flagBlock; j++ {
			id := i + j
			if id >= *flagRows {
				break
			}
			record := make([]gnx.Valuer, len(sources))
			for k, source := range sources {
				switch values := source.(type) {
				case []gnx.Bool:
					record[k] = values[id]
				case []gnx.Int:
					record[k] = values[id]
				case []gnx.Float:
					record[k] = values[id]
				case []gnx.Text:
					record[k] = values[id]
				default:
					log.Fatalln("unsupported value type")
				}
			}
			records = append(records, record)
		}
		recordsList = append(recordsList, records)
	}
//	fmt.Println(recordsList)

	db, dir, err := gnx.CreateTempDB("", "gnx-benchmark", *flagPartition)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()
	for _, command := range commands {
		for i := 0; i < *flagPartition; i++ {
//			fmt.Println(command)
			if _, err := db.GroongaQuery(i, command); err != nil {
				log.Fatalln(err)
			}
		}
	}

	file, err := os.Create("Load.prof")
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		log.Fatalln(err)
	}
	defer pprof.StopCPUProfile()

	startTime := time.Now()
	for _, records := range recordsList {
		if _, err := db.Load(tableName, columnNames, records); err != nil {
			log.Fatalln(err)
		}
	}
	endTime := time.Now()
	fmt.Println("elapsed:", endTime.Sub(startTime))
}

func benchmarkLoadMap(commands []string, sources []interface{}) {
	fmt.Println("benchmarkLoadMap()")

	var recordMapsList [][]map[string]gnx.Valuer
	for i := 0; i < *flagRows; i += *flagBlock {
		var recordMaps []map[string]gnx.Valuer
		for j := 0; j < *flagBlock; j++ {
			id := i + j
			if id >= *flagRows {
				break
			}
			recordMap := make(map[string]gnx.Valuer)
			for k, source := range sources {
				switch values := source.(type) {
				case []gnx.Bool:
					recordMap[columnNames[k]] = values[id]
				case []gnx.Int:
					recordMap[columnNames[k]] = values[id]
				case []gnx.Float:
					recordMap[columnNames[k]] = values[id]
				case []gnx.Text:
					recordMap[columnNames[k]] = values[id]
				default:
					log.Fatalln("unsupported value type")
				}
			}
			recordMaps = append(recordMaps, recordMap)
		}
		recordMapsList = append(recordMapsList, recordMaps)
	}
//	fmt.Println(recordMapsList)

	db, dir, err := gnx.CreateTempDB("", "gnx-benchmark", *flagPartition)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()
	for _, command := range commands {
		for i := 0; i < *flagPartition; i++ {
//			fmt.Println(command)
			if _, err := db.GroongaQuery(i, command); err != nil {
				log.Fatalln(err)
			}
		}
	}

	file, err := os.Create("LoadMap.prof")
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		log.Fatalln(err)
	}
	defer pprof.StopCPUProfile()

	startTime := time.Now()
	for _, recordMaps := range recordMapsList {
		if _, err := db.LoadMap(tableName, recordMaps); err != nil {
			log.Fatalln(err)
		}
	}
	endTime := time.Now()
	fmt.Println("elapsed:", endTime.Sub(startTime))
}

func benchmarkLoadC(commands []string, sources []interface{}) {
	fmt.Println("benchmarkLoadC()")

	var columnarRecordsList [][]interface{}
	for i := 0; i < *flagRows; i += *flagBlock {
		columnarRecords := make([]interface{}, len(sources))
		begin := i
		end := begin + *flagBlock
		if end > *flagRows {
			end = *flagRows
		}
		for j, source := range sources {
			switch values := source.(type) {
			case []gnx.Bool:
				columnarRecords[j] = values[begin:end]
			case []gnx.Int:
				columnarRecords[j] = values[begin:end]
			case []gnx.Float:
				columnarRecords[j] = values[begin:end]
			case []gnx.Text:
				columnarRecords[j] = values[begin:end]
			default:
				log.Fatalln("unsupported value type")
			}
		}
		columnarRecordsList = append(columnarRecordsList, columnarRecords)
	}
//	fmt.Println(columnarRecordsList)

	db, dir, err := gnx.CreateTempDB("", "gnx-benchmark", *flagPartition)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()
	for _, command := range commands {
		for i := 0; i < *flagPartition; i++ {
//			fmt.Println(command)
			if _, err := db.GroongaQuery(i, command); err != nil {
				log.Fatalln(err)
			}
		}
	}

	file, err := os.Create("LoadC.prof")
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		log.Fatalln(err)
	}
	defer pprof.StopCPUProfile()

	startTime := time.Now()
	for _, columnarRecords := range columnarRecordsList {
		_, err := db.LoadC(tableName, columnNames, columnarRecords)
		if err != nil {
			log.Fatalln(err)
		}
	}
	endTime := time.Now()
	fmt.Println("elapsed:", endTime.Sub(startTime))
}

func benchmarkLoadCMap(commands []string, sources []interface{}) {
	fmt.Println("benchmarkLoadCMap()")

	var columnarRecordsMaps []map[string]interface{}
	for i := 0; i < *flagRows; i += *flagBlock {
		columnarRecordsMap := make(map[string]interface{})
		begin := i
		end := begin + *flagBlock
		if end > *flagRows {
			end = *flagRows
		}
		for j, source := range sources {
			switch values := source.(type) {
			case []gnx.Bool:
				columnarRecordsMap[columnNames[j]] = values[begin:end]
			case []gnx.Int:
				columnarRecordsMap[columnNames[j]] = values[begin:end]
			case []gnx.Float:
				columnarRecordsMap[columnNames[j]] = values[begin:end]
			case []gnx.Text:
				columnarRecordsMap[columnNames[j]] = values[begin:end]
			default:
				log.Fatalln("unsupported value type")
			}
		}
		columnarRecordsMaps = append(columnarRecordsMaps, columnarRecordsMap)
	}
//	fmt.Println(columnarRecordsMaps)

	db, dir, err := gnx.CreateTempDB("", "gnx-benchmark", *flagPartition)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()
	for _, command := range commands {
		for i := 0; i < *flagPartition; i++ {
//			fmt.Println(command)
			if _, err := db.GroongaQuery(i, command); err != nil {
				log.Fatalln(err)
			}
		}
	}

	file, err := os.Create("LoadCMap.prof")
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		log.Fatalln(err)
	}
	defer pprof.StopCPUProfile()

	startTime := time.Now()
	for _, columnarRecordsMap := range columnarRecordsMaps {
		_, err := db.LoadCMap(tableName, columnarRecordsMap)
		if err != nil {
			log.Fatalln(err)
		}
	}
	endTime := time.Now()
	fmt.Println("elapsed:", endTime.Sub(startTime))
}

func benchmarkDirect(commands []string, sources []interface{}) {
	fmt.Println("benchmarkDirect()")

//	fmt.Println(sources)

	db, dir, err := gnx.CreateTempDB("", "gnx-benchmark", *flagPartition)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()
	for _, command := range commands {
		for i := 0; i < *flagPartition; i++ {
//			fmt.Println(command)
			if _, err := db.GroongaQuery(i, command); err != nil {
				log.Fatalln(err)
			}
		}
	}

	file, err := os.Create("Direct.prof")
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		log.Fatalln(err)
	}
	defer pprof.StopCPUProfile()

	startTime := time.Now()
	var rowIDs []gnx.Int
	if *flagKey == "" {
		for i := 0; i < *flagRows; i++ {
			_, rowID, err := db.InsertRow(tableName, nil)
			if err != nil {
				log.Fatalln(err)
			}
			rowIDs = append(rowIDs, rowID)
		}
	} else {
		switch keys := sources[0].(type) {
		case []gnx.Int:
			for _, key := range keys {
				_, rowID, err := db.InsertRow(tableName, key)
				if err != nil {
					log.Fatalln(err)
				}
				rowIDs = append(rowIDs, rowID)
			}
		case []gnx.Float:
			for _, key := range keys {
				_, rowID, err := db.InsertRow(tableName, key)
				if err != nil {
					log.Fatalln(err)
				}
				rowIDs = append(rowIDs, rowID)
			}
		case []gnx.Text:
			for _, key := range keys {
				_, rowID, err := db.InsertRow(tableName, key)
				if err != nil {
					log.Fatalln(err)
				}
				rowIDs = append(rowIDs, rowID)
			}
		default:
			log.Fatalln("unsupported key type")
		}
	}
	for i, source := range sources {
		if columnNames[i] == "_key" {
			continue
		}
		switch values := source.(type) {
		case []gnx.Bool:
			for j, value := range values {
				err := db.SetValue(tableName, columnNames[i], rowIDs[j], value)
				if err != nil {
					log.Fatalln(err)
				}
			}
		case []gnx.Int:
			for j, value := range values {
				err := db.SetValue(tableName, columnNames[i], rowIDs[j], value)
				if err != nil {
					log.Fatalln(err)
				}
			}
		case []gnx.Float:
			for j, value := range values {
				err := db.SetValue(tableName, columnNames[i], rowIDs[j], value)
				if err != nil {
					log.Fatalln(err)
				}
			}
		case []gnx.Text:
			for j, value := range values {
				err := db.SetValue(tableName, columnNames[i], rowIDs[j], value)
				if err != nil {
					log.Fatalln(err)
				}
			}
		}
	}
	endTime := time.Now()
	fmt.Println("elapsed:", endTime.Sub(startTime))
}

func benchmarkDirect2(commands []string, sources []interface{}) {
	fmt.Println("benchmarkDirect2()")

//	fmt.Println(sources)

	db, dir, err := gnx.CreateTempDB("", "gnx-benchmark", *flagPartition)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()
	for _, command := range commands {
		for i := 0; i < *flagPartition; i++ {
//			fmt.Println(command)
			if _, err := db.GroongaQuery(i, command); err != nil {
				log.Fatalln(err)
			}
		}
	}

	file, err := os.Create("Direct2.prof")
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		log.Fatalln(err)
	}
	defer pprof.StopCPUProfile()

	startTime := time.Now()
	table, err := db.FindTable(tableName)
	if err != nil {
		log.Fatalln(err)
	}
	var columns []*gnx.Column
	for _, columnName := range columnNames {
		column, err := db.FindColumn(table, columnName)
		if err != nil {
			log.Fatalln(err)
		}
		columns = append(columns, column)
	}
	var rowIDs []gnx.Int
	if *flagKey == "" {
		for i := 0; i < *flagRows; i++ {
			_, rowID, err := db.InsertRow2(table, nil)
			if err != nil {
				log.Fatalln(err)
			}
			rowIDs = append(rowIDs, rowID)
		}
	} else {
		switch keys := sources[0].(type) {
		case []gnx.Int:
			for _, key := range keys {
				_, rowID, err := db.InsertRow2(table, key)
				if err != nil {
					log.Fatalln(err)
				}
				rowIDs = append(rowIDs, rowID)
			}
		case []gnx.Float:
			for _, key := range keys {
				_, rowID, err := db.InsertRow2(table, key)
				if err != nil {
					log.Fatalln(err)
				}
				rowIDs = append(rowIDs, rowID)
			}
		case []gnx.Text:
			for _, key := range keys {
				_, rowID, err := db.InsertRow2(table, key)
				if err != nil {
					log.Fatalln(err)
				}
				rowIDs = append(rowIDs, rowID)
			}
		default:
			log.Fatalln("unsupported key type")
		}
	}
	for i, source := range sources {
		if columnNames[i] == "_key" {
			continue
		}
		switch values := source.(type) {
		case []gnx.Bool:
			for j, value := range values {
				err := db.SetValue2(columns[i], rowIDs[j], value)
				if err != nil {
					log.Fatalln(err)
				}
			}
		case []gnx.Int:
			for j, value := range values {
				err := db.SetValue2(columns[i], rowIDs[j], value)
				if err != nil {
					log.Fatalln(err)
				}
			}
		case []gnx.Float:
			for j, value := range values {
				err := db.SetValue2(columns[i], rowIDs[j], value)
				if err != nil {
					log.Fatalln(err)
				}
			}
		case []gnx.Text:
			for j, value := range values {
				err := db.SetValue2(columns[i], rowIDs[j], value)
				if err != nil {
					log.Fatalln(err)
				}
			}
		}
	}
	endTime := time.Now()
	fmt.Println("elapsed:", endTime.Sub(startTime))
}

func benchmark() {
	if *flagCPU == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(*flagCPU)
	}
	commands := generateCommands()
	sources := generateSources()

	benchmarkLoad(commands, sources)
	benchmarkLoadMap(commands, sources)
	benchmarkLoadC(commands, sources)
	benchmarkLoadCMap(commands, sources)
	benchmarkDirect(commands, sources)
	benchmarkDirect2(commands, sources)
}

func print() {
	commands := generateCommands()
	for _, command := range commands {
		fmt.Println(command)
	}
}

func main() {
	flag.Parse()
	switch *flagMode {
	case "benchmark":
		benchmark()
	case "print":
		print()
	default:
		log.Fatalln("undefined mode")
	}
}
