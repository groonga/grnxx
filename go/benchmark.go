package main

import "./gnx"

import "encoding/json"
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

var flagMode = flag.String("mode", "run", "mode (run or print)")
var flagLoad = flag.String("load", "load", "load type (load or gnx_load)")
var flagKey = flag.String("key", "", "key data type")
var flagColumns = flag.String("columns", "Bool,Int16,Float,ShortText",
	"comma-separated column data types")
var flagRows = flag.Int("rows", 1000000, "number of rows")
var flagBlock = flag.Int("block", 10, "number of rows per command")
var flagPartition = flag.Int("partition", 1, "number of groonga DBs")
var flagCPU = flag.Int("cpu", 1, "number of CPU cores")
var flagProfile = flag.String("profile", "benchmark.prof",
	"path of profile data")

func generateValue(typeName string) interface{} {
	switch typeName {
	case "Bool":
		return (rand.Int() % 2) == 1
	case "Int8":
		return int8(rand.Int() % 128)
	case "Int16":
		return int16(rand.Int() % 32768)
	case "Int32":
		return rand.Int31()
	case "Int64":
		return rand.Int63()
	case "UInt8":
		return uint8(rand.Int() % 256)
	case "UInt16":
		return uint16(rand.Int() % 65536)
	case "UInt32":
		return rand.Uint32()
	case "UInt64":
		return uint64(rand.Int63())
	case "Float":
		return rand.Float64()
	case "ShortText", "Text", "LongText":
		return strconv.Itoa(int(rand.Int31()))
	default:
		log.Fatalln("unsupported data type: name =", typeName)
		return nil
	}
}

func generateCommands() []string {
	var commands []string
	if *flagKey == "" {
		commands = append(commands, "table_create Table TABLE_NO_KEY")
	} else {
		commands = append(commands,
			fmt.Sprintf("table_create Table TABLE_PAT_KEY %s", *flagKey))
	}

	var columnTypes []string
	var columnNames []string
	if *flagColumns != "" {
		tokens := strings.Split(*flagColumns, ",")
		for _, token := range tokens {
			columnType := strings.TrimFunc(token, unicode.IsSpace)
			if columnType != "" {
				columnTypes = append(columnTypes, columnType)
				columnName := fmt.Sprintf("Col%d", len(columnNames))
				columnNames = append(columnNames, columnName)
				commands = append(commands,
					fmt.Sprintf("column_create Table %s COLUMN_SCALAR %s",
						columnName, columnType))
			}
		}
	}

	columnsOption := strings.Join(columnNames, ",")
	numValues := len(columnNames)
	if *flagKey != "" {
		if columnsOption == "" {
			columnsOption = "_key"
		} else {
			columnsOption = "_key," + columnsOption
		}
		numValues += 1
	}
	loadPrefix := fmt.Sprintf("%s --table Table --columns '%s' --values ",
		*flagLoad, columnsOption)
	for i := 0; i < *flagRows; i += *flagBlock {
		var blockValues [][]interface{}
		for j := 0; (j < *flagBlock) && ((i + j) < *flagRows); j++ {
			var rowValues []interface{}
			if *flagKey != "" {
				rowValues = append(rowValues, generateValue(*flagKey))
			}
			for _, columnType := range columnTypes {
				rowValues = append(rowValues, generateValue(columnType))
			}
			blockValues = append(blockValues, rowValues)
		}
		bytes, err := json.Marshal(blockValues)
		if err != nil {
			log.Fatalln(err)
		}
		commands = append(commands, loadPrefix+"'"+string(bytes)+"'")
	}
	return commands
}

func runCommands(commands []string) {
	file, err := os.Create(*flagProfile)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	if err := pprof.StartCPUProfile(file); err != nil {
		log.Fatalln(err)
	}
	defer pprof.StopCPUProfile()

	db, dir, err := gnx.CreateTempDB("", "benchmark", *flagPartition)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()
	startTime := time.Now()
	for _, command := range commands {
		if _, err := db.Query(command); err != nil {
			log.Fatalln(err)
		}
	}
	endTime := time.Now()
	fmt.Println("elapsed:", endTime.Sub(startTime))
}

func printCommands(commands []string) {
	db, dir, err := gnx.CreateTempDB("", "benchmark", *flagPartition)
	if err != nil {
		log.Fatalln(err)
	}
	defer os.RemoveAll(dir)
	defer db.Close()
	for _, command := range commands {
		fmt.Println(command)
//		if _, err := db.Query(command); err != nil {
//			log.Fatalln(err)
//		}
	}
}

func main() {
	flag.Parse()
	if *flagCPU == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(*flagCPU)
	}
	commands := generateCommands()
	switch *flagMode {
	case "run":
		runCommands(commands)
	case "print":
		printCommands(commands)
	default:
		log.Fatalln("undefined mode")
	}
}
