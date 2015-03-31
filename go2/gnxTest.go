package main

import "./gnx"
import "fmt"
import "log"
import "os"

func main() {
	db, dir, err := gnx.CreateTempDB("", "gnxConsole", 1)
	if err != nil {
		log.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	_, err = db.GroongaQuery(0, "table_create Table TABLE_NO_KEY")
	if err != nil {
		log.Println(err)
		return
	}

	_, err = db.GroongaQuery(0, "column_create Table Value COLUMN_SCALAR Int32")
	if err != nil {
		log.Println(err)
		return
	}

	{
		records := make([][]gnx.Valuer, 3)
		records[0] = []gnx.Valuer{gnx.Int(10)}
		records[1] = []gnx.Valuer{gnx.Int(20)}
		records[2] = []gnx.Valuer{gnx.Int(30)}
		count, err := db.Load("Table", []string{"Value"}, records)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		recordMaps := make([]map[string]gnx.Valuer, 3)
		recordMaps[0] = map[string]gnx.Valuer{"Value":gnx.Int(100)}
		recordMaps[1] = map[string]gnx.Valuer{"Value":gnx.Int(200)}
		recordMaps[2] = map[string]gnx.Valuer{"Value":gnx.Int(300)}
		count, err := db.LoadMap("Table", recordMaps)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		columnarRecords := make([]interface{}, 1)
		columnarRecords[0] = []gnx.Int{-10,-20,-30}
		count, err := db.LoadC("Table", []string{"Value"}, columnarRecords)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		columnarRecordsMap := make(map[string]interface{})
		columnarRecordsMap["Value"] = []gnx.Int{-100,-200,-300}
		count, err := db.LoadCMap("Table", columnarRecordsMap)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	command := "select Table --limit -1"
	jsonBytes, err := db.GroongaQuery(0, command)
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Println("result:", string(jsonBytes))
}
