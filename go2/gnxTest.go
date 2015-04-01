package main

import "./gnx"
import "fmt"
import "log"
import "os"

func testA() {
	log.Println("testA()")

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
		var records [][]gnx.Valuer
		records = append(records, []gnx.Valuer{gnx.Int(1)})
		records = append(records, []gnx.Valuer{gnx.Int(2)})
		records = append(records, []gnx.Valuer{gnx.Int(3)})
		count, err := db.Load("Table", []string{"Value"}, records)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		var recordMaps []map[string]gnx.Valuer
		recordMaps = append(recordMaps, map[string]gnx.Valuer{"Value":gnx.Int(10)})
		recordMaps = append(recordMaps, map[string]gnx.Valuer{"Value":gnx.Int(20)})
		recordMaps = append(recordMaps, map[string]gnx.Valuer{"Value":gnx.Int(30)})
		count, err := db.LoadMap("Table", recordMaps)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		var columnarRecords []interface{}
		columnarRecords = append(columnarRecords, []gnx.Int{-1,-2,-3})
		count, err := db.LoadC("Table", []string{"Value"}, columnarRecords)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		columnarRecordsMap := make(map[string]interface{})
		columnarRecordsMap["Value"] = []gnx.Int{-10,-20,-30}
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

func testB() {
	log.Println("testB()")

	db, dir, err := gnx.CreateTempDB("", "gnxConsole", 3)
	if err != nil {
		log.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	for i := 0; i < 3; i++ {
		_, err = db.GroongaQuery(i, "table_create Table TABLE_NO_KEY")
		if err != nil {
			log.Println(err)
			return
		}
	}

	for i := 0; i < 3; i++ {
		_, err = db.GroongaQuery(i, "column_create Table Value COLUMN_SCALAR Int32")
			if err != nil {
			log.Println(err)
			return
		}
	}

	{
		var records [][]gnx.Valuer
		records = append(records, []gnx.Valuer{gnx.Int(1)})
		records = append(records, []gnx.Valuer{gnx.Int(2)})
		records = append(records, []gnx.Valuer{gnx.Int(3)})
		records = append(records, []gnx.Valuer{gnx.Int(4)})
		count, err := db.Load("Table", []string{"Value"}, records)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		var records [][]gnx.Valuer
		records = append(records, []gnx.Valuer{gnx.Int(1), gnx.Int(5)})
		records = append(records, []gnx.Valuer{gnx.Int(2), gnx.Int(6)})
		records = append(records, []gnx.Valuer{gnx.Int(3), gnx.Int(7)})
		records = append(records, []gnx.Valuer{gnx.Int(4), gnx.Int(8)})
		fmt.Println("records (before):", records)
		count, err := db.Load("Table", []string{"_id","Value"}, records)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("records (after):", records)
		fmt.Println("count:", count)
	}

	{
		var recordMaps []map[string]gnx.Valuer
		recordMaps = append(recordMaps, map[string]gnx.Valuer{"Value":gnx.Int(10)})
		recordMaps = append(recordMaps, map[string]gnx.Valuer{"Value":gnx.Int(20)})
		recordMaps = append(recordMaps, map[string]gnx.Valuer{"Value":gnx.Int(30)})
		recordMaps = append(recordMaps, map[string]gnx.Valuer{"Value":gnx.Int(40)})
		fmt.Println("recordMaps (before):", recordMaps)
		count, err := db.LoadMap("Table", recordMaps)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("recordMaps (after):", recordMaps)
		fmt.Println("count:", count)
	}

	{
		var recordMaps []map[string]gnx.Valuer
		recordMaps = append(recordMaps, map[string]gnx.Valuer{
			"_id":gnx.Int(1), "Value":gnx.Int(50)})
		recordMaps = append(recordMaps, map[string]gnx.Valuer{
			"_id":gnx.Int(2), "Value":gnx.Int(60)})
		recordMaps = append(recordMaps, map[string]gnx.Valuer{
			"_id":gnx.Int(3), "Value":gnx.Int(70)})
		recordMaps = append(recordMaps, map[string]gnx.Valuer{
			"_id":gnx.Int(4), "Value":gnx.Int(80)})
		fmt.Println("recordMaps (before):", recordMaps)
		count, err := db.LoadMap("Table", recordMaps)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("recordMaps (after):", recordMaps)
		fmt.Println("count:", count)
	}

//	{
//		recordMaps := make([]map[string]gnx.Valuer, 3)
//		recordMaps[0] = map[string]gnx.Valuer{"Value":gnx.Int(100)}
//		recordMaps[1] = map[string]gnx.Valuer{"Value":gnx.Int(200)}
//		recordMaps[2] = map[string]gnx.Valuer{"Value":gnx.Int(300)}
//		count, err := db.LoadMap("Table", recordMaps)
//		if err != nil {
//			log.Println(err)
//			return
//		}
//		fmt.Println("count:", count)
//	}

//	{
//		columnarRecords := make([]interface{}, 1)
//		columnarRecords[0] = []gnx.Int{-10,-20,-30}
//		count, err := db.LoadC("Table", []string{"Value"}, columnarRecords)
//		if err != nil {
//			log.Println(err)
//			return
//		}
//		fmt.Println("count:", count)
//	}

//	{
//		columnarRecordsMap := make(map[string]interface{})
//		columnarRecordsMap["Value"] = []gnx.Int{-100,-200,-300}
//		count, err := db.LoadCMap("Table", columnarRecordsMap)
//		if err != nil {
//			log.Println(err)
//			return
//		}
//		fmt.Println("count:", count)
//	}

	command := "select Table --limit -1"
	for i := 0; i < 3; i++ {
		jsonBytes, err := db.GroongaQuery(i, command)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Printf("result[%d]: %s\n", i, string(jsonBytes))
	}
}

func main() {
	testA()
	testB()
}
