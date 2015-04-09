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
		// NOTE: In fact, the IDs are ignored.
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

	{
		var columnarRecords []interface{}
		columnarRecords = append(columnarRecords, []gnx.Int{-1,-2,-3,-4})
		count, err := db.LoadC("Table", []string{"Value"}, columnarRecords)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		var columnarRecords []interface{}
		// NOTE: In fact, the IDs are ignored.
		columnarRecords = append(columnarRecords, []gnx.Int{5,6,7,8})
		columnarRecords = append(columnarRecords, []gnx.Int{-5,-6,-7,-8})
		count, err := db.LoadC("Table", []string{"_id", "Value"}, columnarRecords)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		columnarRecordsMap := make(map[string]interface{})
		columnarRecordsMap["Value"] = []gnx.Int{-10,-20,-30,-40}
		count, err := db.LoadCMap("Table", columnarRecordsMap)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

	{
		columnarRecordsMap := make(map[string]interface{})
		// NOTE: In fact, the IDs are ignored.
		columnarRecordsMap["_id"] = []gnx.Int{9,10,11,12}
		columnarRecordsMap["Value"] = []gnx.Int{-50,-60,-70,-80}
		count, err := db.LoadCMap("Table", columnarRecordsMap)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println("count:", count)
	}

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

func testC() {
	log.Println("testC()")

	db, dir, err := gnx.CreateTempDB("", "gnxConsole", 2)
	if err != nil {
		log.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	defer db.Close()

	{
		for i := 0; i < 2; i++ {
			_, err = db.GroongaQuery(i, "table_create Table TABLE_NO_KEY")
			if err != nil {
				log.Println(err)
				return
			}
		}
		keys := []gnx.Valuer{nil, nil, nil}
		for i, key := range keys {
			inserted, rowID, err := db.InsertRow("Table", key)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("i: %v, key: %v, inserted: %v, rowID: %v\n",
				i, key, inserted, rowID)
		}
		command := "select Table --limit -1 --cache no"
		for i := 0; i < 2; i++ {
			jsonBytes, err := db.GroongaQuery(i, command)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("result[%d]: %s\n", i, string(jsonBytes))
		}
	}

	{
		for i := 0; i < 2; i++ {
			_, err = db.GroongaQuery(i, "table_create Table2 TABLE_PAT_KEY Int32")
			if err != nil {
				log.Println(err)
				return
			}
		}
		keys := []gnx.Valuer{gnx.Int(10), gnx.Int(20), gnx.Int(30)}
		for i, key := range keys {
			inserted, rowID, err := db.InsertRow("Table2", key)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("i: %v, key: %v, inserted: %v, rowID: %v\n",
				i, key, inserted, rowID)
		}
		command := "select Table2 --limit -1 --cache no"
		for i := 0; i < 2; i++ {
			jsonBytes, err := db.GroongaQuery(i, command)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("result[%d]: %s\n", i, string(jsonBytes))
		}
	}

	{
		for i := 0; i < 2; i++ {
			_, err = db.GroongaQuery(i, "table_create Table3 TABLE_PAT_KEY Float")
			if err != nil {
				log.Println(err)
				return
			}
		}
		keys := []gnx.Valuer{gnx.Float(1.25), gnx.Float(2.5), gnx.Float(3.75)}
		for i, key := range keys {
			inserted, rowID, err := db.InsertRow("Table3", key)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("i: %v, key: %v, inserted: %v, rowID: %v\n",
				i, key, inserted, rowID)
		}
		command := "select Table3 --limit -1 --cache no"
		for i := 0; i < 2; i++ {
			jsonBytes, err := db.GroongaQuery(i, command)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("result[%d]: %s\n", i, string(jsonBytes))
		}
	}

	{
		for i := 0; i < 2; i++ {
			_, err = db.GroongaQuery(i, "table_create Table4 TABLE_PAT_KEY WGS84GeoPoint")
			if err != nil {
				log.Println(err)
				return
			}
		}
		keys := []gnx.Valuer{
			gnx.GeoPoint{100,200}, gnx.GeoPoint{300,400}, gnx.GeoPoint{500,600}}
		for i, key := range keys {
			inserted, rowID, err := db.InsertRow("Table4", key)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("i: %v, key: %v, inserted: %v, rowID: %v\n",
				i, key, inserted, rowID)
		}
		command := "select Table4 --limit -1 --cache no"
		for i := 0; i < 2; i++ {
			jsonBytes, err := db.GroongaQuery(i, command)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("result[%d]: %s\n", i, string(jsonBytes))
		}
	}

	{
		for i := 0; i < 2; i++ {
			_, err = db.GroongaQuery(i, "table_create Table5 TABLE_PAT_KEY ShortText")
			if err != nil {
				log.Println(err)
				return
			}
		}
		keys := []gnx.Valuer{gnx.Text("cat"), gnx.Text("dog"), gnx.Text("horse")}
		for i, key := range keys {
			inserted, rowID, err := db.InsertRow("Table5", key)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("i: %v, key: %v, inserted: %v, rowID: %v\n",
				i, key, inserted, rowID)
		}
		command := "select Table5 --limit -1 --cache no"
		for i := 0; i < 2; i++ {
			jsonBytes, err := db.GroongaQuery(i, command)
			if err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("result[%d]: %s\n", i, string(jsonBytes))
		}
	}
}

func testD() {
	log.Println("testC()")

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
		_, err = db.GroongaQuery(
			i, "column_create Table Value1 COLUMN_SCALAR Bool")
		if err != nil {
			log.Println(err)
			return
		}
		_, err = db.GroongaQuery(
			i, "column_create Table Value2 COLUMN_SCALAR Int32")
		if err != nil {
			log.Println(err)
			return
		}
		_, err = db.GroongaQuery(
			i, "column_create Table Value3 COLUMN_SCALAR Float")
		if err != nil {
			log.Println(err)
			return
		}
		_, err = db.GroongaQuery(
			i, "column_create Table Value4 COLUMN_SCALAR Text")
		if err != nil {
			log.Println(err)
			return
		}
	}

	var rowIDs []gnx.Int
	for i := 0; i < 5; i++ {
		inserted, rowID, err := db.InsertRow("Table", nil)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Printf("i: %v, key: %v, inserted: %v, rowID: %v\n",
			i, nil, inserted, rowID)
		rowIDs = append(rowIDs, rowID)
	}

	values1 := []gnx.Bool{
		gnx.FALSE, gnx.TRUE, gnx.NABool(), gnx.FALSE, gnx.TRUE}
	values2 := []gnx.Int{
		gnx.Int(10), gnx.Int(20), gnx.Int(30), gnx.Int(40), gnx.Int(50)}
	values3 := []gnx.Float{
		gnx.Float(1.25), gnx.Float(2.5), gnx.Float(3.75),
		gnx.Float(5.0), gnx.Float(6.25)}
	values4 := []gnx.Text{
		gnx.Text("Apple"), gnx.Text("Banana"), gnx.Text("Orange"),
		gnx.Text("Pineapple"), gnx.Text("Strawberry")}
	for i, rowID := range rowIDs {
		if err := db.SetValue("Table", "Value1", rowID, values1[i]); err != nil {
			log.Println(err)
			return
		}
		if err := db.SetValue("Table", "Value2", rowID, values2[i]); err != nil {
			log.Println(err)
			return
		}
		if err := db.SetValue("Table", "Value3", rowID, values3[i]); err != nil {
			log.Println(err)
			return
		}
		if err := db.SetValue("Table", "Value4", rowID, values4[i]); err != nil {
			log.Println(err)
			return
		}
	}

	command := "select Table --limit -1 --cache no"
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
	testC()
	testD()
}
