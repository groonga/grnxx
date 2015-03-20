package main

import "./gnx/groonga"
import "bufio"
import "flag"
import "fmt"
import "io"
import "log"
import "os"
import "strings"

// Command-line flags.
var flagNew = flag.Bool("new", false, "create new database")
var flagTemp = flag.Bool("temp", false, "create temporary database")

func openOrCreateDB() (*groonga.DB, string, error) {
	path := ""
	if flag.NArg() != 0 {
		path = flag.Arg(0)
	}
	if *flagTemp {
		db, dir, err := groonga.CreateTempDB(path, "groonga")
		if err != nil {
			log.Println("groonga.CreateTempDB() failed: err =", err)
			return nil, "", err
		}
		return db, dir, err
	}
	if *flagNew {
		db, err := groonga.CreateDB(path)
		if err != nil {
			log.Println("groonga.CreateDB() failed: err =", err)
			return nil, "", err
		}
		return db, "", err
	}
	db, err := groonga.OpenDB(path)
	if err != nil {
		log.Println("groonga.OpenDB() failed: err =", err)
		return nil, "", err
	}
	return db, "", err
}

func consoleMain() int {
	// Parse command-line options.
	flag.Parse()

	// Open or create a database.
	db, dir, err := openOrCreateDB()
	if err != nil {
		return 1
	}
	if len(dir) != 0 {
		defer os.RemoveAll(dir)
	}
	defer db.Close()

	// Read lines from the standard input.
	console := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		line, err := console.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			log.Println("console error: err =", err)
			return 1
		}
		command := strings.TrimRight(line, "\r\n")
		bytes, err := db.Query(command)
		if err != nil {
			log.Println("query error: err =", err)
		} else {
			fmt.Println(string(bytes))
		}

		name, options, err := db.ParseCommand(command)
		fmt.Println("name:", name)
		fmt.Println("options:", options)
		if name == "select" {
			records, err := db.Select(options)
			if err != nil {
				log.Println("select error: err =", err)
			}
			fmt.Println("records:", records)
		}
	}
	return 0
}

func main() {
	os.Exit(consoleMain())
}
