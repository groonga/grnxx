package main

import "./gnx"
import "bufio"
import "flag"
import "fmt"
import "io"
import "log"
import "os"
import "runtime/pprof"
import "strings"

// Command-line flags.
var flagNew = flag.Bool("new", false, "create new database")
var flagTemp = flag.Bool("temp", false, "create temporary database")
var flagPartition = flag.Int("partition", 1, "the number of groonga DBs")
var flagProfile = flag.String("profile", "", "the file for cpu profiling")

func openOrCreateDB() (*gnx.DB, string, error) {
	path := ""
	if flag.NArg() != 0 {
		path = flag.Arg(0)
	}
	if *flagTemp {
		db, dir, err := gnx.CreateTempDB(path, "gnx", *flagPartition)
		if err != nil {
			log.Println("gnx.CreateTempDB() failed: err =", err)
			return nil, "", err
		}
		return db, dir, err
	}
	if *flagNew {
		db, err := gnx.CreateDB(path, *flagPartition)
		if err != nil {
			log.Println("gnx.CreateDB() failed: err =", err)
			return nil, "", err
		}
		return db, "", err
	}
	db, err := gnx.OpenDB(path)
	if err != nil {
		log.Println("gnx.OpenDB() failed: err =", err)
		return nil, "", err
	}
	return db, "", err
}

func consoleMain() int {
	// Parse command-line options.
	flag.Parse()
	if *flagProfile != "" {
		file, err := os.Create(*flagProfile)
		if err != nil {
			log.Println("profile failed: err =", err)
			return 1
		}
		pprof.StartCPUProfile(file)
		defer pprof.StopCPUProfile()
	}

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
//		name, options, err := db.ParseCommand(command)
//		fmt.Println("name:", name)
//		fmt.Println("options:", options)
		bytes, err := db.Query(command)
		if err != nil {
			log.Println("query error: err =", err)
		} else {
			fmt.Println(string(bytes))
		}
	}
	return 0
}

func main() {
	os.Exit(consoleMain())
}
