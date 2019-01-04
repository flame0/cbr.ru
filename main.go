package main

import (
	"fmt"
	"github.com/akamensky/argparse"
	"log"
	"os"
	"time"
)

const formsUrl = "http://www.cbr.ru/vfs/credit/forms/"

func main() {
	// Create new parser object
	parser := argparse.NewParser("print", "CBR.RU FORMS")
	// Create string flag
	gte := parser.String("g", "date-gte", &argparse.Options{Required: false, Help: "Find zip for date >= than"})
	lte := parser.String("l", "date-lte", &argparse.Options{Required: false, Help: "Find zip for date <= than"})
	startCmd := parser.NewCommand("download", "Will start a process")
	unpackCmd := parser.NewCommand("unpack", "Will start a unpack zip rar")

	// Parse input
	err := parser.Parse(os.Args)
	if err != nil {
		// In case of error print error and print usage
		// This can also be done by passing -h or --help flags
		fmt.Print(parser.Usage(err))
		return
	}
	// Finally print the collected string

	if startCmd.Happened() {
		start := time.Now()

		downloadAll(*gte, *lte)
		elapsed := time.Since(start)
		log.Printf("took %s", elapsed)
	} else if unpackCmd.Happened() {
		start := time.Now()
		fmt.Println("Start unpacking...")
		unpack()
		elapsed := time.Since(start)
		log.Printf("took %s", elapsed)
	}

}
