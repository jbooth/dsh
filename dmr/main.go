package main

import (
	"flag"
	//"github.com/jbooth/dsh"
	"fmt"
	"log"
	"os"
)

var (
	USER    = flag.String("u", os.Getenv("USER"), "ssh username, defaults to current user")
	KEYFILE = flag.String("i", "", "private key file, defaults to ~/.ssh/id_*")
)

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 3 {
		log.Printf("Need at least 3 args:  <fileToMap:mapScript>... <reduceScript> <outDir>")
	}
	mapTasks := args[:len(args)-2]
	reduceScript := args[len(args)-2]
	outDir := args[len(args)-1]

	fmt.Printf("Args: %+v\n", args)
	fmt.Printf("maps: %+v\n", mapTasks)
	fmt.Printf("reduce script %s\n", reduceScript)
	fmt.Printf("outdir %s\n", outDir)
}
