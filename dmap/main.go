package main

import (
	"filepath"
	"flag"
	"fmt"
	"github.com/jbooth/dsh"
	"os"
)

var (
	USER    = flag.String("u", os.Getenv("USER"), "ssh username, defaults to current user")
	KEYFILE = flag.String("i", "", "private key file, defaults to ~/.ssh/id_*")
)

// args are a list of filenames/globs (or directories) followed by a command
func main() {
	flag.Parse()
	args := flag.Args()
	fmt.Printf("u: %s k %s args %+v", *USER, *KEYFILE, args)
	if len(args < 2) {
		log.Printf("Expected at least 2 args, <path..> <cmd>")
		os.Exit(1)
	}
	globs := args[:len(args)-2]
	files := make([]string)
	for _, g := range globs {
		matches, err := filepath.Glob(g)
		if err != nil {
			log.Printf("Error globbing path %s : %s", g, err)
		}
	}
	sshConfig, err := dsh.SshConfig(USER, KEYFILE)
	if err != nil {
		log.Printf("Error getting ssh config: %s", err)
		os.Exit(1)
	}

}
