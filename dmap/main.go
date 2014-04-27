package main

import (
	"flag"
	"github.com/jbooth/dsh"
	"log"
	"os"
	"path/filepath"
)

var (
	USER    = flag.String("u", os.Getenv("USER"), "ssh username, defaults to current user")
	KEYFILE = flag.String("i", "", "private key file, defaults to ~/.ssh/id_*")
)

// args are a list of filenames/globs (or directories) followed by a command
func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		log.Printf("Expected at least 2 args, <path..> <cmd>")
		os.Exit(1)
	}
	var globs []string
	if len(args) == 2 {
		globs = []string{args[0]}
	} else {
		globs = args[:len(args)-2]
	}
	files := make([]string, 0)
	for _, g := range globs {
		matches, err := filepath.Glob(g)
		if err != nil {
			log.Printf("Error globbing path %s : %s", g, err)
		}
		files = append(files, matches...)
	}
	splits, err := dsh.GetSplits(files)
	if err != nil {
		log.Printf("Error calculation splits for files %+v : %s", files, err)
		os.Exit(1)
	}
	cmd := args[len(args)-1]
	splitCmds := dsh.Commands(splits, cmd)
	sshConfig := dsh.SshConf(*USER, *KEYFILE)
	if err != nil {
		log.Printf("Error getting ssh config: %s", err)
		os.Exit(1)
	}
	err = dsh.ExecShells(sshConfig, splitCmds, os.Stdout, os.Stderr)
	if err != nil {
		log.Printf("Error execing commands %s", err)
		os.Exit(1)
	}
}
