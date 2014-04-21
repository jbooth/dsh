package main

import (
	"github.com/jbooth/dsh"
)

var (
	
var (
	USER = flag.String("l", os.Getenv("USER"), "ssh username, defaults to current user")
	KEYFILE = flag.String("i",fmt.Sprintf("%s/.ssh/id_rsa","private key file, defaults to ~/.ssh/id_rsa")
)

func init() {
	flag.StringVar(&user,"u",)
}

// args are a list of filenames/globs (or directories) followed by a command
func main() {

}

