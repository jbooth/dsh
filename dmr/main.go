package main

import (
	"flag"
	"fmt"
	"github.com/jbooth/dsh"
	"log"
	"os"
	"path/filepath"
	"strings"
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

	// dispatch map tasks piped through shuffle
	for _, taskStr := range mapTasks {
		taskSplits := strings.Split(taskStr, ":")

	}
	// for each reduce task

	// collect list of files

	// dispatch a single task to fileSplit from first file
	// task pulls in sort -m from all files

}

// returns commands which will work on a particular series of splits by using tail -c and head -c
// commands set the environment variable $TASK_ID so that it will be unique per invocation
// commands will be piped through our shuffle command
var taskId = 0

func mapCommands(fileSplits []dsh.Split, cmd string, numReducers int, outDir string) []dsh.HostCmd {
	ret := make([]dsh.HostCmd, len(fileSplits), len(fileSplits))
	for idx, split := range fileSplits {
		taskId++
		length := split.EndIdx - split.StartIdx
		// outputs will look like .mapOut_0.0, .mapOut_0.1, .mapOut_1.0...
		outTemplate := fmt.Sprintf("%s/.mapOut_%d", outDir)
		ret[idx] = dsh.HostCmd{
			Host: split.Host,
			Cmd:  fmt.Sprintf("export TASK_ID=%d; tail -c +%d %s | head -c %d | %s | shuffle %d %s", taskId, split.StartIdx, split.FilePath, length, cmd, numReducers, outTemplate),
		}
		fmt.Printf("Made cmd %+v from split %+v\n", ret[idx], split)
	}
	return ret
}

func reduceCommands(outDir string, numReducers int, cmd string) ([]dsh.HostCmd, error) {
	ret := make([]dsh.HostCmd, numReducers, numReducers)
	for i := 0; i < numReducers; i++ {
		reduceGlob := fmt.Sprintf("%s/.mapOut_*.%d", outDir, i)
		reduceInputs, err := filepath.Glob(reduceGlob)
		if err != nil {
			return nil, err
		}
		if len(reduceInputs == 0) {
			return nil, fmt.Errorf("No files found for glob %s", reduceGlob)
		}
		splits, err := dsh.GetFileSplits(reduceInputs[0])
		if err != nil {
			return nil, err
		}
		host := splits[0].Host
		sortArg := strings.Join(reduceInputs, " ")
		ret[i] = dsh.HostCmd{
			Host: host,
			Cmd:  fmt.Sprintf("export TASK_ID=%d; sort --batch-size=%d -m %s | %s > %s/reduceOut.%d", i, len(reduceInputs)+1, sortArg, cmd, outDir, i),
		}
	}
	return ret
}
