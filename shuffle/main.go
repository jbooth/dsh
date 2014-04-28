package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
)

// usage:  shuffle <numOutputs> <outputTemplate>
// hashes stdin on first field and sorts, prior to outputting to an output based on outputTemplate.OUTNUM
func main() {
	flag.Parse()
	args := flag.Args()
	numOutputs, err := strconv.Atoi(args[0])
	if err != nil {
		panic(err)
	}
	baseOutPath := args[1]
	processes := make([]*exec.Cmd, numOutputs, numOutputs)
	sortInputs := make([]*bufio.Writer, numOutputs, numOutputs)
	sortInputsRaw := make([]io.WriteCloser, numOutputs, numOutputs)
	waitAllDone := new(sync.WaitGroup)
	log.Printf("Shuffle running with numOutputs %d output %s", numOutputs, baseOutPath)
	// one day we'll do this sort in-process.  maybe.
	for i := 0; i < numOutputs; i++ {
		log.Printf("Forking sort command\n")
		processes[i] = exec.Command("sort", "-t\t", "-k1,1", "-")
		in, err := processes[i].StdinPipe()
		if err != nil {
			panic(err)
		}
		sortInputsRaw[i] = in
		sortInputs[i] = bufio.NewWriter(in)
		procOut, err := processes[i].StdoutPipe()
		if err != nil {
			panic(err)
		}
		err = processes[i].Start()
		if err != nil {
			panic(err)
		}
		// sort is started, now launch goroutine to funnel sorted data to final destination
		waitAllDone.Add(1)
		go func(in io.Reader, outPath string, wg *sync.WaitGroup) {
			out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0755)
			if err != nil {
				panic(err)
			}
			_, err = io.Copy(out, in)
			if err != nil {
				panic(err)
			}
			out.Close()
			wg.Done()
		}(procOut, fmt.Sprintf("%s.%d", baseOutPath, i), waitAllDone)
	}
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		line := in.Bytes()
		tabIdx := -1
		for idx, b := range line {
			if b == '\t' {
				tabIdx = idx
				break
			}
		}
		if tabIdx < 0 {
			log.Printf("Line %s has no tab to split on!", string(line))
			continue
		}
		key := line[:tabIdx]
		hash := uint32(7)
		for _, b := range key {
			hash = hash*31 + uint32(b)
		}
		//log.Printf("Got key %s hash %d for line %s, writing to output %d", key, hash, line, int(hash)%len(outputs))
		_, err := sortInputs[int(hash)%len(sortInputs)].Write(line)
		if err != nil {
			panic(err)
		}
		err = sortInputs[int(hash)%len(sortInputs)].WriteByte('\n')
		if err != nil {
			panic(err)
		}
	}
	for _, o := range sortInputs {
		err = o.Flush()
		if err != nil {
			panic(err)
		}
	}
	for _, o := range sortInputsRaw {
		err = o.Close()
		if err != nil {
			panic(err)
		}
	}
	for _, p := range processes {
		err = p.Wait()
		if err != nil {
			panic(err)
		}
	}
	waitAllDone.Wait()
}
