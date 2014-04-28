package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
)

var (
	NUMOUTPUTS = flag.Int("n", 1, "Number of outputs")
)

func main() {
	flag.Parse()
	args := flag.Args()
	outPath := args[0]
	processes := make([]*exec.Cmd, *NUMOUTPUTS, *NUMOUTPUTS)
	outputs := make([]*bufio.Writer, *NUMOUTPUTS, *NUMOUTPUTS)
	outputsRaw := make([]io.WriteCloser, *NUMOUTPUTS, *NUMOUTPUTS)
	for i := 0; i < *NUMOUTPUTS; i++ {
		processes[i] = exec.Command("sh", fmt.Sprintf("\"sort -k1,1 > %s.%d\"", outPath, i))
		out, err := processes[i].StdinPipe()
		if err != nil {
			panic(err)
		}
		outputsRaw[i] = out
		outputs[i] = bufio.NewWriter(out)
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
		}
		key := line[:tabIdx]
		hash := 0
		for b, _ := range key {
			hash += hash*31 + b
		}
		_, err := outputs[hash%len(outputs)].Write(line)
		if err != nil {
			panic(err)
		}
		err = outputs[hash%len(outputs)].WriteByte('\n')
	}
	for _, o := range outputs {
		o.Flush()
	}
	for _, o := range outputsRaw {
		o.Close()
	}
	for _, p := range processes {
		p.Wait()
	}
}
