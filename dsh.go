package dsh

import (
	"code.google.com/p/go.crypto/ssh"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type Split struct {
	filePath string
	host     string
	startIdx int64
	endIdx   int64
}

func GetSplits(filePaths []string) ([]Split, error) {
	ret := make([]Split, 0, 0)
	for _, f := range filePaths {
		ret = append(ret, GetFileSplits(f)...)
	}
	return ret
}

// gets splits which are aligned with the nearest linebreak to a file boundary
func GetFileSplits(filePath string) ([]Split, error) {
	// open file and get size
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("Error opening %s for read : %s", filePath, err)
	}
	defer f.Close()
	stat, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("Error getting stat for file %s : %s", filePath, err)
	}
	fileSize := stat.Size()

	// list blocks
	blocLocBytes := make([]byte, 64*1024, 64*1024)
	bytesLen, err := syscall.Getxattr(filePath, "user.mfs.blockLocs", blocLocBytes)
	if err != nil {
		return nil, err
	}
	blocLocStr := string(blocLocBytes[:bytesLen])
	lines := strings.Split(blocLocStr, "\n")

	ret := make([]Split, 0, len(lines))
	// find first linebreak after each to make splits
	for _, line := range lines {
		lineSplits := strings.split(line, "\t")
		if len(lineSplits) < 3 {
			return nil, fmt.Errorf("Error, improperly formatted blockLocs line (less than 3 elements): %s", line)
		}
		blockStartPos, err := strconv.ParseInt(lineSplits[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Error, blockStartPos not a number! %s : %s", err, line)
		}
		blockEndPos, err := strconv.ParseInt(lineSplits[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Error, blockEndPos not a number! %s : %s", err, line)
		}
		if blockStartPos > 0 {
			blockStartPos, err = nextLineBreak(f, blockStartPos)
			if err != nil {
				return nil, err
			}
		}
		if blockEndPos < fileSize {
			blockEndPos, err = nextLineBreak(f, blockEndPos)
			if err != nil {
				return nil, err
			}
		}
		blockHosts := split(lineSplits[2], ",")
		if len(blockHosts == 0) {

		}
		ret = append(ret, Split{filePath, blockHosts[0], blockStartPos, blockEndPos})
	}
	return ret
}

func nextLineBreak(f *os.File, fromPos int64) (int64, error) {
	newOff, err := f.Seek(fromPos, 0)
	if newOff != fromPos || err != nil {
		return fmt.Errorf("Error seeking to %d : %s", blockStartPos, err)
	}
	buf := make([]byte, 4096)
	currPos := fromPos
	for {
		n, err := f.Read(buf)
		if err != nil {
			if err == io.EOF {
				return currPos + n, nil
			}
			return 0, err
		}
		for i := 0; i < n; i++ {
			if buf[i] == '\n' {
				return currPos + i, nil
			}
		}
		currPos += n
	}
	return currPos, nil
}

type HostCmd struct {
	host string
	cmd  string
}

// execs multiple commands, each in their goroutine, and pipes all output
// line-by-line to the provided stderr and stdout streams
// guarantees that each line arrives whole, and returns after all commands have completed
// returns error if any command finishes with bad result
func ExecShells(commands []HostCmd, stdout io.Writer, stderr io.Writer) error {
	var wg sync.WaitGroup
	outBuff := make(chan string, 100)
	errBuff := make(chan string, 100)
	for _, cmd := range commands {
		wg.Add(1)
		go func() {

		}()
	}
	outDone := make(chan bool)
	errDone := make(chan bool)
	go func() {
		for line := range outBuff {
			stdout.WriteString(line)
		}
		outDone <- true
	}()
	go func() {
		for line := range errBuff {
			stderr.WriteString(line)
		}
		errDone <- true
	}()
	return nil
}
