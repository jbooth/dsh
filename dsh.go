package dsh

import (
	"bufio"
	"code.google.com/p/go.crypto/ssh"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type Split struct {
	FilePath string
	Host     string
	StartIdx int64
	EndIdx   int64
}

// returns commands which will work on a particular series of splits by using tail -c and head -c
// commands set the environment variable $TASK_ID so that it will be unique per invocation
func Commands(fileSplits []Split, cmd string) []HostCmd {
	ret := make([]Command, len(fileSplits), len(fileSplits))
	for idx, split := range fileSplits {
		length := split.EndIdx - split.StartIdx
		ret[idx] = HostCmd{
			Host: split.Host,
			Cmd: fmt.Sprintf("export TASK_ID=%d; tail -c +%d | head -c %d | %s",idx+1,split.StartIdx,length,cmd)
		}
	}
	return ret
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
	Host string
	Cmd  string
}

// execs multiple commands, each in their goroutine, and pipes all output
// line-by-line to the provided stderr and stdout streams
// guarantees that each line arrives whole, returns after all commands have completed
func ExecShells(sshcfg *ssh.ClientConfig, commands []HostCmd, stdout io.Writer, stderr io.Writer) error {
	var wg sync.WaitGroup
	outBuff := make(chan string, 100)
	errBuff := make(chan string, 100)
	// fork the commands
	for _, cmd := range commands {
		wg.Add(1)
		go func() {
			// decrement waitgroup when done
			defer wg.Done()
			// connect ssh
			cli, err := ssh.Dial("tcp4", cmd.Host, sshcfg)
			if err != nil {
				log.Printf("Error connecting to host %s : %s", cmd.Host, err)
				return
			}
			sesh, err := cli.NewSession()
			if err != nil {
				log.Printf("Error obtaining session on host %s : %s", cmd.Host, err)
				return
			}
			// pipe outputs
			go func() {
				seshOut, err := sesh.StdoutPipe()
				if err != nil {
					log.Printf("Error obtaining session stdout on host %s : %s", cmd.Host, err)
					return
				}
				readLinesToChan(seshOut, nil, outBuff)
			}()
			go func() {
				seshOut, err := sesh.StderrPipe()
				if err != nil {
					log.Printf("Error obtaining session stderr on host %s : %s", cmd.Host, err)
					return
				}
				readLinesToChan(seshOut, fmt.Sprintf("%s: ", cmd.Host), errBuff)
			}()
			// issue command with proper env
			toExec := fmt.Sprintf("if [ -f ~/.bashrc ]; then source ~/.bashrc ; fi; %s", cmd.Cmd)
			err := sesh.Run(toExec)
			if err != nil {
				log.Printf("Error running command %s on host %s", toExec, cmd.Host)
			}
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
	wg.Wait()
	return nil
}

func readLinesToChan(in io.Reader, linePrefix string, out chan string) {
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := scanner.Text()
		if linePrefix != nil {
			line = linePrefix + line
		}
		out <- line
	}
}
