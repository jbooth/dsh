Tools for distributed operation on files from the  [maggiefs](http://github.com/jbooth/maggiefs/) distributed filesystem.

We split files and dispatch work to the node the files are located at using ssh, either by ssh-agent or by key files in $HOME/.ssh.


Dmap
----

    dmap inputFile [inputFile ...] command

Splits a file on the line breaks closest to block boundaries and dispatches one task per block, which will be piped into the provided command.  Stdout and stderr from the command, if not redirected on the shell, will be communicated back over the network to the launching process.  Commands will have the environment variable TASK_ID populated to a different value for each subprocess.


    dmap /mfs/hayStack.txt "grep needle"   # writes matching lines to stdout on the launching dmap process
  
    dmap /mfs/toSplit.txt "cat > /mfs/outFolder/\$TASK_ID.out"  # each task will write to its own file  
  
    dmap /mfs/toSplit.txt "/mfs/bin/myProgram"  # files stored on maggiefs can be executable, too  
  
  

Dmr
---

    dmr [-numReduce N] inFile:mapCmd [inFile:mapCmd ...] reduceCmd outputDir
    
Executes a mapreduce job.  For each inFile glob, we break the file into line-separated chunks as in dmap.  We then pipe it through its provided mapCmd and partition output on the portion of text prior to the first tab character, or the whole line if no tab is present.  We then join each partition's output from each map join, sort them to ensure like keys are adjacent, and pipe that sorted partition through the reduceCommand, creating a file in outputDir for each reducer run in parallel.

For example, if we wanted to count occurences of words in a file, the 1-liner

    dmr /tmp/mfs/mfs2.*:"sed 's/\s\+/\n/g'" "uniq -c" /tmp/mfs/output
    
would, during the map phase, split all whitespace into line breaks, putting each word on it's own line.  Our output is sorted before arriving at our reduce phase, where we count the occurences of each word.  

