package dsh

import (
	"code.google.com/p/go.crypto/ssh"
	"ioutil"
	"os"
)

func sshCommand(host string, cmd string, sshConf *ssh.ClientConfig, outLines chan string, errLines chan string) error {

}

// generates an ssh config that attempt ssh-agents and then authorizes from keyFile
// if keyFile is nil, we'll search for the usual suspects
// (~/.ssh/id_rsa, id_dsa)
func SshConf(user string, keyFile string) *ssh.ClientConfig {

	var auths []ssh.ClientAuth
	if agent, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		auths = append(auths, ssh.ClientAuthAgent(ssh.NewAgentClient(agent)))
	}

	var keyFiles []string
	if keyFile != nil {
		keyFiles = []string{keyFile}
	} else {
		keyFiles = lookupKeyFiles()
	}
}

func lookupKeyFiles() []string {
	ret := make([]string, 0, 0)
	home := os.Getenv("HOME")
	if home == nil {
		return ret
	}
	sshDir := fmt.Sprintf("%s/.ssh", home)
	rsaKey := sshDir + "/id_rsa"

}
