package dsh

import (
	"code.google.com/p/go.crypto/ssh"
	"code.google.com/p/go.crypto/ssh/agent"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
)

// generates an ssh config that attempt ssh-agents and then authorizes from keyFile
// if keyFile is nil, we'll search for the usual suspects
// (~/.ssh/id_rsa, id_dsa)
func SshConf(user string, keyFile string) *ssh.ClientConfig {

	var auths []ssh.AuthMethod
	// ssh-agent auth goes first
	if agentPipe, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		ag := agent.NewClient(agentPipe)
		agentSigners, err := ag.Signers()
		if err != nil {
			log.Printf("Error pulling signers from ssh-agent: %s", err)
		} else {
			if len(agentSigners) > 0 {
				auths = append(auths, ssh.PublicKeys(agentSigners...))
			}
		}
	}

	// provided keyfile or default keyfiles
	var keyFiles []string
	if keyFile != "" {
		keyFiles = []string{keyFile}
	} else {
		keyFiles = lookupKeyFiles()
	}
	signers := make([]ssh.Signer, 0, 0)
	for _, keyFile := range keyFiles {
		keyFileH, err := os.Open(keyFile)
		if err != nil {
			log.Printf("Error opening keyFile %s : %s", keyFile, err)
			continue
		}
		keyBytes, err := ioutil.ReadAll(keyFileH)
		keyFileH.Close()
		if err != nil {
			log.Printf("Error reading keyFile %s, skipping : %s", keyFile, err)
			continue
		}
		signer, err := ssh.ParsePrivateKey(keyBytes)
		if err != nil {
			log.Printf("Error parsing keyFile contents from %s, skipping: %s", keyFile, err)
		}
		signers = append(signers, signer)
	}
	auths = append(auths, ssh.PublicKeys(signers...))
	return &ssh.ClientConfig{
		User: user,
		Auth: auths,
	}
}

func lookupKeyFiles() []string {
	ret := make([]string, 0, 0)
	home := os.Getenv("HOME")
	if home == "" {
		return ret
	}
	sshDir := fmt.Sprintf("%s/.ssh", home)
	rsaKey := sshDir + "/id_rsa"
	if keyexists(rsaKey) {
		ret = append(ret, rsaKey)
	}
	dsaKey := sshDir + "/id_dsa"
	if keyexists(dsaKey) {
		ret = append(ret, dsaKey)
	}
	ecdsaKey := sshDir + "/id_ecdsa"
	if keyexists(ecdsaKey) {
		ret = append(ret, ecdsaKey)
	}
	return ret

}

func keyexists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	log.Printf("Error examining private key: %s", err)
	return false
}
