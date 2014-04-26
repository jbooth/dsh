package dsh

import (
	"code.google.com/p/go.crypto/ssh"
	"code.google.com/p/go.crypto/ssh/agent"
	"io/ioutil"
	"os"
)

// generates an ssh config that attempt ssh-agents and then authorizes from keyFile
// if keyFile is nil, we'll search for the usual suspects
// (~/.ssh/id_rsa, id_dsa)
func SshConf(user string, keyFile string) *ssh.ClientConfig {

	var auths []ssh.ClientAuth
	// ssh-agent auth goes first
	if agentPipe, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		auths = append(auths, agent.ClientAuthAgent(agent.NewAgentClient(agentPipe)))
	}

	// provided keyfile or default keyfiles
	var keyFiles []string
	if keyFile != nil && keyFile != "" {
		keyFiles = []string{keyFile}
	} else {
		keyFiles = lookupKeyFiles()
	}
	signers := make(ssh.Signer, 0, 0)
	for _, keyFile := range keyFiles {
		keyBytes, err := ioutil.ReadAll(keyFile)
		if err != nil {
			log.Printf("Error reading keyFile %s, skipping : %s", keyFile, err)
			continue
		}
		signer, err := ssh.ParsePrivateKey(keyBytes)
		if err != nil {
			log.Printf("Error parsing keyFile contents from %s, skipping: %s", keyFilem, err)
		}
		signers = append(signers, signer)
	}
	auths = append(auths, ssh.PublicKeys(signers))
	return &ssh.ClientConfig {
		user,
		auths
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
