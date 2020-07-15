package ssh

import (
	"fmt"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"strings"
	"time"
)

type SSHTool struct {
	host        string
	port        string
	username    string
	password    string
	privatePath string
}

//NewSSHTool returned *SSHTool
func NewSSHTool(hostname, username, password string, privatePath string) (*SSHTool, error) {
	//get host port
	var host, port string
	hosts := strings.Split(hostname, ":")
	if len(hosts) == 1 {
		host = hosts[0]
		port = "22"
	} else if len(hosts) == 2 {
		host = hosts[0]
		port = hosts[1]
	} else {
		return nil, fmt.Errorf("ssh host(%s) invalid", hostname)
	}
	//object
	tool := SSHTool{
		host:        host,
		port:        port,
		username:    username,
		password:    password,
		privatePath: privatePath,
	}
	return &tool, nil
}

//SSHTool Connect  returned *ssh.Client which can be used for proxy
func (tool *SSHTool) Connect() (conn *ssh.Client, err error) {
	//tool config
	var conf *ssh.ClientConfig
	if tool.password != "" {
		conf, err = makePasswordConfig(tool.username, tool.password)
		if err != nil {
			return nil, fmt.Errorf("ssh makePasswordConfig err:%s", err.Error())
		}
	} else if tool.privatePath != "" {
		//get private key
		content, err := ioutil.ReadFile(tool.privatePath)
		if err != nil {
			return nil, fmt.Errorf("ssh read privateKey error. path(%s),err:%s", tool.privatePath, err.Error())
		}
		conf, err = makePrivateKeyConfig(tool.username, string(content))
		if err != nil {
			return nil, fmt.Errorf("ssh makePrivateKeyConfig err:%s", err.Error())
		}
	} else {
		return nil, fmt.Errorf("ssh password and privateKey both empty")
	}
	//dial
	conn, err = ssh.Dial("tcp", fmt.Sprintf("%s:%s", tool.host, tool.port), conf)
	if err != nil {
		return nil, fmt.Errorf("ssh dial err:%s", err.Error())
	}
	return conn, nil
}

func makePasswordConfig(username string, password string) (*ssh.ClientConfig, error) {
	conf := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(5) * time.Second,
	}
	return conf, nil
}

func makePrivateKeyConfig(username string, privateKey string) (*ssh.ClientConfig, error) {
	signer, err := ssh.ParsePrivateKey([]byte(privateKey))
	if err != nil {
		return nil, fmt.Errorf("ssh ParsePrivateKey err:%s", err.Error())
	}
	conf := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Duration(5) * time.Second,
	}
	return conf, nil
}
