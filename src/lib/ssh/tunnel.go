package ssh

import (
	"context"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"log"
	"net"
	"time"
)

type TunnelServer struct {
	ctx           context.Context
	localHost     string
	remoteHost    string
	localListener net.Listener
	sshProxy      *ssh.Client
}

func NewTunnelServer(ctx context.Context, localHost, remoteHost string, sshTool *SSHTool) (*TunnelServer, error) {
	//check local host
	listener, err := net.Listen("tcp", localHost)
	if err != nil {
		return nil, fmt.Errorf("localHost(%s) listen err:%s", localHost, err.Error())
	}
	//open ssh tunnel
	sshClient, err := sshTool.Connect()
	if err != nil {
		return nil, fmt.Errorf("sshTool.Connect err:%s", err.Error())
	}
	//check ssh remote host
	testRemoteConn, err := sshClient.Dial("tcp", remoteHost)
	if err != nil {
		return nil, fmt.Errorf("ssh dial remoteHost(%s) err:%s", remoteHost, err.Error())
	}
	testRemoteConn.Close()
	//object
	server := &TunnelServer{
		ctx:           ctx,
		localHost:     localHost,
		localListener: listener,
		remoteHost:    remoteHost,
		sshProxy:      sshClient,
	}
	return server, nil
}

func (server *TunnelServer) Start() {
	//server state
	go server.monitor()
	//loop
	for {
		//accept
		conn, err := server.localListener.Accept()
		if err != nil {
			log.Printf("[error][TunnelServer.localListener] accept err:%s", err.Error())
			break
		}
		go server.handleForward(server.ctx, conn)
	}
}

func (server *TunnelServer) monitor() {
	for {
		//context is canceled
		if server.ctx.Err() != nil {
			log.Printf("[info][TunnelServer.monitor] context canceled")
			if err := server.close(); err != nil {
				log.Printf("[error][TunnelServer.monitor] server close err:%s", err.Error())
			}
			break
		}
		time.Sleep(time.Second*5)
	}
}

func (server *TunnelServer) close() error {
	if err := server.sshProxy.Close(); err != nil {
		return fmt.Errorf("TunnelServer sshProxy close err:%s", err.Error())
	}
	if err := server.localListener.Close(); err != nil {
		return fmt.Errorf("TunnelServer localListener close err:%s", err.Error())
	}
	return nil
}

func (server *TunnelServer) handleForward(ctx context.Context, localConn net.Conn) {
	remoteConn, err := server.sshProxy.Dial("tcp", server.remoteHost)
	if err != nil {
		log.Printf("[error][TunnelServer.handleForward] ssh dial remote host(%s) err:%s", server.remoteHost, err.Error())
		return
	}
	//two node
	connNodeLocal := newConnNode(ctx, "local-conn", localConn)
	connNodeRemote := newConnNode(ctx, "remote-conn", remoteConn)
	//routine
	go connNodeLocal.WatchPeer(connNodeRemote)
	go connNodeRemote.WatchPeer(connNodeLocal)
}

func newConnNode(ctx context.Context, name string, netConn net.Conn) *connNode {
	return &connNode{
		ctx:     ctx,
		name:    name,
		netConn: netConn,
	}
}

type connNode struct {
	ctx     context.Context
	name    string
	netConn net.Conn
}

func (node *connNode) Close() error {
	return node.netConn.Close()
}

func (node *connNode) WatchPeer(peer *connNode) {
	_, err := io.Copy(node.netConn, peer.netConn)
	if err != nil {
		log.Printf("[error][connNode.WatchPeer] name:%s, io.Copy err:%s", node.name, err.Error())
	}
	err = node.Close()
	if err != nil {
		log.Printf("[error][connNode.WatchPeer] name:%s, Close err:%s", node.name, err.Error())
	}
}
