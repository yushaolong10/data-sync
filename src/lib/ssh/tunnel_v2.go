package ssh

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"
)

type TunnelServerV2 struct {
	ctx           context.Context
	localHost     string
	remoteHost    string
	localListener net.Listener
	sshCtl        *sshController
}

//use ssh connection pool
func NewTunnelServerWithPool(ctx context.Context, localHost, remoteHost string, sshTool *SSHTool) (*TunnelServerV2, error) {
	//check local host
	listener, err := net.Listen("tcp", localHost)
	if err != nil {
		return nil, fmt.Errorf("localHost(%s) listen err:%s", localHost, err.Error())
	}
	//new ssh controller
	sshCtl := NewSshController(ctx, sshTool, remoteHost)
	if err := sshCtl.Run(); err != nil {
		return nil, err
	}
	//object
	server := &TunnelServerV2{
		ctx:           ctx,
		localHost:     localHost,
		localListener: listener,
		remoteHost:    remoteHost,
		sshCtl:        sshCtl,
	}
	return server, nil
}

func (server *TunnelServerV2) isCanceled() bool {
	return server.ctx.Err() != nil
}

func (server *TunnelServerV2) Start() {
	//monitor
	go server.monitor()
	//loop
	for {
		//accept
		conn, err := server.localListener.Accept()
		if err != nil {
			log.Printf("[error][TunnelServer.localListener] accept err:%s", err.Error())
			//not canceled
			if !server.isCanceled() {
				time.Sleep(time.Second * 3)
				continue
			}
			//canceled
			break
		}
		go server.handleForward(server.ctx, conn)
	}
}

func (server *TunnelServerV2) monitor() {
	for {
		//context is canceled
		if server.isCanceled() {
			log.Printf("[info][TunnelServer.monitor] context canceled")
			if err := server.localListener.Close(); err != nil {
				log.Printf("[error][TunnelServer.monitor] server close err:%s", err.Error())
			}
			break
		}
		time.Sleep(time.Second * 3)
	}
}

func (server *TunnelServerV2) handleForward(ctx context.Context, localConn net.Conn) {
	remoteConn, err := server.sshCtl.GetNetConn()
	if err != nil {
		log.Printf("[error][TunnelServer.handleForward] sshCtl GetNetConn err:%s", err.Error())
		return
	}
	//two node
	connNodeLocal := newConnNode(ctx, "local-netConn", localConn)
	connNodeRemote := newConnNode(ctx, "remote-netConn", remoteConn)
	//routine
	go connNodeLocal.WatchPeer(connNodeRemote)
	go connNodeRemote.WatchPeer(connNodeLocal)
}
