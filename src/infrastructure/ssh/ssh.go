package ssh

import (
	"fmt"
	"infrastructure/config"
	"infrastructure/context"
	"lib/logger"
	"lib/routine"
	"lib/ssh"
)

func OpenSSHTunnel(ctx *context.BaseContext, username string, localAddr string, remoteAddr string) error {
	conf, err := findSShConfByUsername(username)
	if err != nil {
		logger.Error("[ssh.OpenSSHTunnel] findSShConfByUsername err:%s", err.Error())
		return err
	}
	//create ssh tool
	sshTool, err := ssh.NewSSHTool(conf.BoardHost, conf.UserName, conf.Password, conf.PrivatePath)
	if err != nil {
		logger.Error("[ssh.OpenSSHTunnel] NewSSHTool err:%s", err.Error())
		return err
	}
	//tunnel server
	proxyServer, err := ssh.NewTunnelServer(ctx.RootCtx, localAddr, remoteAddr, sshTool)
	if err != nil {
		logger.Error("[ssh.OpenSSHTunnel] NewForwardServer err:%s", err.Error())
		return err
	}
	//run ssh proxy
	routine.Go(proxyServer.Start)
	return nil
}

func findSShConfByUsername(username string) (config.SShConfig, error) {
	var emptyConf config.SShConfig
	if len(config.Conf.SSh) == 0 {
		return emptyConf, fmt.Errorf("ssh config empty")
	}
	conf, ok := config.Conf.SSh[username]
	if !ok {
		return emptyConf, fmt.Errorf("not found ssh config by username(%s)", username)
	}
	return conf, nil
}
