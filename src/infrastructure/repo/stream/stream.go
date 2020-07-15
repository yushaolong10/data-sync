package stream

import (
	"fmt"
	"infrastructure/config"
	"infrastructure/context"
	"infrastructure/drivers"
	"infrastructure/ssh"
	"lib/logger"
)

type StreamRepo interface {
	Read(HandleReadFunc) (interface{}, error)
	Write(HandleWriteFunc) error
}

type HandleReadFunc func(engine *drivers.MysqlEngine) (interface{}, error)

type HandleWriteFunc func(engine *drivers.MysqlEngine) error

func BuildMysqlStreamRepo(ctx *context.BaseContext, database string, conf config.MysqlMachineConfig) (StreamRepo, error) {
	//raw host,port
	host := conf.DbHost
	port := conf.DbPort
	if conf.EnableSSh {
		sshConf := conf.MysqlSSh
		//local addr
		localAddr := fmt.Sprintf("%s:%d", sshConf.TunnelHost, sshConf.TunnelPort)
		//remote addr
		remoteAddr := fmt.Sprintf("%s:%d", host, port)
		//open tunnel
		if err := ssh.OpenSSHTunnel(ctx, sshConf.SShUsername, localAddr, remoteAddr); err != nil {
			logger.Error("[BuildMysqlStreamRepo] OpenSSHTunnel err:%s", err.Error())
			return nil, err
		}
		//ssh proxy local host,port
		host = sshConf.TunnelHost
		port = sshConf.TunnelPort
	}
	//new mysql engine
	conn, err := drivers.NewCustomMysqlEngine(host, port, conf.DbUsername, conf.DbPassword, database)
	if err != nil {
		logger.Error("[BuildMysqlStreamRepo] NewCustomMysqlEngine err:%s", err.Error())
		return nil, err
	}
	return &MysqlStreamImpl{ctx: ctx, database: database, conn: conn}, nil
}
