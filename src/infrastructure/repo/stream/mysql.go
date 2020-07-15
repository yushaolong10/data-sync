package stream

import (
	"infrastructure/context"
	"infrastructure/drivers"
)

type MysqlStreamImpl struct {
	ctx      *context.BaseContext
	conn     *drivers.MysqlEngine
	database string
}

func (srv *MysqlStreamImpl) Read(fn HandleReadFunc) (ret interface{}, err error) {
	return fn(srv.conn)
}

func (srv *MysqlStreamImpl) Write(fn HandleWriteFunc) (err error) {
	return fn(srv.conn)
}
