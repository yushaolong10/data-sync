package service

import (
	"fmt"
	"infrastructure/context"
	"infrastructure/dao"
	"infrastructure/drivers"
	"infrastructure/monitor"
	"infrastructure/repo/stream"
	"lib/logger"
	"lib/util"
	"runtime/debug"
	"strconv"
	"time"
)

type MysqlStreamService struct {
	ctx *context.BizContext
}

func NewMysqlStreamService(ctx *context.BizContext) *MysqlStreamService {
	return &MysqlStreamService{ctx: ctx}
}

func (srv *MysqlStreamService) ShowTables() stream.HandleReadFunc {
	return func(engine *drivers.MysqlEngine) (ret interface{}, err error) {
		defer func(begin time.Time) {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic")
				logger.Error("[MysqlStreamService.ShowTables]requestId:%s,#Panic#(%v),backTrace:%s", srv.ctx.RequestId, p, string(debug.Stack()))
				monitor.UpdatePanic("mysqlStreamService")
			}
			//timeUsed
			interval := util.GetDurationMillis(begin)
			monitor.UpdateDependence("mysqlStreamService", "showTables", interval, err)
			logger.Info("[MysqlStreamService.ShowTables]requestId:%s,timeUsed:%dus,err:%v", srv.ctx.RequestId, interval, err)
		}(time.Now())
		//sql show tables
		sql := fmt.Sprintf("SHOW tables")
		data, err := engine.SQL(sql).Query()
		if err != nil {
			logger.Error("[MysqlStreamService.ShowTables] engine.Query error. requestId:%s,err:%s", srv.ctx.RequestId, err.Error())
			return nil, err
		}
		return dao.BuildShowTables(data), nil
	}
}

func (srv *MysqlStreamService) TableDesc(table string) stream.HandleReadFunc {
	return func(engine *drivers.MysqlEngine) (ret interface{}, err error) {
		defer func(begin time.Time) {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic")
				logger.Error("[MysqlStreamService.TableDesc]requestId:%s,#Panic#(%v),backTrace:%s", srv.ctx.RequestId, p, string(debug.Stack()))
				monitor.UpdatePanic("mysqlStreamService")
			}
			//timeUsed
			interval := util.GetDurationMillis(begin)
			monitor.UpdateDependence("mysqlStreamService", "tableDesc", interval, err)
			logger.Info("[MysqlStreamService.TableDesc]requestId:%s,table:%s,timeUsed:%dus,err:%v", srv.ctx.RequestId, table, interval, err)
		}(time.Now())
		//sql desc
		sql := fmt.Sprintf("DESC %s", table)
		data, err := engine.SQL(sql).Query()
		if err != nil {
			logger.Error("[MysqlStreamService.TableFormat]engine.Query error. requestId:%s,table:%s,err:%s", srv.ctx.RequestId, table, err.Error())
			return nil, err
		}
		return dao.BuildTableDesc(data), nil
	}
}

func (srv *MysqlStreamService) ExecSql(sql []interface{}) stream.HandleWriteFunc {
	return func(engine *drivers.MysqlEngine) (err error) {
		defer func(begin time.Time) {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic")
				logger.Error("[MysqlStreamService.ExecSql]requestId:%s,sql:%v, #Panic#(%v),backTrace:%s", srv.ctx.RequestId, sql, p, string(debug.Stack()))
				monitor.UpdatePanic("mysqlStreamService")
			}
			//timeUsed
			interval := util.GetDurationMillis(begin)
			monitor.UpdateDependence("mysqlStreamService", "execSql", interval, err)
			logger.Info("[MysqlStreamService.ExecSql]requestId:%s,timeUsed:%dus,err:%v", srv.ctx.RequestId, interval, err)
		}(time.Now())
		//sql exec
		result, err := engine.Exec(sql...)
		if err != nil {
			logger.Error("[MysqlStreamService.ExecSql] engine.Exec error. requestId:%s,sql:%v,err:%s", srv.ctx.RequestId, sql, err.Error())
			return err
		}
		affects, err := result.RowsAffected()
		if err != nil {
			logger.Error("[MysqlStreamService.ExecSql] result.RowsAffected error. requestId:%s,sql:%v,err:%s", srv.ctx.RequestId, sql, err.Error())
			return nil
		}
		logger.Info("[MysqlStreamService.ExecSql] exec sql success. requestId:%s,sql:%s,affected:%d", srv.ctx.RequestId, sql, affects)
		return nil
	}
}

func (srv *MysqlStreamService) SelectLatestPrimaryId(table string, primaryCol string) stream.HandleReadFunc {
	return func(engine *drivers.MysqlEngine) (ret interface{}, err error) {
		var sql string
		defer func(begin time.Time) {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic")
				logger.Error("[MysqlStreamService.SelectLatestPrimaryId]requestId:%s,sql:%s, #Panic#(%v),backTrace:%s", srv.ctx.RequestId, sql, p, string(debug.Stack()))
				monitor.UpdatePanic("mysqlStreamService")
			}
			//timeUsed
			interval := util.GetDurationMillis(begin)
			monitor.UpdateDependence("mysqlStreamService", "selectLatestPrimaryId", interval, err)
			logger.Info("[MysqlStreamService.SelectLatestPrimaryId]requestId:%s,sql:%s,timeUsed:%dus,err:%v", srv.ctx.RequestId, sql, interval, err)
		}(time.Now())
		//sql select
		sql = fmt.Sprintf("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1", primaryCol, table, primaryCol)
		data, err := engine.SQL(sql).Query()
		if err != nil {
			logger.Error("[MysqlStreamService.SelectLatestPrimaryId] engine.Query error. requestId:%s,table:%s,sql:%s,err:%s", srv.ctx.RequestId, table, sql, err.Error())
			return nil, err
		}
		if len(data) == 0 {
			//data not exist
			return int64(-1), nil
		}
		value := data[0][primaryCol]
		integer, _ := strconv.ParseInt(string(value), 10, 64)
		return integer, nil
	}
}

//(]
func (srv *MysqlStreamService) SelectFromPrimaryIdBetween(table string, primaryCol string, begin, end int64) stream.HandleReadFunc {
	return func(engine *drivers.MysqlEngine) (rets interface{}, err error) {
		var sql string
		defer func(begin time.Time) {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic")
				logger.Error("[MysqlStreamService.SelectFromPrimaryIdBetween]requestId:%s,sql:%s, #Panic#(%v),backTrace:%s", srv.ctx.RequestId, sql, p, string(debug.Stack()))
				monitor.UpdatePanic("mysqlStreamService")
			}
			//timeUsed
			interval := util.GetDurationMillis(begin)
			monitor.UpdateDependence("mysqlStreamService", "selectFromPrimaryIdBetween", interval, err)
			logger.Info("[MysqlStreamService.SelectFromPrimaryIdBetween]requestId:%s,sql:%s,timeUsed:%dus,err:%v", srv.ctx.RequestId, sql, interval, err)
		}(time.Now())

		//SELECT * FROM product_v2 WHERE product_id>416 ORDER BY product_id ASC LIMIT 10
		sql = fmt.Sprintf("SELECT * FROM %s WHERE %s>%d AND %s<=%d ORDER BY %s ASC", table, primaryCol, begin, primaryCol, end, primaryCol)
		data, err := engine.SQL(sql).Query()
		if err != nil {
			logger.Error("[MysqlStreamService.SelectFromPrimaryIdBetween] engine.Query error. requestId:%s,table:%s,sql:%s,err:%s", srv.ctx.RequestId, table, sql, err.Error())
			return nil, err
		}
		ret := make([]map[string]interface{}, 0, len(data))
		for _, vs := range data {
			item := make(map[string]interface{})
			for col, val := range vs {
				item[col] = string(val)
			}
			ret = append(ret, item)
		}
		return ret, nil
	}
}
