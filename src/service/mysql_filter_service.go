package service

import (
	"infrastructure/context"
	"infrastructure/repo/condition"
	"infrastructure/repo/format"
	"infrastructure/repo/serializer"
	"lib/logger"
	"lib/util"
	"service/customfilter"
)

type MysqlFilterService struct {
	ctx *context.BizContext
}

func NewMysqlFilterService(ctx *context.BizContext) *MysqlFilterService {
	return &MysqlFilterService{ctx: ctx}
}

//过滤插入数据
func (srv *MysqlFilterService) FilterInsert(cond *condition.MysqlRunTimeFilterCondImpl, insertFmt *format.InsertFormat) (serializer.MysqlSeries, bool) {
	//1.是否存在目标表  done before
	//2.是否在配置中过滤
	//2.1 配置过滤表
	if cond.IsTableNeedFilter(insertFmt.Table) {
		logger.Info("[MysqlFilterService.FilterInsert] table(%s) in config filter", insertFmt.Table)
		return nil, false
	}
	//2.2 配置过滤表字段
	for col, _ := range cond.GetTableFilterCols(insertFmt.Table) {
		if _, ok := insertFmt.Data[col]; ok {
			delete(insertFmt.Data, col)
		}
	}
	//3.目标表字段是否缺失
	for col, _ := range insertFmt.Data {
		if _, ok := cond.TableFormat.Cols[col]; !ok {
			delete(insertFmt.Data, col)
		}
	}
	//表定制行为
	custom := customfilter.GetCustomFilter(insertFmt.Database, insertFmt.Table)
	if custom != nil {
		customInsert, err := custom.FilterInsert(cond, insertFmt)
		if err != nil {
			logger.Error("[MysqlFilterService.FilterInsert] custom.FilterInsert error.requestId:%s,insertFmt:%s,err:%s", srv.ctx.RequestId, util.StructToJson(insertFmt), err.Error())
			return nil, false
		}
		insertFmt = customInsert
	}
	//生成序列化
	sqlSeries := serializer.SqlBehaviour{
		Type:     serializer.SqlBehaviourInsert,
		Database: insertFmt.Database,
		Table:    insertFmt.Table,
		Data:     insertFmt.Data,
	}
	//是否on duplicate update
	if cond.IsUpsert(insertFmt.Table) {
		sqlSeries.Type = serializer.SqlBehaviourUpsert
	}
	return &sqlSeries, true
}

//过滤更新数据
func (srv *MysqlFilterService) FilterUpdate(cond *condition.MysqlRunTimeFilterCondImpl, updateFmt *format.UpdateFormat) (serializer.MysqlSeries, bool) {
	//1.是否存在目标表 done before
	//2.是否在配置中过滤
	//2.1 配置过滤表
	if cond.IsTableNeedFilter(updateFmt.Table) {
		logger.Info("[MysqlFilterService.FilterUpdate] table(%s) in config regular", updateFmt.Table)
		return nil, false
	}
	//2.2 配置过滤表字段
	for col, _ := range cond.GetTableFilterCols(updateFmt.Table) {
		if _, ok := updateFmt.Old[col]; ok {
			delete(updateFmt.Old, col)
		}
	}
	//3.目标表字段是否缺失
	for col, _ := range updateFmt.Old {
		if _, ok := cond.TableFormat.Cols[col]; !ok {
			delete(updateFmt.Old, col)
		}
	}
	//4.表定制行为
	//todo
	//5.生成序列化
	primaryKvs := make(map[string]interface{})
	for index, col := range updateFmt.PrimaryCols {
		primaryKvs[col] = updateFmt.PrimaryVals[index]
	}
	sqlSeries := serializer.SqlBehaviour{
		Type:          serializer.SqlBehaviourUpdate,
		Database:      updateFmt.Database,
		Table:         updateFmt.Table,
		PrimaryKeyVal: primaryKvs,
		Data:          updateFmt.Old,
	}
	return &sqlSeries, true
}

//过滤删除数据
func (srv *MysqlFilterService) FilterDelete(cond *condition.MysqlRunTimeFilterCondImpl, deleteFmt *format.DeleteFormat) (serializer.MysqlSeries, bool) {
	//1.是否存在目标表 done before
	//2.是否在配置中过滤
	//2.1 配置过滤表
	if cond.IsTableNeedFilter(deleteFmt.Table) {
		logger.Info("[MysqlFilterService.FilterDelete] table(%s) in config regular", deleteFmt.Table)
		return nil, false
	}
	//4.表定制行为
	//todo
	//5.生成序列化
	primaryKvs := make(map[string]interface{})
	for index, col := range deleteFmt.PrimaryCols {
		primaryKvs[col] = deleteFmt.PrimaryVals[index]
	}
	sqlSeries := serializer.SqlBehaviour{
		Type:          serializer.SqlBehaviourDelete,
		Database:      deleteFmt.Database,
		Table:         deleteFmt.Table,
		PrimaryKeyVal: primaryKvs,
	}
	return &sqlSeries, true
}
