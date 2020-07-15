package application

import (
	"application/strategy"
	"application/strategy/syncmq"
	"fmt"
	"infrastructure/config"
	"infrastructure/context"
	"infrastructure/dao"
	"infrastructure/monitor"
	"infrastructure/repo/condition"
	"infrastructure/repo/format"
	"infrastructure/repo/serializer"
	"infrastructure/repo/stream"
	"lib/json"
	"lib/logger"
	"lib/lru"
	"lib/util"
	"service"
)

type MysqlHandler struct {
	ctx            *context.BaseContext
	name           string
	database       string
	strategyMode   int16
	conf           config.MysqlTaskConfig
	strategySets   strategy.StrategySets
	regularCond    condition.BaseRegularCond
	sourceConnRepo stream.StreamRepo
	targetConnRepo stream.StreamRepo
	//inspect state
	//key:table
	//val:lastInspectPrimaryId
	inspectStates map[string]int64
	//database table info
	tmpCache *lru.LRUCache
}

func NewMysqlHandler(ctx *context.BaseContext) *MysqlHandler {
	return &MysqlHandler{
		ctx:           ctx,
		inspectStates: make(map[string]int64),
		tmpCache:      lru.NewLRUCache(1000, 10), //10s
	}
}

//加载配置
func (handler *MysqlHandler) LoadConfig(conf config.MysqlTaskConfig) error {
	handler.name = conf.TaskName
	handler.database = conf.DataBase
	handler.strategyMode = conf.Strategy.StrategyMode
	//创建过滤条件
	handler.regularCond = condition.BuildMysqlDataBaseRegularCond(handler.ctx, conf)
	//创建连接条件
	//源连接
	srcConnRepo, err := stream.BuildMysqlStreamRepo(handler.ctx, handler.database, conf.Source)
	if err != nil {
		logger.Error("[MysqlHandler.LoadConfig] build srcConnRepo err:%s.", err.Error())
		return err
	}
	handler.sourceConnRepo = srcConnRepo
	//目标连接
	targetConnRepo, err := stream.BuildMysqlStreamRepo(handler.ctx, handler.database, conf.Target)
	if err != nil {
		logger.Error("[MysqlHandler.LoadConfig] build targetConnRepo err:%s.", err.Error())
		return err
	}
	handler.targetConnRepo = targetConnRepo
	//获取策略集合
	input := strategy.GetMysqlSetsRequest{
		TaskName:     conf.TaskName,
		TaskStrategy: conf.Strategy,
		PolyHook:     handler, //poly hook
	}
	sets, err := strategy.GetMysqlSets(handler.ctx, input)
	if err != nil {
		logger.Error("[MysqlHandler.LoadConfig] strategy GetMysqlSets err:%s.", err.Error())
		return err
	}
	handler.strategySets = sets
	return nil
}

//运行
func (handler *MysqlHandler) Execute() error {
	//运行策略集合
	err := handler.strategySets.RunAll()
	if err != nil {
		logger.Error("[MysqlHandler.Execute] strategy RunAll err:%s.", err.Error())
		return err
	}
	monitor.UpdateIndexState("mysql", handler.name, int64(handler.strategyMode))
	return nil
}

//syncMq implement
func (handler *MysqlHandler) HandleMqMessage(msg *syncmq.TopicMessage) error {
	//biz context with requestId
	ctx := &context.BizContext{
		RequestId:   util.GetTraceId(),
		BaseContext: handler.ctx,
	}
	//解析
	decode := make(map[string]interface{})
	if err := json.Unmarshal([]byte(msg.Message), &decode); err != nil {
		logger.Error("[MysqlHandler.HandleMqMessage] decode json unmarshal error. requestId:%s,message:%s,err:%s", ctx.RequestId, msg.Message, err.Error())
		return err
	}
	reqType, ok := decode["type"]
	if !ok {
		return fmt.Errorf("maxwell type field not exist")
	}
	switch reqType.(string) {
	case "insert":
		connSrv := service.NewMysqlStreamService(ctx)
		//格式化消息
		insertFmt := service.NewMysqlFormatService(ctx).BuildMaxwellInsert(decode)
		err := handler.handleInsertSql(ctx, connSrv, insertFmt)
		if err != nil {
			logger.Error("[MysqlHandler.HandleMqMessage] handleInsertSql error. requestId:%s,message:%s,err:%s", ctx.RequestId, msg.Message, err.Error())
			return err
		}
	case "update":
		connSrv := service.NewMysqlStreamService(ctx)
		updateFmt := service.NewMysqlFormatService(ctx).BuildMaxwellUpdate(decode)
		err := handler.handleUpdateSql(ctx, connSrv, updateFmt)
		if err != nil {
			logger.Error("[MysqlHandler.HandleMqMessage] handleUpdateSql error. requestId:%s,message:%s,err:%s", ctx.RequestId, msg.Message, err.Error())
			return err
		}
	case "delete":
		connSrv := service.NewMysqlStreamService(ctx)
		deleteFmt := service.NewMysqlFormatService(ctx).BuildMaxwellDelete(decode)
		err := handler.handleDeleteSql(ctx, connSrv, deleteFmt)
		if err != nil {
			logger.Error("[MysqlHandler.HandleMqMessage] handleDeleteSql error. requestId:%s,message:%s,err:%s", ctx.RequestId, msg.Message, err.Error())
			return err
		}
	}
	return nil
}

//处理插入sql
func (handler *MysqlHandler) handleInsertSql(ctx *context.BizContext, connSrv *service.MysqlStreamService, insertFmt *format.InsertFormat) error {
	//是否存在目标表
	err := handler.checkTableInTargetDatabase(ctx, connSrv, insertFmt.Table)
	if err != nil {
		logger.Error("[MysqlHandler.handleInsertSql] checkTableInTargetDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	tableFmt, err := handler.getTableDescFromTargetDatabase(ctx, connSrv, insertFmt.Table)
	if err != nil {
		logger.Error("[MysqlHandler.handleInsertSql] getTableDescFromTargetDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	//过滤处理
	condRepo := condition.NewMysqlRunTimeFilterCond(ctx, handler.regularCond, tableFmt)
	filterSrv := service.NewMysqlFilterService(ctx)
	series, need := filterSrv.FilterInsert(condRepo, insertFmt)
	if !need {
		return nil
	}
	//序列化并写入目标库
	err = handler.writeSeriesToDatabase(ctx, connSrv, series)
	if err != nil {
		logger.Error("[MysqlHandler.handleInsertSql] writeSeriesToDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	return nil
}

//处理更新sql
func (handler *MysqlHandler) handleUpdateSql(ctx *context.BizContext, connSrv *service.MysqlStreamService, updateFmt *format.UpdateFormat) error {
	//是否存在目标表
	err := handler.checkTableInTargetDatabase(ctx, connSrv, updateFmt.Table)
	if err != nil {
		logger.Error("[MysqlHandler.handleUpdateSql] checkTableInTargetDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	//获取目标表结构
	tableFmt, err := handler.getTableDescFromTargetDatabase(ctx, connSrv, updateFmt.Table)
	if err != nil {
		logger.Error("[MysqlHandler.handleUpdateSql] getTableDescFromTargetDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	//过滤处理
	condRepo := condition.NewMysqlRunTimeFilterCond(ctx, handler.regularCond, tableFmt)
	filterSrv := service.NewMysqlFilterService(ctx)
	series, need := filterSrv.FilterUpdate(condRepo, updateFmt)
	if !need {
		return nil
	}
	//序列化并写入目标库
	err = handler.writeSeriesToDatabase(ctx, connSrv, series)
	if err != nil {
		logger.Error("[MysqlHandler.handleUpdateSql] writeSeriesToDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	return nil
}

//处理删除sql
func (handler *MysqlHandler) handleDeleteSql(ctx *context.BizContext, connSrv *service.MysqlStreamService, deleteFmt *format.DeleteFormat) error {
	//是否存在目标表
	err := handler.checkTableInTargetDatabase(ctx, connSrv, deleteFmt.Table)
	if err != nil {
		logger.Error("[MysqlHandler.handleDeleteSql] checkTableInTargetDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	//获取目标表结构
	tableFmt, err := handler.getTableDescFromTargetDatabase(ctx, connSrv, deleteFmt.Table)
	if err != nil {
		logger.Error("[MysqlHandler.handleDeleteSql] getTableDescFromTargetDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	//过滤处理
	condRepo := condition.NewMysqlRunTimeFilterCond(ctx, handler.regularCond, tableFmt)
	filterSrv := service.NewMysqlFilterService(ctx)
	series, need := filterSrv.FilterDelete(condRepo, deleteFmt)
	if !need {
		return nil
	}
	//序列化并写入目标库
	err = handler.writeSeriesToDatabase(ctx, connSrv, series)
	if err != nil {
		logger.Error("[MysqlHandler.handleDeleteSql] writeSeriesToDatabase error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	return nil
}

//是否存在目标表
func (handler *MysqlHandler) checkTableInTargetDatabase(ctx *context.BizContext, connSrv *service.MysqlStreamService, table string) error {
	//获取目标库表信息
	getTablesFn := func() ([]string, error) {
		cacheVal, _ := handler.tmpCache.Get("ShowTables")
		if cacheVal != nil {
			return cacheVal.(dao.Tables), nil
		}
		tables, err := handler.targetConnRepo.Read(connSrv.ShowTables())
		if err != nil {
			logger.Error("[MysqlHandler.handleInsertSql] targetRepo read ShowTables error. requestId:%s, err:%s", ctx.RequestId, err.Error())
			return nil, err
		}
		handler.tmpCache.Update("ShowTables", tables)
		return tables.(dao.Tables), nil
	}
	tables, err := getTablesFn()
	if err != nil {
		return err
	}
	//是否存在目标表
	if !util.StringArrayHas(tables, table) {
		logger.Error("[MysqlHandler.handleInsertSql] table not in target database. requestId:%s, database:%s,table:%s", ctx.RequestId, handler.database, table)
		return fmt.Errorf("table(%s) not exist in target database(%s)", table, handler.database)
	}
	return nil
}

//获取目标库表结构
func (handler *MysqlHandler) getTableDescFromTargetDatabase(ctx *context.BizContext, connSrv *service.MysqlStreamService, table string) (*dao.TableFormat, error) {
	cacheVal, _ := handler.tmpCache.Get("TableDesc")
	if cacheVal != nil {
		return cacheVal.(*dao.TableFormat), nil
	}
	tableDesc, err := handler.targetConnRepo.Read(connSrv.TableDesc(table))
	if err != nil {
		logger.Error("[MysqlHandler.getTableDescFromTargetDatabase] targetRepo read TableDesc error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return nil, err
	}
	handler.tmpCache.Update("TableDesc", tableDesc)
	return tableDesc.(*dao.TableFormat), nil
}

//序列化并写入目标库
func (handler *MysqlHandler) writeSeriesToDatabase(ctx *context.BizContext, connSrv *service.MysqlStreamService, series serializer.MysqlSeries) error {
	//进行序列化处理
	sqlFmt, err := series.Encode()
	if err != nil {
		logger.Error("[MysqlHandler.writeSeriesToDatabase] series.Encode error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	//写入
	err = handler.targetConnRepo.Write(connSrv.ExecSql(sqlFmt.PrepareSql))
	if err != nil {
		logger.Error("[MysqlHandler.writeSeriesToDatabase] targetRepo write error. requestId:%s, prepareSql:%v,err:%s", ctx.RequestId, sqlFmt.PrepareSql, err.Error())
		return err
	}
	return nil
}

//inspect implement
func (handler *MysqlHandler) HandleInspect() bool {
	//biz context with requestId
	ctx := &context.BizContext{
		RequestId:   util.GetTraceId(),
		BaseContext: handler.ctx,
	}
	//获取目标库表信息
	connSrv := service.NewMysqlStreamService(ctx)
	//获取相同表
	sameTables, err := handler.getIntersectTables(ctx, connSrv)
	if err != nil {
		logger.Error("[MysqlHandler.HandleInspect] getIntersectTables error. requestId:%s,err:%s", ctx.RequestId, err.Error())
		return false
	}
	//监控表
	for _, table := range sameTables {
		//是否配置过滤表
		if handler.regularCond.IsTableNeedFilter(table) {
			continue
		}
		//获取目标表主键
		primaryCol, err := handler.getTablePrimaryKeyCol(ctx, connSrv, table)
		if err != nil {
			logger.Error("[MysqlHandler.getPrimaryKeyLatestId] getTablePrimaryKeyCol error. requestId:%s,table:%s,err:%s", ctx.RequestId, table, err.Error())
			return false
		}
		//获取目标表主键
		latestId, err := handler.getPrimaryKeyLatestId(ctx, connSrv, table, primaryCol)
		if err != nil {
			logger.Error("[MysqlHandler.HandleInspect] getPrimaryKeyLatestId error. requestId:%s,err:%s", ctx.RequestId, err.Error())
			return false
		}
		targetId := latestId.TargetPrimaryId
		sourceId := latestId.SourcePrimaryId
		//源库小于目标库的主键
		//则数据存在问题
		if sourceId < targetId {
			logger.Error("[MysqlHandler.HandleInspect] source primaryKeyId is less than target. requestId:%s,database(%s),table(%s),sourcePrimaryId:%d,targetPrimaryId:%d", ctx.RequestId, handler.database, table, sourceId, targetId)
			return false
		}
		//源库等于目标库的主键
		//同步正常
		if sourceId == targetId {
			handler.inspectStates[table] = targetId
			continue
		}
		//源库大于目标库的主键
		//且目标库主键与上次检测时一致
		//此段时间没有新的变更
		if handler.inspectStates[table] == targetId {
			//本次targetId 与上次一致
			return false
		}
		//目标库主键与上次检测不一致
		//说明同步正常,此段时间有新的变更
		handler.inspectStates[table] = targetId
	}
	return true
}

//获取源库与目标库的交集表
//无需cache,实时查询
func (handler *MysqlHandler) getIntersectTables(ctx *context.BizContext, connSrv *service.MysqlStreamService) ([]string, error) {
	targetTables, err := handler.targetConnRepo.Read(connSrv.ShowTables())
	if err != nil {
		logger.Error("[MysqlHandler.getIntersectTables] targetRepo ShowTables error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return nil, err
	}
	sourceTables, err := handler.sourceConnRepo.Read(connSrv.ShowTables())
	if err != nil {
		logger.Error("[MysqlHandler.getIntersectTables] sourceRepo ShowTables error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return nil, err
	}
	//获取相同表
	sameTables := util.StringArrayIntersect(sourceTables.(dao.Tables), targetTables.(dao.Tables))
	return sameTables, nil
}

type primaryKeyLatestId struct {
	SourcePrimaryId int64
	TargetPrimaryId int64
}

//获取最新主键id
func (handler *MysqlHandler) getPrimaryKeyLatestId(ctx *context.BizContext, connSrv *service.MysqlStreamService, table string, primaryCol string) (*primaryKeyLatestId, error) {
	//获取目标表主键
	primaryCol, err := handler.getTablePrimaryKeyCol(ctx, connSrv, table)
	if err != nil {
		logger.Error("[MysqlHandler.getPrimaryKeyLatestId] getTablePrimaryKeyCol error. requestId:%s,table:%s,err:%s", ctx.RequestId, table, err.Error())
		return nil, err
	}
	//查询目标表与原表主键最新Id
	targetPriId, err := handler.targetConnRepo.Read(connSrv.SelectLatestPrimaryId(table, primaryCol))
	if err != nil {
		logger.Error("[MysqlHandler.getPrimaryKeyLatestId] targetRepo SelectLatestPrimaryId error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return nil, err
	}
	sourcePriId, err := handler.sourceConnRepo.Read(connSrv.SelectLatestPrimaryId(table, primaryCol))
	if err != nil {
		logger.Error("[MysqlHandler.getPrimaryKeyLatestId] sourceRepo SelectLatestPrimaryId error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return nil, err
	}
	latestId := primaryKeyLatestId{
		SourcePrimaryId: sourcePriId.(int64),
		TargetPrimaryId: targetPriId.(int64),
	}
	return &latestId, nil
}

//获取表主键col名称
func (handler *MysqlHandler) getTablePrimaryKeyCol(ctx *context.BizContext, connSrv *service.MysqlStreamService, table string) (string, error) {
	//获取目标表结构
	tableFmt, err := handler.getTableDescFromTargetDatabase(ctx, connSrv, table)
	if err != nil {
		logger.Error("[MysqlHandler.HandleInspect] getTableDescFromTargetDatabase error. requestId:%s,table:%s,err:%s", ctx.RequestId, table, err.Error())
		return "", err
	}
	primaryCols := make([]string, 0)
	for col, _ := range tableFmt.PrimaryCols {
		item := tableFmt.Cols[col]
		if !item.IsIntType() {
			logger.Error("[MysqlHandler.HandleInspect] unSupport primaryKey dataType (not integer). requestId:%s,table:%s,primaryInfo:%v", ctx.RequestId, table, item)
			return "", fmt.Errorf("not support table(%s) primaryKey(%s) type", table, item.Field)
		}
		primaryCols = append(primaryCols, col)
	}
	if len(primaryCols) == 0 {
		return "", fmt.Errorf("table(%s) not exist integer primaryKey", table)
	}
	if len(primaryCols) > 1 {
		logger.Error("[MysqlHandler.HandleInspect] table has more than one primaryKey. requestId:%s,table:%s,primaryKeys:%v", ctx.RequestId, table, primaryCols)
		return "", fmt.Errorf("table(%s) primaryKey integer key more than one", table)
	}
	return primaryCols[0], nil
}

//syncDirect implement
func (handler *MysqlHandler) HandleDirect() error {
	//biz context with requestId
	ctx := &context.BizContext{
		RequestId:   util.GetTraceId(),
		BaseContext: handler.ctx,
	}
	//获取目标库表信息
	connSrv := service.NewMysqlStreamService(ctx)
	//获取交集表
	sameTables, err := handler.getIntersectTables(ctx, connSrv)
	if err != nil {
		logger.Error("[MysqlHandler.HandleDirect] getIntersectTables error. requestId:%s,err:%s", ctx.RequestId, err.Error())
		return err
	}
	//同步表
	for _, table := range sameTables {
		//上下文是否终止
		if ctx.IsCanceled() {
			logger.Info("[MysqlHandler.HandleDirect] context canceled")
			return nil
		}
		//是否配置过滤表
		if handler.regularCond.IsTableNeedFilter(table) {
			continue
		}
		//获取目标表主键
		primaryCol, err := handler.getTablePrimaryKeyCol(ctx, connSrv, table)
		if err != nil {
			logger.Error("[MysqlHandler.getPrimaryKeyLatestId] getTablePrimaryKeyCol error. requestId:%s,table:%s,err:%s", ctx.RequestId, table, err.Error())
			return err
		}
		//获取目标表主键
		latestId, err := handler.getPrimaryKeyLatestId(ctx, connSrv, table, primaryCol)
		if err != nil {
			logger.Error("[MysqlHandler.HandleDirect] getPrimaryKeyLatestId error. requestId:%s,err:%s", ctx.RequestId, err.Error())
			return err
		}
		if latestId.SourcePrimaryId <= latestId.TargetPrimaryId {
			logger.Info("[MysqlHandler.HandleDirect] not need sync, source primaryKeyId is less than or equal target. requestId:%s,database(%s),table(%s),sourcePrimaryId:%d,targetPrimaryId:%d", ctx.RequestId, handler.database, table, latestId.SourcePrimaryId, latestId.TargetPrimaryId)
			continue
		}
		//同步
		err = handler.syncTableDirect(ctx, connSrv, table, primaryCol, latestId)
		if err != nil {
			logger.Error("[MysqlHandler.HandleDirect] sync table data from source to target database error. requestId:%s,table:%s,err:%s", ctx.RequestId, table, err.Error())
		}
	}
	return nil
}

func (handler *MysqlHandler) syncTableDirect(ctx *context.BizContext, connSrv *service.MysqlStreamService, table string, primaryCol string, latestId *primaryKeyLatestId) error {
	//获取表结构
	tableFmt, err := handler.getTableDescFromTargetDatabase(ctx, connSrv, table)
	if err != nil {
		logger.Error("[MysqlHandler.syncTableDirect] targetRepo read TableFormat error. requestId:%s, err:%s", ctx.RequestId, err.Error())
		return err
	}
	//数据起终位置
	fromId := latestId.TargetPrimaryId
	toId := latestId.SourcePrimaryId
	//每次同步条数
	limit := int64(100)
	fmtSrv := service.NewMysqlFormatService(ctx)
	//循环同步
	for begin := fromId; begin < toId; begin = begin + limit {
		dataList, err := handler.sourceConnRepo.Read(connSrv.SelectFromPrimaryIdBetween(table, primaryCol, begin, begin+limit))
		if err != nil {
			logger.Error("[MysqlHandler.syncTableDirect] SelectFromPrimaryIdBetween error. requestId:%s, err:%s", ctx.RequestId, err.Error())
			return err
		}
		for _, data := range dataList.([]map[string]interface{}) {
			//上下文是否终止
			if ctx.IsCanceled() {
				logger.Info("[MysqlHandler.syncTableDirect] context canceled. curData:%v", data)
				return nil
			}
			//获取insertFmt
			insertFmt, err := fmtSrv.BuildDirectInsert(handler.database, table, tableFmt, data)
			if err != nil {
				logger.Error("[MysqlHandler.syncTableDirect] BuildDirectInsert error. requestId:%s, err:%s", ctx.RequestId, err.Error())
				return err
			}
			err = handler.handleInsertSql(ctx, connSrv, insertFmt)
			if err != nil {
				logger.Error("[MysqlHandler.syncTableDirect] handleInsertSql error. requestId:%s, err:%s", ctx.RequestId, err.Error())
				return err
			}
		}
	}
	return nil
}
