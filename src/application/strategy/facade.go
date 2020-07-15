package strategy

import (
	"application/strategy/inspect"
	"application/strategy/syncdirect"
	"application/strategy/syncmq"
	"fmt"
	"infrastructure/config"
	"infrastructure/context"
	"lib/logger"
)

const (
	ModeStrategySyncMq = 1 << iota
	ModeStrategySyncDirect
	ModeStrategyInspect
)

//策略条项
type StrategyItem interface {
	Name() string
	Run() error
}

//策略集合
type StrategySets []StrategyItem

func (sets StrategySets) RunAll() error {
	for _, item := range sets {
		if err := item.Run(); err != nil {
			logger.Error("[strategySets.RunAll] name:%s,err:%s", item.Name(), err.Error())
			return err
		}
	}
	return nil
}

//请求mysql策略
type GetMysqlSetsRequest struct {
	TaskName     string
	TaskStrategy config.TaskStrategyConfig
	PolyHook     interface{} //polymorphic 多态
}

//获取mysql策略集合
func GetMysqlSets(ctx *context.BaseContext, input GetMysqlSetsRequest) (StrategySets, error) {
	sets := make([]StrategyItem, 0)
	//inspect
	if ModeStrategyInspect&input.TaskStrategy.StrategyMode != 0 {
		strategy, err := obtainInspectSet(ctx, input)
		if err != nil {
			logger.Error("[strategy.GetMysqlSets] obtainMysqlSyncMqSet err:%s", err.Error())
			return nil, err
		}
		sets = append(sets, strategy)
	}
	//sync direct
	if ModeStrategySyncDirect&input.TaskStrategy.StrategyMode != 0 {
		strategy, err := obtainSyncDirectSet(ctx, input)
		if err != nil {
			logger.Error("[strategy.GetMysqlSets] obtainSyncDirectSet err:%s", err.Error())
			return nil, err
		}
		sets = append(sets, strategy)
	}
	//sync mq
	if ModeStrategySyncMq&input.TaskStrategy.StrategyMode != 0 {
		strategy, err := obtainMysqlSyncMqSet(ctx, input)
		if err != nil {
			logger.Error("[strategy.GetMysqlSets] obtainMysqlSyncMqSet err:%s", err.Error())
			return nil, err
		}
		sets = append(sets, strategy)
	}
	return StrategySets(sets), nil
}

//获取通用inspect策略
func obtainInspectSet(ctx *context.BaseContext, input GetMysqlSetsRequest) (StrategyItem, error) {
	//hook action
	hookAction, ok := input.PolyHook.(inspect.Hooker)
	if !ok {
		return nil, fmt.Errorf("polyHook assert (inspect.Hooker) failed")
	}
	//object
	conf := config.Conf.Strategy.Inspect
	strategy, err := inspect.NewInspectStrategy(ctx, input.TaskName, conf, hookAction)
	if err != nil {
		logger.Error("[strategy.obtainInspectSet] NewInspectStrategy err:%s", err.Error())
		return nil, err
	}
	return strategy, nil
}

//获取通用syncDirect策略
func obtainSyncDirectSet(ctx *context.BaseContext, input GetMysqlSetsRequest) (StrategyItem, error) {
	//hook action
	hookAction, ok := input.PolyHook.(syncdirect.Hooker)
	if !ok {
		return nil, fmt.Errorf("polyHook assert (syncdirect.Hooker) failed")
	}
	//object
	conf := config.Conf.Strategy.SyncDirect
	strategy, err := syncdirect.NewSyncDirectStrategy(ctx, input.TaskName, conf, hookAction)
	if err != nil {
		logger.Error("[strategy.obtainSyncDirectSet] NewSyncDirectStrategy err:%s", err.Error())
		return nil, err
	}
	return strategy, nil
}

//获取mysql syncMq策略
func obtainMysqlSyncMqSet(ctx *context.BaseContext, input GetMysqlSetsRequest) (StrategyItem, error) {
	//hook action
	hookAction, ok := input.PolyHook.(syncmq.Hooker)
	if !ok {
		return nil, fmt.Errorf("polyHook assert (syncmq.Hooker) failed")
	}
	//config
	conf, err := findMysqlSyncMqConf(input.TaskStrategy.MqName)
	if err != nil {
		logger.Error("[strategy.obtainMysqlSyncMqSet] findMysqlSyncMqConf err:%s", err.Error())
		return nil, err
	}
	//object
	strategy, err := syncmq.NewSyncMqStrategy(ctx, input.TaskName, conf, hookAction)
	if err != nil {
		logger.Error("[strategy.obtainMysqlSyncMqSet] NewSyncMqStrategy err:%s", err.Error())
		return nil, err
	}
	return strategy, nil
}

//根据mysql的database 查询mq配置
func findMysqlSyncMqConf(name string) (config.MqConfig, error) {
	var emptyConf config.MqConfig
	if len(config.Conf.Strategy.SyncMq.Mysql) == 0 {
		return emptyConf, fmt.Errorf("strategy.sync_mq.mysql config empty")
	}
	//get config
	for _, item := range config.Conf.Strategy.SyncMq.Mysql {
		if item.Name == name {
			return item, nil
		}
	}
	return emptyConf, fmt.Errorf("strategy.sync_mq.mysql config not found name(%s)", name)
}
