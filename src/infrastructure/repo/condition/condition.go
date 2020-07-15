package condition

import (
	"infrastructure/config"
	"infrastructure/context"
)

const (
	DataModeAll  = "ALL"
	DataModePart = "PART"
)

type BaseRegularCond interface {
	IsTableNeedFilter(table string) bool
	GetTableFilterCols(table string) map[string]struct{}
}

func BuildMysqlDataBaseRegularCond(ctx *context.BaseContext, conf config.MysqlTaskConfig) BaseRegularCond {
	regular := &MysqlBaseRegularCondImpl{
		DataMode:        conf.DataMode,
		IncludeTables:   make(map[string]struct{}),
		ExcludeTables:   make(map[string]struct{}),
		RegularTableMap: make(map[string]RegularTableMap),
	}
	for _, o := range conf.IncludeTables {
		regular.IncludeTables[o] = struct{}{}
	}
	for _, o := range conf.ExcludeTables {
		regular.ExcludeTables[o] = struct{}{}
	}
	for table, tableRegular := range conf.Regular {
		filterCols := make(map[string]struct{})
		for _, v := range tableRegular.FilterCols {
			filterCols[v] = struct{}{}
		}
		regular.RegularTableMap[table] = RegularTableMap{FilterCols: filterCols}
	}
	return regular
}
