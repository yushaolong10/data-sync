package condition

import (
	"infrastructure/context"
	"infrastructure/dao"
)

type MysqlRunTimeFilterCondImpl struct {
	BaseRegularCond
	ctx         *context.BizContext
	TableFormat *dao.TableFormat
}

func NewMysqlRunTimeFilterCond(ctx *context.BizContext, repo BaseRegularCond, tableDesc *dao.TableFormat) *MysqlRunTimeFilterCondImpl {
	return &MysqlRunTimeFilterCondImpl{
		ctx:             ctx,
		BaseRegularCond: repo,
		TableFormat:     tableDesc,
	}
}

//注意主键不删除
func (cond *MysqlRunTimeFilterCondImpl) GetTableFilterCols(table string) map[string]struct{} {
	confCols := cond.BaseRegularCond.GetTableFilterCols(table)
	//主键不被过滤
	filterCols := make(map[string]struct{})
	for col, _ := range confCols {
		if _, ok := cond.TableFormat.PrimaryCols[col]; !ok {
			filterCols[col] = struct{}{}
		}
	}
	return filterCols
}
