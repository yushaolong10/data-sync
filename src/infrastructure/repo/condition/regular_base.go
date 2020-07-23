package condition

type RegularTableMap struct {
	FilterCols map[string]struct{}
	IsUpsert   bool
}

type MysqlBaseRegularCondImpl struct {
	DataMode        string
	IncludeTables   map[string]struct{}
	ExcludeTables   map[string]struct{}
	RegularTableMap map[string]RegularTableMap
}

func (cond *MysqlBaseRegularCondImpl) IsTableNeedFilter(table string) bool {
	if cond.DataMode == DataModeAll {
		return false
	}
	if _, ok := cond.IncludeTables[table]; ok {
		return false
	}
	if _, ok := cond.ExcludeTables[table]; ok {
		return true
	}
	//若用户仅填写excludeTables, 未填写includeTables, 则说明其余表都无需过滤
	if len(cond.ExcludeTables) != 0 && len(cond.IncludeTables) == 0 {
		return false
	}
	//若用户仅填写includeTables, 未填写excludeTables,则说明其余表都过滤
	if len(cond.IncludeTables) != 0 && len(cond.ExcludeTables) == 0 {
		return true
	}
	//安全考虑: 默认表被过滤,不进行下一步操作
	return true
}

func (cond *MysqlBaseRegularCondImpl) GetTableFilterCols(table string) map[string]struct{} {
	if cols, ok := cond.RegularTableMap[table]; ok {
		return cols.FilterCols
	}
	return make(map[string]struct{})
}

func (cond *MysqlBaseRegularCondImpl) IsUpsert(table string) bool {
	if cols, ok := cond.RegularTableMap[table]; ok {
		return cols.IsUpsert
	}
	return false
}
