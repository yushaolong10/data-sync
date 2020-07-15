package customfilter

import (
	"infrastructure/repo/condition"
	"infrastructure/repo/format"
	"infrastructure/repo/serializer"
)

var (
	filterMap = make(map[string]CustomFilter)
)

type CustomFilter interface {
	FilterInsert(cond *condition.MysqlRunTimeFilterCondImpl, format *format.InsertFormat) (*serializer.SqlBehaviour, error)
}

func GetCustomFilter(database string, table string) CustomFilter {
	key := makeKey(database, table)
	if c, ok := filterMap[key]; ok {
		return c
	}
	return nil
}

func makeKey(database, table string) string {
	return database + "_" + table
}

func registerCustomFilter(database string, table string, filter CustomFilter) {
	key := makeKey(database, table)
	filterMap[key] = filter
}
