package customfilter

import (
	"infrastructure/repo/condition"
	"infrastructure/repo/format"
)

const (
	//database
	DatabaseHoneycomb = "honeycomb"

	//table
	TableHoneycombAccount = "account"
)

func init() {
	registerCustomFilter(DatabaseHoneycomb, TableHoneycombAccount, &customFilterHoneycombAccount{})
}

type customFilterHoneycombAccount struct {
}

func (filter *customFilterHoneycombAccount) FilterInsert(cond *condition.MysqlRunTimeFilterCondImpl, fmt *format.InsertFormat) (*format.InsertFormat, error) {
	return fmt, nil
}
