package dao

import "strings"

type TableFormat struct {
	PrimaryCols map[string]struct{}
	Cols        map[string]*TableItem
}

type TableItem struct {
	Field string
	Type  string
	Key   string
}

func (item *TableItem) IsIntType() bool {
	return strings.Contains(item.Type, "int")
}

func (item *TableItem) IsCharType() bool {
	return strings.Contains(item.Type, "char")
}

func BuildTableDesc(data []map[string][]byte) *TableFormat {
	desc := TableFormat{
		PrimaryCols: map[string]struct{}{},
		Cols:        make(map[string]*TableItem),
	}
	for _, v := range data {
		col := TableItem{
			Field: string(v["Field"]),
			Type:  string(v["Type"]),
			Key:   string(v["Key"]),
		}
		desc.Cols[col.Field] = &col
		if col.Key == "PRI" {
			desc.PrimaryCols[col.Field] = struct{}{}
		}
	}
	return &desc
}
