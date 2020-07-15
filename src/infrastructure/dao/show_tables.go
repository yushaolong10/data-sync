package dao

type Tables []string

func BuildShowTables(data []map[string][]byte) Tables {
	tables := make([]string, 0)
	for _, vs := range data {
		for _, v := range vs {
			tables = append(tables, string(v))
		}
	}
	return Tables(tables)
}
