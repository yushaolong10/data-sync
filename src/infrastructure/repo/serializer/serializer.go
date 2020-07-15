package serializer

import (
	"fmt"
	"strings"
)

const (
	SqlBehaviourInsert = iota
	SqlBehaviourUpdate
	SqlBehaviourDelete
	SqlBehaviourUpsert
)

type MysqlSeries interface {
	Encode() (*SqlFmt, error)
}

type SqlFmt struct {
	//PrepareSql string
	//Values     []interface{}
	PrepareSql []interface{}
}

type SqlBehaviour struct {
	Type          int
	Database      string
	Table         string
	PrimaryKeyVal map[string]interface{}
	Data          map[string]interface{}
}

func (sql *SqlBehaviour) Encode() (*SqlFmt, error) {
	switch sql.Type {
	case SqlBehaviourInsert:
		return sql.buildInsert()
	case SqlBehaviourUpdate:
		return sql.buildUpdate()
	case SqlBehaviourDelete:
		return sql.buildDelete()
	case SqlBehaviourUpsert:
		return sql.buildUpsert()
	}
	return nil, nil
}

func (sql *SqlBehaviour) buildInsert() (*SqlFmt, error) {
	//prepare format
	cols := make([]string, 0, len(sql.Data))
	vals := make([]interface{}, 0, len(sql.Data))
	pres := make([]string, 0, len(sql.Data))
	for c, v := range sql.Data {
		cols = append(cols, c)
		vals = append(vals, v)
		pres = append(pres, "?")
	}
	colPart := strings.Join(cols, ",")
	valPart := strings.Join(pres, ",")
	//generate sql
	preSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", sql.Table, colPart, valPart)
	execSql := []interface{}{preSql}
	for _, v := range vals {
		execSql = append(execSql, v)
	}
	sqlFmt := SqlFmt{
		PrepareSql: execSql,
	}
	return &sqlFmt, nil
}

func (sql *SqlBehaviour) buildUpdate() (*SqlFmt, error) {
	//prepare format
	//change
	changeCols := make([]string, 0)
	changeVals := make([]interface{}, 0)
	for c, v := range sql.Data {
		changeCols = append(changeCols, fmt.Sprintf("%s=? ", c))
		changeVals = append(changeVals, v)
	}
	changePart := strings.Join(changeCols, ",")
	//condition
	condCols := make([]string, 0)
	condVals := make([]interface{}, 0)
	for col, val := range sql.PrimaryKeyVal {
		condCols = append(condCols, fmt.Sprintf("%s=? ", col))
		condVals = append(condVals, val)
	}
	condPart := strings.Join(condCols, "AND")
	//generate sql
	//update test.e set m = 5.444, c = now(3) where id = 1;
	preSql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", sql.Table, changePart, condPart)
	execSql := []interface{}{preSql}
	for _, v := range changeVals {
		execSql = append(execSql, v)
	}
	for _, v := range condVals {
		execSql = append(execSql, v)
	}
	sqlFmt := SqlFmt{
		PrepareSql: execSql,
	}
	return &sqlFmt, nil
}

func (sql *SqlBehaviour) buildDelete() (*SqlFmt, error) {
	//prepare format
	//condition
	condCols := make([]string, 0)
	condVals := make([]interface{}, 0)
	for col, val := range sql.PrimaryKeyVal {
		condCols = append(condCols, fmt.Sprintf("%s=? ", col))
		condVals = append(condVals, val)
	}
	condPart := strings.Join(condCols, "AND")
	//generate sql
	//delete from test.e where id = 1;
	preSql := fmt.Sprintf("DELETE FROM %s WHERE %s", sql.Table, condPart)
	execSql := []interface{}{preSql}
	for _, v := range condVals {
		execSql = append(execSql, v)
	}
	sqlFmt := SqlFmt{
		PrepareSql: execSql,
	}
	return &sqlFmt, nil
}

func (sql *SqlBehaviour) buildUpsert() (*SqlFmt, error) {
	cols := make([]string, 0, len(sql.Data))
	pres := make([]string, 0, len(sql.Data))
	insertVals := make([]interface{}, 0, len(sql.Data))
	for c, v := range sql.Data {
		cols = append(cols, c)
		pres = append(pres, "?")
		insertVals = append(insertVals, v)
	}
	insertPart := strings.Join(cols, ",")
	insertCond := strings.Join(pres, ",")
	//update condition
	updateCols := make([]string, 0, len(sql.Data))
	updateVals := make([]interface{}, 0, len(sql.Data))
	for c, v := range sql.Data {
		updateCols = append(updateCols, fmt.Sprintf("%s=? ", c))
		updateVals = append(updateVals, v)
	}
	updatePart := strings.Join(updateCols, ",")
	//generate sql
	//on duplicate key
	preSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s", sql.Table, insertPart, insertCond, updatePart)
	//exec
	execSql := []interface{}{preSql}
	for _, v := range insertVals {
		execSql = append(execSql, v)
	}
	for _, v := range updateVals {
		execSql = append(execSql, v)
	}
	sqlFmt := SqlFmt{
		PrepareSql: execSql,
	}
	return &sqlFmt, nil
}
