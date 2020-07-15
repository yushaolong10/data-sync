package service

import (
	"fmt"
	"infrastructure/context"
	"infrastructure/dao"
	"infrastructure/repo/format"
)

type MysqlFormatService struct {
	ctx *context.BizContext
}

func NewMysqlFormatService(ctx *context.BizContext) *MysqlFormatService {
	return &MysqlFormatService{ctx: ctx}
}

//maxwell insert 格式构建
func (srv *MysqlFormatService) BuildMaxwellInsert(data map[string]interface{}) *format.InsertFormat {
	reqType, _ := data["type"]
	reqDatabase, _ := data["database"]
	reqTable, _ := data["table"]
	reqPrimaryVals, _ := data["primary_key"]
	reqPrimaryCols, _ := data["primary_key_columns"]
	reqData, _ := data["data"]
	//primaryCols
	primaryCols := make([]string, 0)
	for _, v := range reqPrimaryCols.([]interface{}) {
		primaryCols = append(primaryCols, v.(string))
	}
	inserts := format.InsertFormat{
		Table:       reqTable.(string),
		Type:        reqType.(string),
		Database:    reqDatabase.(string),
		PrimaryVals: reqPrimaryVals.([]interface{}),
		PrimaryCols: primaryCols,
		Data:        reqData.(map[string]interface{}),
	}
	return &inserts
}

//maxwell update 格式构建
func (srv *MysqlFormatService) BuildMaxwellUpdate(data map[string]interface{}) *format.UpdateFormat {
	reqType, _ := data["type"]
	reqDatabase, _ := data["database"]
	reqTable, _ := data["table"]
	reqPrimaryVals, _ := data["primary_key"]
	reqPrimaryCols, _ := data["primary_key_columns"]
	reqData, _ := data["data"]
	reqOld, _ := data["old"]
	//primaryCols
	primaryCols := make([]string, 0)
	for _, v := range reqPrimaryCols.([]interface{}) {
		primaryCols = append(primaryCols, v.(string))
	}
	updates := format.UpdateFormat{
		Table:       reqTable.(string),
		Type:        reqType.(string),
		Database:    reqDatabase.(string),
		PrimaryVals: reqPrimaryVals.([]interface{}),
		PrimaryCols: primaryCols,
		Data:        reqData.(map[string]interface{}),
		Old:         reqOld.(map[string]interface{}),
	}
	return &updates
}

//maxwell delete 格式构建
func (srv *MysqlFormatService) BuildMaxwellDelete(data map[string]interface{}) *format.DeleteFormat {
	reqType, _ := data["type"]
	reqDatabase, _ := data["database"]
	reqTable, _ := data["table"]
	reqPrimaryVals, _ := data["primary_key"]
	reqPrimaryCols, _ := data["primary_key_columns"]
	//primaryCols
	primaryCols := make([]string, 0)
	for _, v := range reqPrimaryCols.([]interface{}) {
		primaryCols = append(primaryCols, v.(string))
	}
	updates := format.DeleteFormat{
		Table:       reqTable.(string),
		Type:        reqType.(string),
		Database:    reqDatabase.(string),
		PrimaryVals: reqPrimaryVals.([]interface{}),
		PrimaryCols: primaryCols,
	}
	return &updates
}

//direct sync database insert 构建
func (srv *MysqlFormatService) BuildDirectInsert(database string, table string, tableFmt *dao.TableFormat, data map[string]interface{}) (*format.InsertFormat, error) {
	vals := make([]interface{}, 0)
	cols := make([]string, 0)
	for c, _ := range tableFmt.PrimaryCols {
		cols = append(cols, c)
		v, ok := data[c]
		if !ok {
			return nil, fmt.Errorf("table(%s) primaryKey(%s) value not exist", table, c)
		}
		vals = append(vals, v)
	}
	inserts := format.InsertFormat{
		Table:       table,
		Type:        "insert",
		Database:    database,
		PrimaryVals: vals,
		PrimaryCols: cols,
		Data:        data,
	}
	return &inserts, nil
}
