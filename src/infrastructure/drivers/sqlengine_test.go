package drivers

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-xorm/xorm"
	"infrastructure/trace"
	"testing"
	"time"
)

func TestGetEngine(t *testing.T) {
	engineManager = map[string]*MysqlEngine{
		"mysql1": nil,
		"mysql2": &MysqlEngine{
			Engine: &xorm.Engine{
				TagIdentifier: "abc_xxx",
			},
		},
	}
	_, err1 := GetMysqlEngine("mysql3")
	if err1 != nil {
		t.Logf("info : expect not found mysql3 [%v]", err1.Error())
	}
	_, err2 := GetMysqlEngine("mysql1")
	if err2 == nil {
		t.Logf("info : found mysql1, nil")

	}
	m2, err3 := GetMysqlEngine("mysql2")
	if err3 != nil {
		t.Errorf("err : mysql2 should found")
	}
	if m2.TagIdentifier == "abc_xxx" {
		t.Logf("info : mysql2 TagIdentifier is right. the same")
	}
}

func TestEngineRecycle(t *testing.T) {
	engineManager = map[string]*MysqlEngine{
		"mysql1": nil,
		"mysql2": &MysqlEngine{
			Engine: &xorm.Engine{
				TagIdentifier: "abc_xxx",
			},
		},
	}
	Close()
}

func TestSqlEngineInit(t *testing.T) {
	confs := map[string]MySqlConfig{
		"honeyComb": {
			DBName: "honeycomb",
			Host:   "10.138.67.99",
			Port:   2309,
			User:   "honeycomb",
			Passwd: "5da98a424f95b710",
		},
	}
	for portName, conf := range confs {
		engine, err := createMysqlEngine(conf)
		if err != nil {
			t.Errorf("[init SqlEngine Failed] %s", err.Error())
		}
		engine.SetMaxIdleConns(200)
		engine.SetConnMaxLifetime(time.Duration(5) * time.Second)
		engineManager[portName] = engine
	}
}

type testAccount struct {
	AccountId int `xorm:"pk autoincr 'account_id'"`
	QId       int `xorm:"'qid'"`
}

func (a *testAccount) TableName() string {
	return "account"
}

func TestOrmFind(t *testing.T) {
	db, err := GetMysqlEngine("honeyComb")
	if err != nil {
		t.Errorf("not found honeyComb")
		return
	}
	//raw query
	sql := "select account_id,qid from account order by account_id desc limit 10"
	results, _ := db.Query(sql)
	t.Logf("raw query:[%v]", results)
	//orm select
	var account_list = make([]*testAccount, 0)
	err = db.Desc("account_id").Limit(10, 0).Find(&account_list)
	if err != nil {
		t.Errorf("select error [%v]", err)
	}
	t.Logf("find result list")
	for _, i := range account_list {
		t.Logf("account_id:[%v] qid:[%v]", i.AccountId, i.QId)
	}
}

func TestOrmInsert(t *testing.T) {
	db, err := GetMysqlEngine("honeyComb")
	if err != nil {
		t.Errorf("not found honeyComb")
		return
	}
	//raw insert
	sql := "insert into account (qid) value (20181119010)"
	affectRet, _ := db.Exec(sql)
	lastId, _ := affectRet.LastInsertId()
	t.Logf("raw insert:[%v]", lastId)
	//orm insert
	account := &testAccount{
		QId: 20181119011,
	}
	affectRows, err := db.Insert(account)
	if err != nil {
		t.Errorf("insert error [%v]", err)
	}
	t.Logf("insert result: affect rows[%v], lastId[%v]", affectRows, account.AccountId)
}

type DomainContext struct {
	RequestId   string
	Ctx         context.Context
	ExecSession ExecSessionI
}

func TestMysqlMuti(t *testing.T) {
	confs := map[string]MySqlConfig{
		"hyperx": {
			DBName:       "hyperx",
			Host:         "10.208.68.45",
			Port:         2275,
			User:         "hyperx",
			Passwd:       "627d646663ab8278",
			ConnTimeout:  "5s",
			ReadTimeout:  "5s",
			WriteTimeout: "5s",
			MaxConnNum:   200,
		},
	}
	for portName, conf := range confs {
		engine, err := createMysqlEngine(conf)
		if err != nil {
			t.Errorf("[init SqlEngine Failed] %s", err.Error())
		}
		engine.SetMaxIdleConns(200)
		engine.SetConnMaxLifetime(time.Duration(5) * time.Second)
		engineManager[portName] = engine
	}
	dbName := "hyperx"
	cateName := "smoketest"
	cateTableName := "category_backup"
	prodTableName := "product_v2_backup"
	nowTs := time.Now().Unix()

	ctx := trace.SetTraceToCtx(context.Background())

	fnNormalDeleteCate := func(ctx *DomainContext) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, false, func(si ExecSessionI) error {
			sql := fmt.Sprintf("delete from %s where category_name = ? and ctime = ?", cateTableName)
			_, err := si.GetSession().Exec(sql, cateName, nowTs)
			return err
		})
	}

	fnNormalDeleteTxCate := func(ctx *DomainContext) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			sql := fmt.Sprintf("delete from %s where category_name = ? and ctime = ?", cateTableName)
			_, err := si.GetSession().Exec(sql, "smoketest_tx1", nowTs)
			if err != nil {
				return err
			}
			sql = fmt.Sprintf("delete from %s where category_name = ? and ctime = ?", cateTableName)
			_, err = si.GetSession().Exec(sql, "smoketest_tx2", nowTs)
			return err
		})
	}

	fnNormalDeleteProd := func(ctx *DomainContext) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, false, func(si ExecSessionI) error {
			sql := fmt.Sprintf("delete from %s where product_name = ? and product_key = ? and product_group = ?", prodTableName)
			_, err := si.GetSession().Exec(sql, "smoketestname", "smoketestpk", "99999")
			return err
		})
	}

	// muti insert
	fnMutiDetele := func(ctx *DomainContext) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnNormalDeleteCate(ctx)
			if err != nil {
				return err
			}
			err = fnNormalDeleteProd(ctx)
			return err
		})
	}

	fnMutiTxCateDetele := func(ctx *DomainContext) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnNormalDeleteTxCate(ctx)
			if err != nil {
				return err
			}
			err = fnNormalDeleteProd(ctx)
			return err
		})
	}

	fnNormalInsertCate := func(ctx *DomainContext, innerErr error) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, false, func(si ExecSessionI) error {
			sql := fmt.Sprintf("insert into %s (category_name,ctime) values (?,?)", cateTableName)
			_, err := si.GetSession().Exec(sql, cateName, nowTs)
			if err == nil {
				err = innerErr
			}
			return err
		})
	}

	fnNormalInsertCatePanic := func(ctx *DomainContext) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, false, func(si ExecSessionI) error {
			sql := fmt.Sprintf("insert into %s (category_name,ctime) values (?,?)", cateTableName)
			_, err := si.GetSession().Exec(sql, cateName, nowTs)
			panic("panic")
			return err
		})
	}

	fnTxInsertCate1 := func(ctx *DomainContext, innerErr error) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			sql := fmt.Sprintf("insert into %s (category_name,ctime) values (?,?)", cateTableName)
			_, err := si.GetSession().Exec(sql, "smoketest_tx1", nowTs)
			if err == nil {
				err = innerErr
			}
			if err != nil {
				return err
			}
			sql = fmt.Sprintf("insert into %s (category_name,ctime) values (?,?)", cateTableName)
			_, err = si.GetSession().Exec(sql, "smoketest_tx2", nowTs)
			return err
		})
	}

	fnTxInsertCate2 := func(ctx *DomainContext, innerErr error) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			sql := fmt.Sprintf("insert into %s (category_name,ctime) values (?,?)", cateTableName)
			_, err := si.GetSession().Exec(sql, "smoketest_tx1", nowTs)
			sql = fmt.Sprintf("insert into %s (category_name,ctime) values (?,?)", cateTableName)
			_, err = si.GetSession().Exec(sql, "smoketest_tx2", nowTs)
			if err == nil {
				err = innerErr
			}
			return err
		})
	}

	fnTxInsertCatePanic := func(ctx *DomainContext, innerErr error) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			sql := fmt.Sprintf("insert into %s (category_name,ctime) values (?,?)", cateTableName)
			_, err := si.GetSession().Exec(sql, "smoketest_tx1", nowTs)
			sql = fmt.Sprintf("insert into %s (category_name,ctime) values (?,?)", cateTableName)
			_, err = si.GetSession().Exec(sql, "smoketest_tx2", nowTs)
			panic("panic")
			return err
		})
	}

	fnNormalInsertProd := func(ctx *DomainContext, innerErr error) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, false, func(si ExecSessionI) error {
			sql := fmt.Sprintf("insert into %s (category_id,product_name,product_key,product_group,ctime) values (?,?,?,?,?)", prodTableName)
			_, err := si.GetSession().Exec(sql, 99999, "smoketestname", "smoketestpk", "99999", nowTs)
			if err == nil {
				err = innerErr
			}
			return err
		})
	}

	fnNormalInsertProdPanic := func(ctx *DomainContext) error {
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, false, func(si ExecSessionI) error {
			sql := fmt.Sprintf("insert into %s (category_id,product_name,product_key,product_group,ctime) values (?,?,?,?,?)", prodTableName)
			_, err := si.GetSession().Exec(sql, 99999, "smoketestname", "smoketestpk", "99999", nowTs)
			panic("panic")
			return err
		})
	}

	fnMutiInsert1 := func(ctx *DomainContext) error {
		// 正常
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnNormalInsertCate(ctx, nil)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, nil)
			return err
		})
	}

	fnMutiInsert2 := func(ctx *DomainContext) error {
		// 第一个sql错误
		innerErr := errors.New("insert err")
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnNormalInsertCate(ctx, innerErr)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, nil)
			return err
		})
	}

	fnMutiInsert3 := func(ctx *DomainContext) error {
		// 第二个sql错误
		innerErr := errors.New("insert err")
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnNormalInsertCate(ctx, nil)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, innerErr)
			return err
		})
	}

	fnMutiInsert4 := func(ctx *DomainContext) error {
		// 第一个sql panic
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnNormalInsertCatePanic(ctx)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, nil)
			return err
		})
	}

	fnMutiInsert5 := func(ctx *DomainContext) error {
		// 第二个sql panic
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnNormalInsertCate(ctx, nil)
			if err != nil {
				return err
			}
			err = fnNormalInsertProdPanic(ctx)
			return err
		})
	}

	fnMutiInsert6 := func(ctx *DomainContext) error {
		// 正常
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnTxInsertCate1(ctx, nil)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, nil)
			return err
		})
	}

	fnMutiInsert7 := func(ctx *DomainContext) error {
		// 第一个sql错误
		innerErr := errors.New("insert err")
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnTxInsertCate1(ctx, innerErr)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, nil)
			return err
		})
	}

	fnMutiInsert8 := func(ctx *DomainContext) error {
		// 第一个sql错误2
		innerErr := errors.New("insert err")
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnTxInsertCate2(ctx, innerErr)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, nil)
			return err
		})
	}

	fnMutiInsert9 := func(ctx *DomainContext) error {
		// 第二个sql错误
		innerErr := errors.New("insert err")
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnTxInsertCate2(ctx, nil)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, innerErr)
			return err
		})
	}

	fnMutiInsert10 := func(ctx *DomainContext) error {
		// 第一个sql panic
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnTxInsertCatePanic(ctx, nil)
			if err != nil {
				return err
			}
			err = fnNormalInsertProd(ctx, nil)
			return err
		})
	}

	fnMutiInsert11 := func(ctx *DomainContext) error {
		// 第二个sql panic
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnTxInsertCate1(ctx, nil)
			if err != nil {
				return err
			}
			err = fnNormalInsertProdPanic(ctx)
			return err
		})
	}

	fnMutiInsertLevel := func(ctx *DomainContext) error {
		//三级执行
		return MysqlMuti(ctx.Ctx, dbName, ctx.ExecSession, true, func(si ExecSessionI) error {
			ctx.ExecSession = si
			err := fnTxInsertCate1(ctx, nil)
			if err != nil {
				return err
			}
			err = fnMutiInsert1(ctx)
			return err
		})
	}
	/**********1********/
	// normal insert
	err := fnNormalInsertCate(&DomainContext{Ctx: ctx}, nil)
	if err != nil {
		t.Errorf("normal insert error,err=%v", err)
		return
	}
	err = MysqlGetCate(1)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	// normal delete
	err = fnNormalDeleteCate(&DomainContext{Ctx: ctx})
	if err != nil {
		t.Errorf("normal delete error,err=%v", err)
		return
	}
	err = MysqlGetCate(0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	fmt.Println("test 1 end")
	/**********2********/
	// normal insert cate tx
	err = fnTxInsertCate1(&DomainContext{Ctx: ctx}, nil)
	if err != nil {
		t.Errorf("normal tx insert cate1 error,err=%v", err)
		return
	}
	err = MysqlGetTxCate(2)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	err = fnNormalDeleteTxCate(&DomainContext{Ctx: ctx})
	if err != nil {
		t.Errorf("normal tx delete error,err=%v", err)
		return
	}
	err = MysqlGetTxCate(0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	fmt.Println("test 2 end")

	/**********3********/
	// err1 insert cate tx
	err = fnTxInsertCate1(&DomainContext{Ctx: ctx}, errors.New("insert err"))
	if err == nil {
		t.Errorf("normal tx insert cate1 not error")
		return
	}
	err = MysqlGetTxCate(0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	fmt.Println("test 3 end")

	/**********4********/
	// err2 insert cate tx
	err = fnTxInsertCate2(&DomainContext{Ctx: ctx}, errors.New("insert err"))
	if err == nil {
		t.Errorf("normal tx insert cate2 not error")
		return
	}
	err = MysqlGetTxCate(0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	fmt.Println("test 4 end")

	/**********5********/
	// panic insert cate tx
	err = fnTxInsertCatePanic(&DomainContext{Ctx: ctx}, nil)
	if err == nil {
		t.Errorf("panic tx insert cate2 not error")
		return
	}
	err = MysqlGetTxCate(0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	fmt.Println("test 5 end")

	/**********6********/
	// muti insert
	err = fnMutiInsert1(&DomainContext{Ctx: ctx})
	if err != nil {
		t.Errorf("fnMutiInsert1,err=%v", err)
		return
	}
	err = MysqlMutiGet(1, 1)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	err = fnMutiDetele(&DomainContext{Ctx: ctx})
	if err != nil {
		t.Errorf("muti delete error,err=%v", err)
		return
	}
	err = MysqlMutiGet(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 6 end")

	/**********7********/
	err = fnMutiInsert2(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert2 nor error")
		return
	}
	err = MysqlMutiGet(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 7 end")

	/**********8********/
	err = fnMutiInsert3(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert3 nor error")
		return
	}
	err = MysqlMutiGet(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 8 end")

	/**********9********/
	err = fnMutiInsert4(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert4 nor error")
		return
	}
	err = MysqlMutiGet(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 9 end")

	/**********10********/
	err = fnMutiInsert5(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert5 nor error")
		return
	}
	err = MysqlMutiGet(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 10 end")

	/**********11********/
	err = fnMutiInsert6(&DomainContext{Ctx: ctx})
	if err != nil {
		t.Errorf("fnMutiInsert6 error,err=%v", err)
		return
	}
	err = MysqlMutiGet2(2, 1)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	err = fnMutiTxCateDetele(&DomainContext{Ctx: ctx})
	if err != nil {
		t.Errorf("muti delete error,err=%v", err)
		return
	}
	err = MysqlMutiGet(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 11 end")

	/**********12********/
	err = fnMutiInsert7(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert7 nor error")
		return
	}
	err = MysqlMutiGet2(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 12 end")

	/**********13********/
	err = fnMutiInsert8(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert8 nor error")
		return
	}
	err = MysqlMutiGet2(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 13 end")

	/**********14********/
	err = fnMutiInsert9(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert9 nor error")
		return
	}
	err = MysqlMutiGet2(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 14 end")

	/**********15********/
	err = fnMutiInsert10(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert10 nor error")
		return
	}
	err = MysqlMutiGet2(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 15 end")

	/**********16********/
	err = fnMutiInsert11(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsert11 nor error")
		return
	}
	err = MysqlMutiGet2(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 16 end")

	/**********17********/
	err = fnMutiInsertLevel(&DomainContext{Ctx: ctx})
	if err == nil {
		t.Errorf("fnMutiInsertLevel nor error")
		return
	}
	err = MysqlMutiGet2(0, 0)
	if err != nil {
		t.Errorf("%v", err)
		return
	}

	fmt.Println("test 17 end")
	return
}

func MysqlGetCate(compareNum int) (err error) {
	dbName := "hyperx"
	cateName := "smoketest"
	sql := fmt.Sprintf("select * from category_backup where category_name = '%s'", cateName)
	eg, err := GetMysqlEngine(dbName)
	if err != nil {
		err = errors.New("query get eg error" + err.Error())
		return
	}
	results, err := eg.Query(sql)
	if err != nil {
		err = errors.New("query get cate error" + err.Error())
		return
	}
	if len(results) != compareNum {
		err = fmt.Errorf("query get cate compare not match,com=%d,res=%d", compareNum, len(results))
	}
	return
}

func MysqlGetTxCate(compareNum int) (err error) {
	dbName := "hyperx"
	cateName1 := "smoketest_tx1"
	cateName2 := "smoketest_tx2"
	sql := fmt.Sprintf("select * from category_backup where category_name in('%s','%s')", cateName1, cateName2)
	eg, err := GetMysqlEngine(dbName)
	if err != nil {
		err = errors.New("query get eg error" + err.Error())
		return
	}
	results, err := eg.Query(sql)
	if err != nil {
		err = errors.New("query get tx cate error" + err.Error())
		return
	}
	if len(results) != compareNum {
		err = fmt.Errorf("query get tx cate compare not match,com=%d,res=%d", compareNum, len(results))
	}
	return
}

func MysqlMutiGet(compareCateNum, compareProdNum int) (err error) {
	err = MysqlGetCate(compareCateNum)
	if err != nil {
		return
	}
	dbName := "hyperx"
	prodName := "smoketestname"
	prodKey := "smoketestpk"
	sql := fmt.Sprintf("select * from product_v2_backup where product_name = '%s' and product_key = '%s'",
		prodName, prodKey)
	eg, err := GetMysqlEngine(dbName)
	if err != nil {
		err = errors.New("query get eg error" + err.Error())
		return
	}
	results, err := eg.Query(sql)
	if err != nil {
		err = errors.New("query get prod error" + err.Error())
		return
	}
	if len(results) != compareProdNum {
		err = fmt.Errorf("query get prod compare not match,com=%d,res=%d", compareProdNum, len(results))
	}
	return
}

func MysqlMutiGet2(compareCateNum, compareProdNum int) (err error) {
	err = MysqlGetTxCate(compareCateNum)
	if err != nil {
		return
	}
	dbName := "hyperx"
	prodName := "smoketestname"
	prodKey := "smoketestpk"
	sql := fmt.Sprintf("select * from product_v2_backup where product_name = '%s' and product_key = '%s'",
		prodName, prodKey)
	eg, err := GetMysqlEngine(dbName)
	if err != nil {
		err = errors.New("query get eg error" + err.Error())
		return
	}
	results, err := eg.Query(sql)
	if err != nil {
		err = errors.New("query get prod error" + err.Error())
		return
	}
	if len(results) != compareProdNum {
		err = fmt.Errorf("query get prod compare not match,com=%d,res=%d", compareProdNum, len(results))
	}
	return
}
