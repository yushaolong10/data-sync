package drivers

import (
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"time"
)

var (
	ErrNotFoundEngine = errors.New("not found sql engine")
)

var (
	engineManager = make(map[string]*MysqlEngine)
)

type MysqlEngine struct {
	*xorm.Engine
}

func (mysql *MysqlEngine) Close() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[sqlengine] close panic: [%v]\n", r)
		}
	}()
	mysql.Engine.Close()
}

type MySqlConfig struct {
	DBName          string `toml:"db_name"`
	Host            string `toml:"host"`
	Port            int    `toml:"port"`
	User            string `toml:"user"`
	Passwd          string `toml:"passwd"`
	ConnTimeout     string `toml:"conn_timeout"`
	ReadTimeout     string `toml:"read_timeout"`
	WriteTimeout    string `toml:"write_timeout"`
	MaxConnNum      int    `toml:"max_conn_num"`
	MaxIdleConnNum  int    `toml:"max_idle_conn_num"`
	MaxConnLifeTime int    `toml:"max_conn_life_time"`
}

func InitMysql(confs map[string]MySqlConfig) error {
	for dbName, conf := range confs {
		engine, err := createMysqlEngine(conf)
		if err != nil {
			return fmt.Errorf("Load SqlEngine failed: dbname(%s),err(%v)", dbName, err)
		}
		engine.SetMaxIdleConns(conf.MaxIdleConnNum)
		engine.SetMaxOpenConns(conf.MaxConnNum)
		engine.SetConnMaxLifetime(time.Duration(conf.MaxConnLifeTime) * time.Second)
		engineManager[dbName] = engine
	}
	return nil
}

func createMysqlEngine(conf MySqlConfig) (*MysqlEngine, error) {
	//here can use xorm.EngineGroup  for slave db.
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s?charset=utf8&timeout=%s&readTimeout=%s&writeTimeout=%s", conf.User, conf.Passwd, "tcp", conf.Host,
		conf.Port, conf.DBName, conf.ConnTimeout, conf.ReadTimeout, conf.WriteTimeout)
	engine, err := xorm.NewEngine("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &MysqlEngine{Engine: engine}, nil
}

func GetMysqlEngine(db string) (*MysqlEngine, error) {
	if e, ok := engineManager[db]; ok && e != nil {
		return e, nil
	}
	return nil, ErrNotFoundEngine
}

func Close() {
	for dbName, engine := range engineManager {
		fmt.Printf("[Close] mysql dbname:[%s]\n", dbName)
		if engine != nil {
			engine.Close()
		}
	}
}

const (
	mysql_conn_timeout       = "10s"
	mysql_read_timeout       = "10s"
	mysql_write_timeout      = "10s"
	mysql_max_conn_num       = 64
	mysql_max_idle_conn_num  = 10
	mysql_max_conn_life_time = 10
)

func NewCustomMysqlEngine(host string, port int, user, password string, database string) (*MysqlEngine, error) {
	conf := MySqlConfig{
		DBName:          database,
		Host:            host,
		Port:            port,
		User:            user,
		Passwd:          password,
		ConnTimeout:     mysql_conn_timeout,
		ReadTimeout:     mysql_read_timeout,
		WriteTimeout:    mysql_write_timeout,
		MaxConnNum:      mysql_max_conn_num,
		MaxIdleConnNum:  mysql_max_idle_conn_num,
		MaxConnLifeTime: mysql_max_conn_life_time,
	}
	engine, err := createMysqlEngine(conf)
	if err != nil {
		return nil, fmt.Errorf("createMysqlEngine failed. dbname(%s),err:%s", database, err.Error())
	}
	engine.SetMaxIdleConns(conf.MaxIdleConnNum)
	engine.SetMaxOpenConns(conf.MaxConnNum)
	engine.SetConnMaxLifetime(time.Duration(conf.MaxConnLifeTime) * time.Second)
	return engine, nil
}
