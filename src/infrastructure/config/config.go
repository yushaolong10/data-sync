package config

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"lib/logger"
	"lib/util"
)

var Conf GlobalConfig

type GlobalConfig struct {
	Env            string               `toml:"env"`
	Cluster        string               `toml:"cluster"`
	SyncIdFilePath string               `toml:"sync_id_file_path"`
	Http           HttpConfig           `toml:"http"`
	SSh            map[string]SShConfig `toml:"ssh"`
	Strategy       StrategyConfig       `toml:"strategy"`
	MysqlTask      []MysqlTaskConfig    `toml:"mysql_task"`
}

type HttpConfig struct {
	HttpPort    int64 `toml:"http_port"`
	MonitorPort int64 `toml:"monitor_port"`
}

type SShConfig struct {
	UserName    string `toml:"username"`
	BoardHost   string `toml:"board_host"`
	Password    string `toml:"password"`
	PrivatePath string `toml:"private_path"`
}

type StrategyConfig struct {
	SyncMq     SyncMqConfig     `toml:"sync_mq"`
	SyncDirect SyncDirectConfig `toml:"sync_direct"`
	Inspect    InspectConfig    `toml:"inspect"`
}

type SyncMqConfig struct {
	Mysql []MqConfig `toml:"mysql"`
}

type MqConfig struct {
	Name     string   `toml:"name"`
	Kind     string   `toml:"kind"`
	ConfPath string   `toml:"conf_path"`
	Group    string   `toml:"group"`
	Topics   []string `toml:"topics"`
	LogPath  string   `toml:"log_path"`
	Cluster  string   `toml:"cluster"`
}

type SyncDirectConfig struct {
	AppendDuration int64 `toml:"append_duration"`
}

type InspectConfig struct {
	InspectDuration   int64 `toml:"inspect_duration"`
	InspectAlarmCount int32 `toml:"inspect_alarm_count"`
}

type MysqlTaskConfig struct {
	TaskName      string                        `toml:"task_name"`
	DataBase      string                        `toml:"database"`
	DataMode      string                        `toml:"data_mode"`
	IncludeTables []string                      `toml:"include_tables"`
	ExcludeTables []string                      `toml:"exclude_tables"`
	Regular       map[string]MysqlRegularConfig `toml:"regular"`
	Source        MysqlMachineConfig            `toml:"source"`
	Target        MysqlMachineConfig            `toml:"target"`
	Strategy      TaskStrategyConfig            `toml:"strategy"`
}

type MysqlRegularConfig struct {
	TableName  string   `toml:"table_name"`
	FilterCols []string `toml:"filter_cols"`
	Upsert     bool     `toml:"upsert"`
}

type MysqlMachineConfig struct {
	DbHost     string         `toml:"db_host"`
	DbPort     int            `toml:"db_port"`
	DbUsername string         `toml:"db_username"`
	DbPassword string         `toml:"db_password"`
	EnableSSh  bool           `toml:"enable_ssh"`
	MysqlSSh   MysqlSShConfig `toml:"ssh"`
}

type MysqlSShConfig struct {
	SShUsername string `toml:"ssh_username"`
	TunnelHost  string `toml:"tunnel_host"`
	TunnelPort  int    `toml:"tunnel_port"`
}

type TaskStrategyConfig struct {
	StrategyMode int16  `toml:"strategy_mode"`
	MqName       string `toml:"sync_mq_name"`
}

func Init(configFile string) error {
	if _, err := toml.DecodeFile(configFile, &Conf); err != nil {
		return fmt.Errorf("data_sync config loader fail:%s", err.Error())
	}
	logger.Info("data_sync config loader success:\n%s", util.StructToJson(&Conf))
	return nil
}
