

[TOC]

##  数据同步(data-sync)

### 1.场景描述

> 在A、B两地区机房，分别部署了mysql服务。由于业务需求，需要将A地区的数据准实时同步到B地区。

如上述场景，根据目前基础组件支持及业务需求现状，提出了以下方案:

(1) 将源库的 binlog写入mq, 数据同步服务消费mq, 并在目标库执行 **(核心方案)**

- [maxwell](http://maxwells-daemon.io/)将binlog写入消息队列qbus/kafka
- 服务消费mq，进行数据解析，格式过滤，生成sql
- 服务将数据写入到目标库(支持增、删、改)

(2) 周期性的从源库同步数据到目标库 **(降级方案)**

- 定时轮询，根据表主键，查询出增量的源库数据

- 将增量数据进行数据解析，格式过滤，生成sql
- 服务将数据写入到目标库 (仅支持增)

(3) 周期性的检测两库数据状态

- 服务内部启动时钟周期，进行周期检测
- 目标库数据是否与源库一致, 是否正常同步

基于上述方案，开发了数据同步服务`data-sync`

### 2.系统架构

<center>
    <img src="https://github.com/alwaysthanks/data-sync/blob/master/docs/architecture.png">
</center>

### 3.控制流

<center>
    <img src="https://github.com/alwaysthanks/data-sync/blob/master/docs/control-flow.png">
</center>

### 4.项目配置

```toml
#demo
# conf/iot.sync.toml
#主要部分示例如下:
[ssh]
    [ssh.yushaolong]
        username = 'yushaolong'
        board_host = '110.111.112.113:11022'
        password = "yourpassword"   # 与private_path 二选一 (优先)
        private_path = './conf/empty.private.key' # 与 password 二选一

[strategy] #运行策略
    [strategy.sync_mq] #通过binlog写入mq, 消费mq同步
        [[strategy.sync_mq.mysql]]
            name       = "cn_to_aws_database"    #注意只能使用字母，数字，下划线
            kind       = "kafka"
            conf_path  = "./conf/kafka.conf"
            group      = "iot_data_sync"
            topics     = ["iot_mysql_binlog_test"]
            log_path   = "./log/consumer.log"
    [strategy.sync_direct] #定时任务同步
        #仅支持追加
        append_duration = 60 # 单位s, 同步周期

    [strategy.inspect] #巡检模式,不做同步
        inspect_duration = 30 #单位:s, 巡检周期, 当与定时任务同时进行时,应大于 append_duration
        inspect_alarm_count = 5 #巡检5次,数据仍然不一致且[primaryId未曾变化],报警

[[mysql_task]]
    task_name = "mysql_database_name_prod_to_test" #注意命名需要全局唯一, 只能使用字母,数字,下划线,
    database = "database_name"
    data_mode = "PART" #ALL全同步; PART 指定表
    include_tables = ["user"] #同步表,data_mode=PART时, 则使用该字段(优先)
    exclude_tables = [] #不同步表,data_mode=PART时, 则使用该字段

    [mysql_task.regular] #过滤规则定义,在data_mode=PART/ALL均生效
        [mysql_task.regular.table_name]  #表
            table_name = "table_name"     #表名称
            filter_cols = ["col1","col2"] #过滤字段, 生成的sql会过滤相应字段,不进行同步
            upsert = true #当false时, 使用insert格式; 当true时,会insert on duplicate update
        [mysql_task.source]
            db_host = "110.111.112.113"
            db_port = 3306
            db_username = "database_name"
            db_password = "database_password"
            enable_ssh = true #开启ssh隧道模式,直接使用tunnel_host+tunnel_port
            [mysql_task.source.ssh]
                ssh_username = "yushaolong" #使用隧道用户
                tunnel_host = "127.0.0.1" #打通本地ip
                tunnel_port = 3306
        [mysql_task.target]
            db_host = "114.115.116.117"
            db_port = 3307
            db_username = "database_name"
            db_password = "database_password"
            enable_ssh = false   #关闭ssh隧道模式
            [mysql_task.target.ssh]

    [mysql_task.strategy] # 数据库策略
            strategy_mode = 4 # 策略模式: 1仅mq; 2仅定时; 4仅巡检; 5 使用mq并巡检; 6 使用定时并巡检; 7 同时mq,定时,巡检
            sync_mq_name = "cn_to_aws_database" # 当模式包含mq时, 该字段有效
```

### 5.代码目录

```
├── conf                     #源配置
├── src                      #源码目录
│   ├── application
│   │   └── strategy         #策略
│   │       ├── inspect      #策略:巡检模式
│   │       ├── syncdirect   #策略:从源库直接同步
│   │       └── syncmq       #策略:从MQ同步
│   ├── infrastructure
│   │   ├── config           #解析配置
│   │   ├── context          #全局上下文
│   │   ├── dao              #数据库映射对象
│   │   ├── drivers          #数据库驱动组件
│   │   ├── monitor          #监控组件
│   │   ├── mq               #消息队列组件
│   │   ├── repo
│   │   │   ├── condition    #库: 条件管理
│   │   │   ├── format       #库: 数据格式
│   │   │   ├── serializer   #库: 序列化
│   │   │   └── stream       #库: 流
│   │   └── ssh              #ssh隧道组件
│   ├── main
│   ├── server
│   │   └── http             #对外http服务
│   └── service              #业务处理
│       └── customfilter     #业务:定制化过滤
└── tools                    #shell工具
```

### 6.监控指标

项目使用了counter,gauge,histogram三项指标来检测服务运行状态。

(1) counter 指标监控项:

- 服务内部panic
- 数据同步状态异常

(2) gauge 指标监控项:

- 当前服务运行的策略模式
- 当前巡检失败次数

(3) histogram指标监控项:

- 进程goroutine数量、gc次数、gc耗时、heap分配、stack分配
- 服务依赖资源的错误、耗时监控
- 策略运行的错误、耗时监控

> 同时服务暴露了pprof的http端口，可以查看更详细的程序运行状态。

### 7.Todo

- 批量写优化

- mongo支持

