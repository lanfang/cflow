# cflow
 用python实现的高可用可横向扩展的分布式调度系统

# 包含功能
- 定时调度(类似linux crontab)
- 依赖调度(满足依赖关系后才会启动任务)
- 任务格式: 任意命令行可执行程序
- 高可用，可横向扩展(调度器和执行器均可同时运行多个，完善的重试机制)

# 系统完整流程
> 调度器获取任务，通过kafka进行任务分发，执行器消费kafka的任务并执行

![scheduler.png](https://github.com/lanfang/cflow/blob/master/docs/scheduler.png)

# 使用方法:
> cflow命令位于工程目录下

- 安装mysql和etcd，执行 patch.sql
- 运行调度器: cflow scheduler -c cflow_conf.json --ha
- 运行执行器(可启多个): cflow executer -c cflow_conf.json

# 配置文件介绍
> 配置文件为json格式
```
{
  "Common":{
    "MysqlConn":"mysql://test:test@localhost:3306/cflow", # 数据库地址
    "Broker":"localhost:9092"   # kafka地址
  },
  "Scheduler":{
    "LogDir":"/var/log/go_log", # 日志路径
    "LogFile":"scheduler.log", # 日志文件
    "FetchInterval":10, # 扫描cron_conf间隔(秒)
    "WorkerTimeOut":20, # woker 心跳超时时间
    "RetryQueueimes":4  # 超时任务，重新发起调度的次数
  },
  "Worker": {
    "LogDir":"/var/log/go_log",
    "LogFile":"executer.log",
    "Parallelism": 32, # 每个woker，并行执行任务的个数
    "HearteatInterval": 10, # worker 心跳间隔(秒)
    "Retry": 3, # 单个任务重试次数
    "TaskTimeOut": 60 # 执行单个任务时的超时时间(秒), -1 不设置超时
  }
}
```


# 库表介绍(database: cflow)
- cron_conf crontab 配置表，存储定时任务配置
- loader_result 抽取结果表，存抽取结果，以及worker直接文件同步
- stat_result 每天的执行结果
- task_define 普通任务定义
- task_dependency 任务依赖关系
- task_instance 所有的任务实例, 记录任务的运行状态信息等


# 常用命令介绍(cflow)
## 子命令列表 cflow -h:
```
{run,dep,do_all_job,migrate,version,kill,executer,scheduler} ...
```
## 重跑命令(cflow run)
### cflow run -h
```
查看参数列表和使用方法
```

### 重跑单个任务(需满足依赖关系)
```
cflow  run -j task_id -d YYYY-MM-DD  
```

### 重跑单个任务(无需满足依赖)
```
cflow  run -j task_id -d YYYY-MM-DD --force 
```

### 重跑任务以及其后置任务
```
cflow  run -j task_id -d YYYY-MM-DD -down
```

### 重跑任务以及其前置任务
```
cflow  run -j task_id -d YYYY-MM-DD -up
```
  
  