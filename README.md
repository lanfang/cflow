# cflow
 高可用分布式调度系统, 支持时间调度(crontab功能)和依赖调度

# 系统完整流程


# 使用步骤


# 配置文件(json格式)
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
## 子命令列表 cflow -h 查看:
  {run,dep,do_all_job,migrate,version,executer,scheduler}

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
  
  