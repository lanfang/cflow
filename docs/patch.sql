create database cflow;

CREATE TABLE cflow.cron_conf(
  id bigint(20) NOT NULL AUTO_INCREMENT,
  task_id varchar(126) NOT NULL DEFAULT '0' COMMENT '任务ID',
  name varchar(512) NOT NULL DEFAULT '' COMMENT '任务名称',
  enable varchar(32) NOT NULL DEFAULT 'enabled' COMMENT '是否开启 enabled, disabled',
  module tinyint(4) NOT NULL DEFAULT '0' COMMENT '该任务属于哪个模块',
  cron_type varchar(250) NOT NULL DEFAULT '0' COMMENT '任务类型(单次任务(single)，间隔任务(interval)，时任务(hour)，日任务(day)，月任务等(month))',
  command text COMMENT '完整命令',
  start_time varchar(32) NOT NULL DEFAULT '' COMMENT 'crontab格式: 分,时,日,月,星期',
  redo_param varchar(250) DEFAULT '' COMMENT '重试参数:2 4 8',
  modify_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY I_task_id (task_id),
  KEY I_modify_time (modify_time)
 )ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT '定时任务配置';


CREATE TABLE cflow.task_define(
  id bigint(20) NOT NULL AUTO_INCREMENT,
  task_id varchar(250) DEFAULT NULL COMMENT '任务ID',
  task_name varchar(200) DEFAULT NULL COMMENT '任务名称',
  command text COMMENT '完整命令',
  enable varchar(32) NOT NULL DEFAULT 'enabled' COMMENT '是否开启 enabled, disabled',
  keeper varchar(40) DEFAULT NULL COMMENT '负责人',
  PRIMARY KEY (id),
  UNIQUE KEY UK_task_id (task_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT '任务定义';


CREATE TABLE cflow.task_instance (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  etl_day varchar(32) NOT NULL DEFAULT 'unknown' COMMENT '统计日期 YYYYMMDD',
  task_id varchar(250) NOT NULL DEFAULT '0' COMMENT '任务ID(脚本名称), 如果是抽取任务dest_table',
  sub_task_id varchar(250) DEFAULT '0' COMMENT '子任务ID',
  name varchar(512) NOT NULL DEFAULT 'unknown' COMMENT 'task名称',
  task_type varchar(32)  NOT NULL DEFAULT 'unknown' COMMENT '类型 抽取:extract, 普通任务:normal',
  module varchar(32)  NOT NULL DEFAULT 'bi' COMMENT '该任务属于哪个模块',
  status varchar(32)  NOT NULL DEFAULT 'unknown' COMMENT '执行结果 paused, waiting_dep, queued, running, success, failed',
  scheduler_time datetime DEFAULT NULL COMMENT '开始调度时间',
  begin_time datetime DEFAULT NULL COMMENT '开始执行时间',
  end_time datetime DEFAULT NULL COMMENT '结束执行时间',
  heartbeat datetime DEFAULT NULL COMMENT '执行器心跳时间，如果心跳超时,调度器会重新调度',
  command varchar(512) DEFAULT '' COMMENT '执行的命令',
  hostname varchar(512) DEFAULT '' COMMENT '主机名称',
  worker_retry int(11) DEFAULT 0 COMMENT '执行器重试次数',
  scheduler_retry int(11) DEFAULT 0 COMMENT '调度器重试次数',
  PRIMARY KEY (id),
  KEY I_task_id (task_id),
  KEY I_etl_day (etl_day)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT '任务实例';



CREATE TABLE cflow.cron_log (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  task_id varchar(250) NOT NULL DEFAULT '0' COMMENT '任务ID(脚本名称), 如果是抽取任务dest_table',
  name varchar(32) NOT NULL DEFAULT 'unknown' COMMENT 'task名称',
  module varchar(32)  NOT NULL DEFAULT 'bi' COMMENT '该任务属于哪个模块',
  running_type varchar(32)  NOT NULL DEFAULT 'normal' COMMENT 'normal, retry',
  status varchar(32)  NOT NULL DEFAULT 'unknown' COMMENT '执行结果 paused, waiting_dep, queued, running, success, failed',
  etl_day varchar(32) NOT NULL DEFAULT 'unknown' COMMENT '统计日期 YYYYMMDD',
  begin_time datetime DEFAULT NULL COMMENT '开始执行时间',
  end_time datetime DEFAULT NULL COMMENT '结束执行时间',
  command varchar(512) DEFAULT '' COMMENT '执行的命令',
  PRIMARY KEY (id),
  KEY I_task_id (task_id),
  KEY I_etl_day (etl_day)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT '定时任务日志';


CREATE TABLE cflow.task_dependency (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  task_id varchar(250) NOT NULL DEFAULT '' COMMENT '任务ID',
  dependency_task_id varchar(250) NOT NULL DEFAULT '0' COMMENT '依赖的任务ID',
  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT '任务依赖关系表';


CREATE TABLE cflow.stat_result (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  etl_day varchar(32) NOT NULL DEFAULT 'unknown' COMMENT '统计日期 YYYY-MM-DD',
  extract_total_count bigint(11) DEFAULT 0 COMMENT '抽取任务总数',
  extract_finish_count bigint(11) DEFAULT 0 COMMENT '已完成抽取任务总数',
  extract_success_count bigint(11) DEFAULT 0 COMMENT '已成功抽取任务总数',
  extract_status varchar(32) DEFAULT 'running' COMMENT 'running, success',
  extract_notice tinyint(4) DEFAULT 0 COMMENT '已发送通知消息 1: 已发送',
  job_total_count bigint(11) DEFAULT 0 COMMENT 'job总数',
  job_finish_count bigint(11) DEFAULT 0 COMMENT '已完成job总数',
  job_success_count bigint(11) DEFAULT 0 COMMENT '已成功抽取job总数',
  job_status varchar(32) DEFAULT 'running' COMMENT 'running, success',
  job_notice tinyint(4) DEFAULT 0 COMMENT '已发送通知消息 1: 已发送',
  modify_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY I_etl_day (etl_day)
) ENGINE=InnoDB AUTO_INCREMENT=141 DEFAULT CHARSET=utf8mb4 COMMENT '统计结果';

insert into cron_conf(task_id, name, enable, type, module, cron_type, start_time, command) values('root_task','调度根任务', 'enabled', 'task_cron', 1, 'day','00 00 * * *', 'python xxxx.py(this is fake command)')
