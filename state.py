# -*- coding: utf-8 -*-


class State(object):
    # scheduler
    NONE = None
    REMOVED = "removed"
    SCHEDULED = "scheduled"

    # set by the executor (t.b.d.)
    # LAUNCHED = "launched"

    # set by a task
    WAITINGDEP = "waiting_dep"
    QUEUED = "queued"
    RUNNING = "running"
    TIMEOUT = "timeout"
    SUCCESS = "success"
    FAILED = "failed"
    KILLED = "killed"
    ABANDONNED = "abandoned"
    PREPAREED = "prepared"


    PAUSED = "paused"
    UP_FOR_RETRY = "up_for_retry"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    SHUTDOWN = "shutdown"  # External request to shut down

    FINISHED_STATUS = [FAILED, SUCCESS, ABANDONNED, TIMEOUT]

    # cron
    Enabled = "enabled"
    Distabled = "disabled"

    # cron execute type
    NORMAL = "normal"
    RETRY = "retry"

    # task type
    # 抽取任务(extract_task), 定时任务(cron_task), job_define任务(job_task)
    TASK_EXTRACT = "task_extract" # for crontabl extract
    TASK_CRON = "task_cron" # for crontab normal task
    TASK_JOB = "task_job" # for job_define task

    # cron type
    # 单次任务(single)，间隔任务(interval)，时任务(hour)，日任务(day)，月任务等(month)'
    CRON_SINGLE = "single"
    CRON_INTERVAL = "interval"
    CRON_HOUR = "hour"
    CRON_DAY = "day"
    CRON_MONTH = "month"

    FAKE_JOB = "fake_job"
    ROOT_TASK = 'root_task'