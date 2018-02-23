# -*- coding: utf-8 -*-

import time
import copy, json,sys
import croniter as cron
from datetime import datetime, timedelta
import threading

import models
from state import State
from utils import log
import utils.kafka_utils as kafka_utils
import config
import utils.process_utils as process_utils
class TimerInfo(object):
    def __init__(self, task_id, execute_time, etl_day=None):
        self.task_id = task_id
        self.execute_time = execute_time
        self.etl_day = etl_day

    def __repr__(self):
        return "task_id: {}, execute_time: {}, etl_day: {}"\
            .format(self.task_id, self.execute_time, self.etl_day)

class MainScheduler(object):
    """
    the MainScheduler is for crontab, fetch crontab config from mysql
    Keyword Arguments:
        broker: 'host[:port]' string (or list of 'host[:port]'strings)
        fetch_interval (int): scheduler fetch db interval seconds. Default: 30.
        woker_timeout (int): worker timeout milliseconds. Default: 300
        retry_push_times (int): repush cron job to queue times. Default:3
    """
    DEFAULT_CONFIG = {
        'broker': "localhost:9092",
        'fetch_interval': 30,
        'woker_timeout': -1,
        'retry_push_times': 3,
    }
    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        self.crons_conf = {}
        self.timer_list = []
        self.fetch_time = None
        self.depend_scheduler = None
        self.helper_scheduler = None

    def check_sub_threading(self):
        if self.depend_scheduler is None or \
                not self.depend_scheduler.isAlive():
            self.depend_scheduler = DependencyScheduler()
            self.depend_scheduler.setDaemon(True)
            self.depend_scheduler.start()
        else:
            log.logger.info("depend_scheduler is alive")
        if self.helper_scheduler is None or \
                not self.helper_scheduler.isAlive():
            self.helper_scheduler = SchedulerHelper()
            self.helper_scheduler.setDaemon(True)
            self.helper_scheduler.start()
        else:
            log.logger.info("helper_scheduler is alive")

    def run(self):
        log.logger.info("Starting the MainScheduler")
        kafka_utils.setup_kafka(config.G_Conf.Common.Broker)

        exception_count = 0
        exception_begin = datetime.now()
        max_exception_count = 5
        max_exception_seconds = 3 * 60

        while True:
            try:
                # check_sub_threading
                self.check_sub_threading()

                # get cron
                execute_timer = self.getMatchedCronTask()
                for timer in execute_timer:
                    self.run_timer(timer)
                    if timer.task_id in self.crons_conf \
                            and self.crons_conf[timer.task_id].cron_type != State.CRON_SINGLE:
                        self.addTimer(self.crons_conf[timer.task_id])

                # retry timeout worker
                self.retryZombieInstance()
                waiting = self._next_wait()
                time.sleep(waiting)
                exception_count = 0
            except Exception as e:
                exception_count += 1
                if exception_count == 1:
                    exception_begin = datetime.now()
                exception_duration = (datetime.now() - exception_begin).total_seconds()
                alert_msg = "if get exception {} times in {} seconds, " \
                            "the MainScheduler will exit, current:{} times/{} seconds, exception:{}-{}".\
                    format(max_exception_count, max_exception_seconds, exception_count, exception_duration, type(e), e.message)
                if exception_count >= max_exception_count and exception_duration >= max_exception_seconds:
                    alert_msg = "scheduler exit, do somthing {}".format(alert_msg)
                    log.logger.error(alert_msg)
                    process_utils.Alert(alert_msg)
                    sys.exit(1)
                else:
                    log.logger.error(alert_msg)
                    process_utils.Alert(alert_msg)

                time.sleep(10)

        log.logger.info("End the scheduler, exit main loop")

    def _next_wait(self):
        """
        main loop sleep scends
        :return: second
        """
        wait_seconds = self.config["fetch_interval"]
        now = datetime.now()
        if len(self.timer_list) > 0:
            timer = self.timer_list[0]
            distance = (timer.execute_time - now).total_seconds()
            if distance > 0 and distance >= wait_seconds:
                pass
            elif distance > 0 and distance < wait_seconds:
                wait_seconds = distance
            elif distance <= 0:
                wait_seconds = 0
        return wait_seconds

    def getMatchedCronTask(self):
        """
        getMatchedCronTask
        :return: matched crontab list
        """
        self._fetchNewCronTask()
        match_timer = []
        now = datetime.now()
        total_size = len(self.timer_list)
        for index, timer in enumerate(self.timer_list):
            if (now - timer.execute_time).total_seconds() >= 0:
                match_timer.append(timer)
            else:
                self.timer_list = self.timer_list[index:]
                break
        if len(match_timer) == total_size:
            self.timer_list = []
        return match_timer

    def _fetchNewCronTask(self):
        """
        update cron info
        :return:
        """
        cron_list = models.CronConf().getCronTask(fetch_time=self.fetch_time)
        for cron in cron_list:
            if (cron.task_id not in self.crons_conf) or (cron.start_time != self.crons_conf[cron.task_id].start_time):
                self.crons_conf[cron.task_id] = cron
                self.addTimer(cron_conf=cron)
        self.fetch_time = datetime.now()

    def calcExecuteTime(self, cron_format):
        """
        calcExecuteTime
        :param cron_format: crontab format time
        :return: next time to run
        """
        return cron.croniter(cron_format, datetime.now()).get_next(datetime)

    def addTimer(self, cron_conf):
        """
        addTimer
        :param cron_conf: cron conf
        :return:
        """
        self.timer_list = filter(lambda x: x.task_id != cron_conf.task_id, self.timer_list)
        now = datetime.now()
        execute_time = self.calcExecuteTime(cron_conf.start_time)
        if cron_conf.cron_type == State.CRON_SINGLE \
                and (now - execute_time).total_seconds() > 0:
                return
        self.timer_list.append(TimerInfo(task_id=cron_conf.task_id,
                                         execute_time=execute_time
                                         ))
        self.timer_list = sorted(self.timer_list, key=lambda info:info.execute_time)

    def calcCronEtlDay(self, cron_conf, execute_time):
        """
        calcCronEtlDay
        :param cron_conf: cron_conf
        :param execute_time: datetime
        :return: etl_day string
        """
        now = datetime.now()
        etl_day = None
        if cron_conf.cron_type == State.CRON_SINGLE:
            etl_day = cron_conf.start_time # 配置单次任务时，开始日期格式:'%Y-%m-%d %H:%M:%S'
        elif cron_conf.cron_type == State.CRON_INTERVAL:
            etl_day = (now - timedelta(seconds=self.calcInterval(cron_conf.start_time))) \
                .strftime('%Y-%m-%d %H:%M:%S')
        elif cron_conf.cron_type == State.CRON_HOUR:
            etl_day = (now - timedelta(seconds=self.calcInterval(cron_conf.start_time))) \
                .strftime('%Y-%m-%d %H:%M:%S')
        elif cron_conf.cron_type == State.CRON_DAY:
            etl_day = (execute_time + timedelta(days=-1)).strftime('%Y-%m-%d')
        elif cron_conf.cron_type == State.CRON_MONTH:
            etl_day = (datetime.date.today().replace(day=1) - datetime.timedelta(1)) \
                .strftime('%Y-%m')
        else:
            etl_day = now.strftime('%Y-%m-%d %H:%M:%S')
        return etl_day

    def calcInterval(self, cron_format):
        """
        "*/5 * * * *"
        :return 5 * 60
        :param format:
        :return: seconds
        """
        first = cron.croniter(cron_format, datetime.now()).get_next(datetime)
        second = cron.croniter(cron_format, first).get_next(datetime)
        return (second - first).total_seconds()

    def run_timer(self, timer):
        """
        :param task: timer info
        :return: err
        """
        log.logger.info(" begin run timer {} with time {}".format(timer.task_id, timer.execute_time))
        task_list = models.CronConf().getCronTask(timer.task_id)
        err = None
        for cron_conf in task_list:
            if cron_conf.enable != State.Enabled:
                err = " task {}, status {} is not enabled, stop running ".format(cron_conf.task_id,cron_conf.enable)
                log.logger.info(err)
                continue
            timer.etl_day = self.calcCronEtlDay(cron_conf, timer.execute_time)
            self.create_instance(cron_conf, timer.etl_day)
        return err

    def create_instance(self, cron_conf, etl_day):
        instance_list = []
        cron_log = models.CronLog().create_cron_log(cron_conf, etl_day)
        if cron_conf.task_id == State.ROOT_TASK:
            job_list = models.TaskDefine().get_valid_job()
            models.TaskInstance().create_job_task_instance(execute_date=etl_day, job_list=job_list)
        elif cron_conf.type == State.TASK_CRON:
            instance_list = models.TaskInstance().create_cron_task_instance(execute_date=etl_day, cron_list=[cron_conf])
        else:
            err = "task_id:{} name:{} type:{}, unknow task type".format(cron_conf.task_id, cron_conf.name, cron_conf.type)
            log.logger.info("{}".format(err))
            cron_log.update_cron_status(State.SHUTDOWN)
            return

        for instance in instance_list:
            err = kafka_utils.PushMsgWithRetry(kafka_utils.TOPIC_DISPATCHER,
                                           kafka_utils.TaskBeginMsg(
                                               instance_id=instance.id,
                                               task_id=instance.task_id,
                                               execute_date=instance.etl_day)
                                           )
            log.logger.info("push task to queue, instance {}, err {}".format(instance, err))
        cron_log.update_cron_status(State.SUCCESS)

    def retryZombieInstance(self):
        """
        fetchZombieInstance
        :param session:
        :return:
        """
        log.logger.debug("begin retry timeout task instance")
        instance_list = models.TaskInstance().create_retry_instance(timer_out=self.config["woker_timeout"],
                                                                    max_schedule=self.config["retry_push_times"])
        for instance in instance_list:
            err = kafka_utils.PushMsgWithRetry(kafka_utils.TOPIC_DISPATCHER,
                                               kafka_utils.TaskBeginMsg(
                                                   instance_id=instance.id,
                                                   task_id=instance.task_id,
                                                   execute_date=instance.etl_day)
                                               )
            log.logger.debug("retry timeout task instance {}, err {}".format(instance, err))

        log.logger.info("end retry timeout task instance")


class DependencyScheduler(threading.Thread):
    """
    DependencyScheduler is for dependency task
    Keyword Arguments:
        broker: 'host[:port]' string (or list of 'host[:port]'strings)
        fetch_interval (int): scheduler fetch db interval seconds. Default: 30.
        woker_timeout (int): worker timeout milliseconds. Default: 300
        retry_push_times (int): repush cron job to queue times. Default:3
    """
    DEFAULT_CONFIG = {
        'broker': "localhost:9092",
        'fetch_interval': 1000,
        'woker_timeout': 10000,
        'retry_push_times': 3,
    }
    def __init__(self, **configs):
        super(DependencyScheduler, self).__init__(name="DependencyScheduler")
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

    def run(self):
        log.logger.info("Starting the DependencyScheduler")
        def gen_obj(d):
            return kafka_utils.TaskOverMsg(d['instance_id'],
                                           d['task_id'],
                                           d['status'],
                                           d['execute_date'])
        exception_count = 0
        exception_begin = datetime.now()
        max_exception_count = 5
        max_exception_seconds = 3 * 60

        while True:
            try:
                for msg in kafka_utils.scheduler_consumer:
                    msg_obj = json.loads(msg.value, object_hook=gen_obj)
                    log.logger.info("get task result:{}".format(msg_obj))
                    #kafka_utils.scheduler_consumer.commit()

                    # the worker push msg only success
                    met_task = models.TaskDependency().downstream_met_dependency(task_id=msg_obj.task_id,
                                                                                 execute_date=msg_obj.execute_date)
                    if len(met_task) > 0:
                        self.run_task(msg_obj.execute_date, met_task)
                else:
                    log.logger.info("begin fetch waiting_dep task list")
                    self.run_task()
                    log.logger.info("end fetch waiting_dep task list ")
            except Exception as e:
                exception_count += 1
                if exception_count == 1:
                    exception_begin = datetime.now()
                exception_duration = (datetime.now() - exception_begin).total_seconds()
                alert_msg = "if get exception {} times in {} seconds, " \
                            "the DependencyScheduler will exit, current:{} times/{} seconds, exception:{}-{}". \
                    format(max_exception_count, max_exception_seconds, exception_count, exception_duration, type(e), e.message)
                if exception_count >= max_exception_count and exception_duration >= max_exception_seconds:
                    alert_msg = "scheduler exit, do somthing {}".format(alert_msg)
                    log.logger.error(alert_msg)
                    process_utils.Alert(alert_msg)
                    return
                else:
                    log.logger.error(alert_msg)
                    process_utils.Alert(alert_msg)
                time.sleep(10)
        log.logger.info("Quit the DependencyScheduler")

    def run_task(self, execute_date=None, task_list=None):
        """
        run_dep_task
        :param execute_date: YYYY-MM-DD
        :param task_list: task_list
        :return:
        """
        new_instance = []
        if execute_date is None or task_list is None:
            waiting_dep_instance = models.TaskInstance().get_waiting_dep_instance()
            for waiting_task in waiting_dep_instance:
                etl_day = waiting_task.etl_day
                met, msg = models.TaskDependency().\
                    is_met_dependency(waiting_task.task_id,
                                      waiting_task.etl_day)
                if met:
                    tmp = models.TaskInstance(). \
                        job_prepare_running(etl_day, [waiting_task.task_id])
                    if len(tmp) > 0:
                        new_instance.extend(tmp)
                else:
                    log.logger.info("etl_day: {}, task {} not met dependency -> {}".format(etl_day, waiting_task.task_id, msg))
        else:
            new_instance = models.TaskInstance(). \
                job_prepare_running(execute_date, task_list)

        log.logger.info(" queued new_instance {}".format(new_instance))
        for instance in new_instance:
            err = kafka_utils.PushMsgWithRetry(kafka_utils.TOPIC_DISPATCHER,
                                               kafka_utils.TaskBeginMsg(
                                                   instance_id=instance.id,
                                                   task_id=instance.task_id,
                                                   execute_date=instance.etl_day)
                                       )
            log.logger.info("push task to queue, instance {}, err {}".format(instance, err))

class SchedulerHelper(threading.Thread):
    """
    SchedulerHelper SchedulerHelper
    """
    DEFAULT_CONFIG = {
    }
    def __init__(self, **configs):
        super(SchedulerHelper, self).__init__(name="SchedulerHelper")
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)
        self.scheduler_finished_day = {}

    def run(self):
        log.logger.info("Starting SchedulerHelper")
        exception_count = 0
        exception_begin = datetime.now()
        max_exception_count = 5
        max_exception_seconds = 3 * 60

        while True:
            try:
                self.check_scheduler_result()
                time.sleep(30)
            except Exception as e:
                exception_count += 1
                if exception_count == 1:
                    exception_begin = datetime.now()
                exception_duration = (datetime.now() - exception_begin).total_seconds()
                alert_msg = "if get exception {} times in {} seconds, " \
                            "the SchedulerHelper will exit, current:{} times/{} seconds, exception:{}-{}". \
                    format(max_exception_count, max_exception_seconds, exception_count, exception_duration, type(e), e.message)
                if exception_count >= max_exception_count and exception_duration >= max_exception_seconds:
                    alert_msg = "scheduler exit, do somthing {}".format(alert_msg)
                    log.logger.error(alert_msg)
                    process_utils.Alert(alert_msg)
                    return
                else:
                    log.logger.error(alert_msg)
                    process_utils.Alert(alert_msg)
                time.sleep(10)
        log.logger.info("Quit the SchedulerHelper")

    def check_scheduler_result(self, etl_day=None):
        """
        check_scheduler_result, collect execution results
        :param etl_day: YYYY-MM-DD
        :return:
        """
        if etl_day is None:
            etl_day = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
        if etl_day in self.scheduler_finished_day:
            pass

        finished = models.StatResult().update_stat(etl_day)
        if finished:
            self.scheduler_finished_day[etl_day] = True


class Admin(object):
    def __init__(self):
        pass

    def run_all_job(self, date_list):
        """
        run_all_job
        :param date_list:
        :return:
        """
        job_list = models.TaskDefine().get_valid_job()
        msg = ""
        for etl_day in date_list:
            print len(job_list), etl_day
            models.TaskInstance().create_job_task_instance(etl_day, job_list)
        msg = "generate {} job task instance ".format(len(date_list) * len(job_list))
        return msg

    def rerun_task(self, task_id, date_list, up_and_down=False, run_up=False, run_down=False, force=False):
        """
        rerun_task
        :param task_id: task_id
        :param date_list: range list
        :param run_up:run upstream
        :param run_down: run downstream
        :return:
        """
        kafka_utils.setup_kafka(config.G_Conf.Common.Broker)
        # run job define
        instance_list = []
        msg = ""
        run_type = ""
        for loop in ["looop"]:
            _ = loop
            job = models.TaskDefine().get_job_by_task_id(task_id_list=[task_id])
            if job and len(job) > 0:
                run_type="job"
                job_list = []
                if run_up:
                    job_list = models.TaskDependency().get_all_upstream(task_id)
                elif run_down:
                    job_list = models.TaskDependency().get_all_downstream(task_id)
                elif up_and_down:
                    up_job = models.TaskDependency().get_all_upstream(task_id)
                    down_job = models.TaskDependency().get_all_downstream(task_id)
                    if len(up_job) > 0:
                        job_list.extend(up_job)
                    if len(down_job) > 0:
                        job_list.extend(down_job)
                else:
                    # run a job with force
                    if force:
                        for etl_day in date_list:
                            tmp = models.TaskInstance().direct_run_single_job_task(etl_day, job)
                            if tmp > 0:
                                instance_list.extend(tmp)
                        break
                    else:
                        # run single waiting dependency
                        pass

                # add self
                job_list.append(task_id)
                need_run_job_list = models.TaskDefine().get_job_by_task_id(task_id_list=job_list)
                if need_run_job_list and len(need_run_job_list) > 0:
                    for etl_day in date_list:
                        models.TaskInstance().create_job_task_instance(execute_date=etl_day, job_list=need_run_job_list)
                    msg = "generate {} TaskDefine task instance ".format(len(need_run_job_list) * len(date_list))
                break

            # run cron task
            cron = models.CronConf().get_cron_by_task_id(task_id=task_id)
            if cron:
                run_type = "cron"
                for etl_day in date_list:
                    tmp = models.TaskInstance().create_cron_task_instance(execute_date=etl_day, cron_list=[cron])
                    if len(tmp) > 0:
                        instance_list.extend(tmp)
                break

        for instance in instance_list:
            err = kafka_utils.PushMsgWithRetry(kafka_utils.TOPIC_DISPATCHER,
                                               kafka_utils.TaskBeginMsg(
                                                   instance_id=instance.id,
                                                   task_id=instance.task_id,
                                                   execute_date=instance.etl_day)
                                               )
            log.logger.info("push task to queue, instance {}, err {}".format(instance, err))
        if len(instance_list) > 0:
            msg = "generate {} {} task instance ".format(len(instance_list), run_type)
        return msg

    def migrate_dependency(self,  job_id=None, delete=False):
        """
        migrate_dependency
        :param delete:  delete data
        :param job_id:  if job_id is None, migrate all job
        :return:
        """
        models.TaskDependency().migrateDep(job_id=job_id, delete=delete)

    def migrate_lastid(self, etl_day):
        models.migrate_last_id(etl_day)

    def query_dep(self, task_id, etl_day):
        met, msg = models.TaskDependency(). \
            is_met_dependency(task_id, etl_day)
        if met:
            print "task {}, etl_day {} met dependency".format(task_id, etl_day)
        else:
            print "task {}, etl_day {} not met dependency: {}".format(task_id, etl_day, msg)


    def kill_task(self, task_id, date_list):
        """
        kill_task
        :param task_id:
        :param date_list:
        :return:
        """
        return models.TaskInstance().kill_instance(task_id, date_list)

