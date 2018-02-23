# -*- coding: utf-8 -*-

import os, time
import json
import subprocess, socket
import threading
import psutil
import copy
import sys
import utils.log as log
import utils.kafka_utils as kafka_utils
import utils.process_utils as process_utils

import models
from state import State
import config

reload(sys)
sys.setdefaultencoding("utf-8")


class BaseTaskRunner(object):

    def __init__(self, instance):
        self.task_instance = instance
        self.process = None

    def _read_task_log(self, stream):
        while True:
            line = stream.readline()
            if len(line) == 0:
                break
            log.logger.info('Subtask-({}~{}~{}): {}'.format(self.task_instance.task_id,
                                                          self.task_instance.id,
                                                          self.task_instance.etl_day,
                                                          line.rstrip('\n')))

    def run_command(self):
        full_cmd = self.task_instance.command
        log.logger.info('Running Command: [{}]'.format(full_cmd))
        proc = subprocess.Popen(
            full_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True
        )
        # Start daemon thread to read subprocess log.loggerging output

        log.logger_reader = threading.Thread(
            target=self._read_task_log,
            args=(proc.stdout,),
        )
        log.logger_reader.daemon = True
        log.logger_reader.start()

        return proc

    def start(self):
        """
        Start running the task instance in a subprocess.
        """
        raise NotImplementedError()

    def return_code(self):
        """
        :return: The return code associated with running the task instance or
        None if the task is not yet done.
        :rtype int:
        """
        raise NotImplementedError()

    def terminate(self):
        """
        Kill the running task instance.
        """
        raise NotImplementedError()

    def on_finish(self):
        """
        A callback that should be called when this is done running.
        """
        if self._cfg_path and os.path.isfile(self._cfg_path):
            subprocess.call(['sudo', 'rm', self._cfg_path])


class BashTaskRunner(BaseTaskRunner):
    """
    Runs the raw command by invoking through the Bash shell.
    """
    def __init__(self, local_task_job):
        super(BashTaskRunner, self).__init__(local_task_job)

    def start(self):
        self.process = self.run_command()

    def return_code(self):
        return self.process.poll()

    def terminate(self):
        """
        if self.process.stdout:
            self.process.stdout.close()

        if self.process.stdin:
            self.process.stdin.close()

        if self.process.stderr:
            self.process.stderr.close()
        """

        if self.process and psutil.pid_exists(self.process.pid):
            process_utils.kill_process_tree(log.logger, self.process.pid)

    def terminate_proc(self):
        try:
            process_utils.kill_process_tree(log.logger, self.process.pid)
            self.process.terminate()
        except Exception as e:
            pass

    def on_finish(self):
        super(BashTaskRunner, self).on_finish()


class WorkerInstance(threading.Thread):
    def __init__(self, instance, heartbeat_interval, timeout, retry):
        """
        :param instance: task instacne
        :param heartbeat_interval:heartbeat
        :param timeout: instance time out
        :param retry: retry times
        """
        super(WorkerInstance, self).__init__(name="WorkerInstance")
        self.instance = instance
        self.heartbeat_interval = heartbeat_interval
        self.timeout = timeout
        self.retry = retry
        self.begin_time = time.time()
        self.step_seconds = 20

    def run(self):
        running_times = 0
        msg = None
        try:
            while running_times <= self.retry:
                task_runner = BashTaskRunner(self.instance)
                self.begin_time = time.time()
                self.instance.worker_retry = running_times
                should_run = self.instance.start_running(retry=(True if running_times > 0 else False))
                if should_run is not None:
                    log.logger.info("{}".format(should_run))
                    msg = None
                    break
                ret = self.inner_run(task_runner, running_times)
                if ret is None:
                    self.instance.stop_running(State.SUCCESS)
                    kafka_utils.PushMsgWithRetry(kafka_utils.TOPIC_TASK_RESULT,
                                                       kafka_utils.TaskOverMsg(
                                                           instance_id=self.instance.id,
                                                           task_id=self.instance.task_id,
                                                           status=State.SUCCESS,
                                                           execute_date=self.instance.etl_day)
                                                       )
                    msg = None
                    break
                else:
                    msg = "the {} times running:{}".format(running_times, ret)
                    if self.instance.status == State.KILLED:
                        # if instance is killd, should stop running
                        break
                    elif self.instance.status == State.TIMEOUT:
                        self.instance.stop_running(State.TIMEOUT)
                    else:
                        self.instance.stop_running(State.FAILED)

                    if running_times < self.retry:
                        msg = "{}, after {} seconds will try the {} times ".format(msg, self.step_seconds * (running_times+1), running_times + 1)
                    log.logger.error(msg)
                running_times += 1
                if running_times <= self.retry:
                    time.sleep(self.step_seconds * running_times)
            else:
                msg = "reach the max retry times {} with err:{}, stop running".format(self.retry, msg)
                log.logger.info(msg)

        except Exception as e:
            msg = "get Exception {}.{}".format(type(e), e.message)
            log.logger.error(msg)
        finally:
            process_utils.ref_counter.unref()
            if msg is not None:
                keeper = "unknown"
                log.logger.error("run {}, err: {}".format(self.instance, msg))
                if self.instance.task_type == State.TASK_JOB:
                    job_list = models.TaskDefine().get_job_by_task_id([self.instance.task_id])
                    if len(job_list) > 0:
                        keeper = "{}({})".format(self.instance.task_id, job_list[0].keeper)
                    else:
                        # should not come here
                        keeper = "{}".format(self.instance.task_id)
                elif self.instance.task_type == State.TASK_EXTRACT:
                    keeper = "{}(rule_id:{})".format(self.instance.task_id, self.instance.sub_task_id)
                elif self.instance.task_type == State.TASK_CRON:
                    keeper = "{}(定时任务)".format(self.instance.task_id)
                else:
                    pass
                msg = "\nTask: {} \nError: {} \nContext: {}".format(keeper, msg, self.instance)
                process_utils.Alert(msg)
        return

    def inner_run(self, runner, times):
        msg = None
        heartbeat_times = 0
        for x in ["run"]:
            _ = x
            return_code = -1
            try:
                runner.start()
                last_heartbeat = self.begin_time - 2 * self.heartbeat_interval
                running = True
                while running:
                    now = time.time()
                    if (last_heartbeat + self.heartbeat_interval) <= now:
                        heartbeat_times += 1
                        h_ret, new_instance = self.instance.heartbeat_instance()
                        if h_ret == 0 and heartbeat_times > self.retry:
                            raise_msg = "heartbeat failed"
                            if new_instance is not None and new_instance.status == State.KILLED:
                                self.instance.status = State.KILLED
                                raise_msg = "instance is killed"
                            else:
                                # if worker heartbeat failed and reach the max retry times stop running instance
                                self.instance.status = State.FAILED
                            raise Exception(raise_msg)
                        last_heartbeat = now
                    return_code = runner.return_code()
                    if return_code is not None:
                        log.logger.info("instance {}, exited with return code {}".format(self.instance, return_code))
                        running = False
                    if self.timeout > 0 and (self.begin_time + self.timeout) <= now:
                        # worker execute the subtask timeout
                        msg = "execute timeout {}".format(self.timeout)
                        self.instance.status = State.TIMEOUT
                        running = False
                    time.sleep(0.1)
                else:
                    if return_code != 0:
                        msg = "return code is {}, the correct code is 0".format(return_code)
            except Exception as e:
                log.logger.error("get exeception {}.{}".format(type(e), e.message))
                msg = "get Exception: {}".format(e.message)
            finally:
                runner.terminate_proc()
        return msg

class Worker(object):

    """
    DependencyScheduler is for dependency task
    Keyword Arguments:
        parallelism (int): max worker instance at the same time
        heartbeat_interval (int): worker heartbeat to the scheduler
        woker_timeout (int): non-negative, -1 means never timeout
        retry_times (int): retry the worker times
        orphaned_node_wait_seconds (int): if worker node leave from the cluster, wait seconds to join again
        orphaned_node_rejoin_times (int): orphaned node rejoin the cluster times, -1 means always try
    """
    DEFAULT_CONFIG = {
        'parallelism': 32,
        'heartbeat_interval': 5,
        'woker_timeout': -1,
        'retry_times': 3,
        'orphaned_node_wait_seconds': 60,
        'orphaned_node_rejoin_times': -1,
    }
    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)
        self.running_worker = []

    def __del__(self):
        log.logger.info("cflow worker quit, ip:{}, host:{} ".format(process_utils.getServerIp(), socket.gethostname()))

    def run(self):
        log.logger.info("Starting the Worker")
        kafka_utils.setup_kafka(config.G_Conf.Common.Broker)
        """
        sync_file_worker = SyncFileWorker()
        sync_file_worker.setDaemon(True)
        sync_file_worker.start()
        """
        waiting_seconds = self.config["orphaned_node_wait_seconds"]
        rejoin_times = self.config["orphaned_node_rejoin_times"]
        node_info="cflow worker node [{}-{}]".format(socket.gethostname(), process_utils.getServerIp())

        already_join_times = 1
        def gen_obj(d):
            return kafka_utils.TaskBeginMsg(d['instance_id'], d['task_id'], d['execute_date'])

        # main logic
        while True:
            log.logger.info("{} join cluster ".format(node_info))
            try:
                instance_msg = None
                for msg in kafka_utils.worker_consumer:
                    try:
                        instance_msg = json.loads(msg.value, object_hook=gen_obj)

                        # parallelism limit
                        self.block_to_run(instance_msg)

                        err, instance = self.prepare_to_run(instance_msg.instance_id)

                        # if the instance run not success, scheduler will rerun
                        #kafka_utils.worker_consumer.commit()
                        if err is not None or instance is None:
                            log.logger.error("run instace {}, err_msg {}".format(instance_msg, err))
                            process_utils.ref_counter.unref()
                            continue

                        # start a subprocess run the instance
                        self.run_single_task(instance)
                    except Exception as e:
                        msg = "{} run instance {}, execption:{},{}".format(node_info, instance_msg, type(e), e.message)
                        log.logger.error(msg)
                        process_utils.Alert(msg)
                else:
                    alert_msg = "{} kafka msg is empty, quit then waiting msg ".format(node_info)
                    log.logger.info(alert_msg)
            except Exception as e:
                alert_msg = "{}, get exception  {}, {} ".format(node_info, type(e), e.message)
                process_utils.Alert(alert_msg)

            # worker_consumer is blocking, if come here should try join cluster
            if rejoin_times < 0 or (already_join_times < rejoin_times):
                alert_msg = "{} break from the cluster, waiting {} seconds then join again ".format(node_info, waiting_seconds)
                log.logger.info(alert_msg)
                #process_utils.Alert(alert_msg)
                time.sleep(waiting_seconds)
                already_join_times += 1
            else:
                alert_msg = "{} exit, reach the max join cluster times ".format(node_info)
                process_utils.Alert(alert_msg)
                break

        log.logger.info("Quit the Worker")

    def prepare_to_run(self, instance_id):
        """
        prepare_to_run
        :param instance_id:
        :return:
        """
        err, task_instace = models.TaskInstance().refresh_instance(instance_id)
        return err, task_instace


    def run_single_task(self, instance):
        """
        begin run task
        :param instance: task instance
        :return:
        """
        retry_times = self._get_retry_times(instance)
        worker_instance = WorkerInstance(instance,
                                         heartbeat_interval=self.config["heartbeat_interval"],
                                         timeout=self.config["woker_timeout"],
                                         retry=retry_times)
        worker_instance.setDaemon(True)
        worker_instance.start()

    def _get_retry_times(self, task_instace):
        """
        _get_retry_times, extract task and the table is splited, wo should not retry
        :param task_instace:
        :return:
        """
        retry_times = self.config["retry_times"]
        if task_instace.task_type == State.TASK_EXTRACT:
            #抽取任务统一重试更长次数, 25次，约108分钟
            retry_times = 25
            # count = models.ExtractRule().get_dest_table_rule_count(task_instace.task_id)
            # if count > 1:
            #     retry_times = 1
        return retry_times


    def block_to_run(self, task):
        while True:
            ok, val = process_utils.ref_counter.ref(self.config["parallelism"])
            if ok:
                log.logger.info("{}/{},  ready to run {}".format(val, self.config["parallelism"], task))
                break
            else:
                time.sleep(3)
                log.logger.info("{}/{}, block to run {}".format(val + 1, self.config["parallelism"], task))


class SyncFileWorker(threading.Thread):
    def __init__(self):
        super(SyncFileWorker, self).__init__(name="SyncFileWorker")

    def run(self):
        """
        try to sync file from other node
        :return:
        """
        log.logger.info("Starting the SyncFileWorker")
        def gen_obj(d):
            return kafka_utils.SyncFileMsg(d['file_id'])
        waiting_seconds = 20
        while True:
            try:
                for msg in kafka_utils.sync_file_consumer:
                    msg_obj = json.loads(msg.value, object_hook=gen_obj)
                    log.logger.info("SyncFileWorker file_info:{}".format(msg_obj))
                    sync_file = models.LoaderResult().get_dumped_file_by_id(msg_obj.file_id)
                    #kafka_utils.sync_file_consumer.commit()
                    if sync_file is None:
                        continue

                    if os.path.exists(sync_file.dumped_file):
                        continue

                    tmp_file = "{}_rsync_tmp".format(sync_file.dumped_file)
                    command = sync_file.gen_command(tmp_file)
                    dumped_path = os.path.dirname(sync_file.dumped_file)
                    if not os.path.exists(dumped_path):
                        os.makedirs(dumped_path)
                    err = None
                    try:
                        subprocess.check_output(command, shell=True)
                        os.rename(tmp_file, sync_file.dumped_file)
                    except subprocess.CalledProcessError as e:
                        err = e
                    if err is not None:
                        alert_msg = "SyncFileWorker run command [{}] with err: [{}]".format(command, err)
                        log.logger.error(alert_msg)
                        process_utils.Alert(alert_msg)
                else:
                        log.logger.info("SyncFileWorker sync_file_consumer msg is empyt, waiting {} seconds try again".format(waiting_seconds))
                        time.sleep(waiting_seconds)
            except Exception as e:
                alert_msg="SyncFileWorker: {} {} ".format(type(e), e.message)
                log.logger.error(alert_msg)
                process_utils.Alert(alert_msg)

