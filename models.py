# coding=utf-8
import os
import socket
from sqlalchemy import Column, String, Integer, Text, BigInteger, DateTime, SmallInteger
from sqlalchemy import distinct, func, or_
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy.orm.exc import NoResultFound

from datetime import datetime, timedelta

from utils.db import provide_session
from utils import log,process_utils
from state import State

class JobCommand(object):
    def __init__(self, cmd, job):
        self.cmd = cmd
        self.job = job


class Task(object):
    def __init__(self, task_id=None, name=None, task_type=None, sub_count=0, module=None, command=None, raw=None):
        self.task_id = task_id
        self.name = name
        self.task_type = task_type
        self.sub_count = sub_count
        self.module = module
        self.command = command
        self.raw = raw
    def __repr__(self):
        return "Task {self.task_id} {self.name} {self.task_type} {self.sub_count}".format(**locals())

class TaskDefine(Base):
    """
    任务配置表
    """
    __tablename__ = "task_define"

    id = Column(BigInteger, primary_key=True)
    task_id = Column(String(250))
    task_name = Column(String(256))
    command = Column(Text)
    enable = Column(String(20))
    keeper = Column(String(250))

    @provide_session
    def get_valid_job(self, session=None):
        jobs_list = session.query(TaskDefine).filter(TaskDefine.enable == State.Enabled).all()
        return jobs_list

    def job_command(self, execute_date):
        cmd = "{} {}".format(self.command, execute_date)
        if False:
            cmd = "{} {}".format('python /home/etl/bi_code/cflow/mock_script/mock_1.py', execute_date)
        return cmd

    @provide_session
    def get_job_by_task_id(self, task_id_list, session=None):
        """
        get_job_by_task_id
        :param task_id_list:
        :param session:
        :return: TaskDefine
        """
        status = [1, 2]
        job_list = session.query(TaskDefine)\
            .filter(TaskDefine.task_id.in_(task_id_list))\
            .filter(TaskDefine.enable == State.Enabled).all()
        return job_list

class CronConf(Base):
    """
    crontab配置表
    """
    __tablename__ = "cron_conf"

    id = Column(BigInteger, primary_key=True)
    task_id = Column(String(250))
    name = Column(String(512))
    enable = Column(SmallInteger)
    module = Column(SmallInteger)
    type = Column(String(250))
    cron_type = Column(String(250))
    command = Column(Text)
    start_time = Column(String(32))
    redo_param = Column(String(500))
    modify_time = Column(DateTime)

    @provide_session
    def get_cron_by_task_id(self, task_id, session=None):
        """
        get_cron_by_task_id
        :param task_id:
        :param session:
        :return: CronConf
        """
        cron = session.query(CronConf).filter(CronConf.task_id == task_id).one()
        return cron

    @provide_session
    def getCronTask(self, task_id=None, fetch_time=None, session=None):
        """
        根据任务ID查询
        :param task_id: 任务ID
        :param fetch_time:
        :param session:
        :return: CronConfig
        """
        query = session.query(CronConf)
        if task_id is not None:
            query = query.filter(CronConf.task_id == task_id)
        if fetch_time is not None:
            query = query.filter(
                func.cast(CronConf.modify_time, DateTime)
                >= func.cast(fetch_time, DateTime))
        return query.all()

    def cron_command(self, param=None):
        """
        gen a cron command
        :return:
        """
        cmd = self.command
        if param is not None:
            cmd = "{} {}".format(self.command, param)
        return cmd

class TaskInstance(Base):
    """
    调度器触发的任务实例表
    """
    __tablename__ = "task_instance"

    id = Column(BigInteger, primary_key=True)
    etl_day = Column(String(32))
    task_id = Column(String(250))
    sub_task_id = Column(String(250))
    name = Column(String(512))
    task_type = Column(String(32))
    module = Column(String(32), default="bi")
    status = Column(String(32))
    scheduler_time = Column(DateTime)
    begin_time = Column(DateTime)
    end_time = Column(DateTime)
    heartbeat = Column(DateTime)
    command = Column(String(512))
    hostname = Column(String(512))
    worker_retry = Column(Integer)
    scheduler_retry = Column(Integer)

    def __repr__(self):
        return "[ task_id: {}, sub_task_id: {}, etl_day: {}, instance_id: {}, statue {}, command {}]".format(
            self.task_id, self.sub_task_id, self.etl_day, self.id, self.status, self.command)

    @provide_session
    def get_instance_result(self, etl_day, task_type, session=None):
        """
        get_instance_result
        :param etl_day:  YYYY-MM-DD
        :param session:
        :return: finish_count, success_count
        """
        param_values={}
        sql = "select t1.* from task_instance t1 where id = " \
              "(select max(id) from task_instance where task_id=t1.task_id " \
              "and etl_day = t1.etl_day and task_type = t1.task_type) "
        if task_type == State.TASK_EXTRACT:
            sql = sql.replace("task_id=t1.task_id", "sub_task_id=t1.sub_task_id")

        sql += " and etl_day = :etl_day"
        param_values['etl_day'] = etl_day

        sql += " and task_type = :task_type "
        param_values['task_type'] = task_type
        finish_count = 0
        success_count = 0
        task_instance = session.execute(sql, param_values).fetchall()
        for instance in task_instance:
            if instance.task_id == State.FAKE_JOB:
                continue
            if instance.status in State.FINISHED_STATUS:
                finish_count += 1
            if instance.status == State.SUCCESS:
                success_count += 1
        return finish_count, success_count

    @provide_session
    def create_fake_task_instance(self, execute_date, session=None):
        """
        create_fake_task_instance to compat old system
        :param execute_date:
        :param session:
        :return:
        """
        exist = session.query(
            session.query(TaskInstance.id)
                .filter(TaskInstance.etl_day == execute_date,
                        TaskInstance.task_id == State.FAKE_JOB)
                .exists()
        ).scalar()
        now = datetime.now()
        if not exist:
            instance = TaskInstance(
                etl_day=execute_date,
                task_id=State.FAKE_JOB,
                sub_task_id="0",
                name=State.FAKE_JOB,
                task_type=State.TASK_JOB,
                module="bi",
                status=State.SUCCESS,
                scheduler_time=now,
                begin_time=now,
                end_time=now,
                heartbeat=now,
                hostname=socket.gethostname(),
                worker_retry=0,
                scheduler_retry=0,
            )
            session.add(instance)
            session.commit()

    @provide_session
    def create_extract_task_instance(self, execute_date, rule_list, session=None):
        """
        create_extract_task_instance for etl.extract_rule
        :param execute_date: YYYY-MM-DD
        :param rule_list: extract rule list
        :param session:
        :return: task instance list
        """
        scheduelr_time = datetime.now()
        task_ids = []
        instance_list = []
        for rule in rule_list:
            instance = TaskInstance(
                etl_day=execute_date,
                task_id=rule.dest_table,
                sub_task_id="{}".format(rule.rule_id),
                name="{}.{}".format(rule.src_db, rule.src_table),
                task_type=State.TASK_EXTRACT,
                module="bi",
                status=State.QUEUED,
                scheduler_time=scheduelr_time,
                scheduler_retry=0,
                worker_retry=0
            )
            task_ids.append(instance.task_id)
            instance_list.append(instance)

        session.add_all(instance_list)
        session.commit()
        if len(task_ids) == 0:
            return instance_list

        # refresh
        task_instance = session.query(TaskInstance).filter(TaskInstance.task_id.in_(task_ids)) \
            .filter(TaskInstance.etl_day == execute_date) \
            .filter(func.cast(TaskInstance.scheduler_time, DateTime) == func.cast(scheduelr_time, DateTime)) \
            .all()
        return task_instance


    @provide_session
    def direct_run_single_job_task(self, execute_date, job_list, session=None):
        """
        direct_run a job, no dep condition
        :param execute_date: YYYY-MM-DD
        :param task_id:
        :param session:
        :return:
        """
        scheduelr_time = datetime.now()
        task_ids = []
        instance_list = []
        for job in job_list:
            instance = TaskInstance(
                etl_day=execute_date,
                task_id=job.task_id,
                sub_task_id='0',
                name=job.task_name,
                task_type=State.TASK_JOB,
                module="bi",
                status=State.QUEUED,
                scheduler_time=scheduelr_time,
                scheduler_retry=0,
                worker_retry=0,
            )
            task_ids.append(instance.task_id)
            instance_list.append(instance)
        session.add_all(instance_list)
        session.commit()

        # refresh task_instance
        task_instance = session.query(TaskInstance).filter(TaskInstance.task_id.in_(task_ids))\
            .filter(func.cast(TaskInstance.scheduler_time, DateTime) == func.cast(scheduelr_time, DateTime)) \
            .filter(TaskInstance.etl_day == execute_date, TaskInstance.status == State.QUEUED).all()
        return task_instance

    @provide_session
    def create_job_task_instance(self, execute_date, job_list, session=None):
        """
        create_normal_task_instance for etl.schedule_job_define
        :param execute_date: YYYY-MM-DD
        :param task_id: task id
        :param session:
        """
        scheduelr_time = datetime.now()
        task_ids = []
        instance_list = []
        for job in job_list:
            instance = TaskInstance(
                etl_day=execute_date,
                task_id=job.task_id,
                sub_task_id='0',
                name=job.task_name,
                task_type=State.TASK_JOB,
                module="bi",
                status=State.WAITINGDEP,
                scheduler_time=scheduelr_time,
                scheduler_retry=0,
                worker_retry=0,
            )
            task_ids.append(instance.task_id)
            instance_list.append(instance)
        session.add_all(instance_list)
        session.commit()

    @provide_session
    def job_prepare_running(self, execute_date, task_ids, session=None):
        """
        job_prepare_running
        :param execute_date: YYYY-MM-DD
        :param task_id: task id
        :param session:
        :return: task instance
        """
        # refresh
        session.query(TaskInstance).filter(TaskInstance.task_id.in_(task_ids),
                                           TaskInstance.etl_day == execute_date,
                                           TaskInstance.status == State.WAITINGDEP)\
            .update({TaskInstance.status: State.QUEUED}, synchronize_session='fetch')
        session.commit()

        # refresh task_instance
        task_instance = session.query(TaskInstance).filter(TaskInstance.task_id.in_(task_ids)) \
            .filter(TaskInstance.etl_day == execute_date, TaskInstance.status == State.QUEUED).all()
        return task_instance

    @provide_session
    def create_cron_task_instance(self, execute_date, cron_list, session=None):
        """
        create_cron_task_instance for cron_conf(exclude extract task)
        :param execute_date: YYYY-MM-DD
        :param task_id: task id
        :param session:
        :return: task instance
        """
        scheduelr_time = datetime.now()
        task_ids = []
        instance_list = []
        for cron_conf in cron_list:
            instance = TaskInstance(
                etl_day=execute_date,
                task_id=cron_conf.task_id,
                name=cron_conf.name,
                task_type=State.TASK_CRON,
                module="bi",
                status=State.QUEUED,
                scheduler_time=scheduelr_time,
                scheduler_retry=0,
                worker_retry=0,
            )
            task_ids.append(instance.task_id)
            instance_list.append(instance)
        session.add_all(instance_list)
        session.commit()

        # refresh
        task_instance = session.query(TaskInstance).filter(TaskInstance.task_id.in_(task_ids)) \
            .filter(TaskInstance.etl_day == execute_date) \
            .filter(func.cast(TaskInstance.scheduler_time, DateTime) == func.cast(scheduelr_time, DateTime)) \
            .all()
        return task_instance

    @provide_session
    def heartbeat_instance(self, session=None):
        """
        heartbeat for task instance
        :param session:
        :return: affect rows, new instance
        """
        instance = None
        cnt = session.query(TaskInstance).filter(TaskInstance.id == self.id,
                                                 TaskInstance.status == State.RUNNING)\
            .update({TaskInstance.heartbeat: datetime.now()})
        session.commit()
        if cnt == 0:
            instance = session.query(TaskInstance).filter(TaskInstance.id == self.id).one()
        return cnt, instance

    @provide_session
    def update_status(self, status, session=None):
        """
        update_status
        :param status: task status
        :param session:
        :return:
        """
        cur_time = datetime.now()
        update_value = {}
        update_value[TaskInstance.status] = status
        update_value[TaskInstance.heartbeat] = cur_time
        if status == State.SUCCESS or status == State.FAILED:
            update_value[TaskInstance.end_time] = cur_time
        session.query(TaskInstance).filter(TaskInstance.id == self.id).update(update_value)
        session.commit()

    @provide_session
    def is_job_finished(self, etl_day, task_list, session=None):
        """
        is_job_finished
        :param etl_day: YYYY-MM-DD
        :param task_list: task id list
        :param session:
        :return: bool, not_finish_list
        """
        param_values = {}
        sql = "select t1.* from task_instance t1 where id = " \
              "(select max(id) from task_instance where task_id=t1.task_id and etl_day = t1.etl_day) "

        sql += " and etl_day = :etl_day"
        param_values['etl_day'] = etl_day

        sql += " and task_id in :ids "
        param_values['ids'] = tuple(task_list)
        
        task_instance = session.execute(sql, param_values).fetchall()
        instance_result = {x.task_id: x for x in task_instance}

        ok_status = [State.SUCCESS, State.SKIPPED]
        not_finish_list = []
        for task_id in task_list:
            if task_id in instance_result and \
                            instance_result[task_id].status in ok_status:
                pass
            else:
                not_finish_list.append(task_id)
        return len(not_finish_list) == 0, not_finish_list

    @provide_session
    def get_waiting_dep_instance(self, etl_day=None, session=None):
        qry = session.query(TaskInstance).filter(TaskInstance.status == State.WAITINGDEP)
        if etl_day is not None:
            qry = qry.filter(TaskInstance.etl_day == etl_day)
        return qry.all()

    @provide_session
    def refresh_instance(self, instance_id, session=None):
        """
        instance id
        :param instance_id:
        :param session:
        :return: err msg ,
        """
        instance = None
        err = None
        now = datetime.now()
        for loop in ["loop"]:
            _ = loop
            update_value = {}
            update_value[TaskInstance.hostname] = process_utils.node

            filters = []
            filters.append(TaskInstance.id == instance_id)
            filters.append(TaskInstance.status == State.QUEUED)
            filters.append(TaskInstance.heartbeat.is_(None))
            filters.append(TaskInstance.begin_time.is_(None))
            filters.append(TaskInstance.command.is_(None))
            filters.append(TaskInstance.hostname.is_(None))

            cnt = session.query(TaskInstance).filter(*filters).update(update_value, synchronize_session='fetch')
            session.commit()

            if cnt == 0:
                err = "instance {} is already run, should not run at this moment".format(instance_id)
                break
            else:
                instance = session.query(TaskInstance).filter(TaskInstance.id == instance_id).one()

            if instance.task_type == State.TASK_CRON:
                cron_conf = session.query(CronConf).filter(CronConf.task_id == instance.task_id,
                                               CronConf.enable == State.Enabled,
                                               ).one()
                instance.command = cron_conf.cron_command("{}".format(instance.etl_day))
            elif instance.task_type == State.TASK_JOB:
                job_define = session.query(TaskDefine).filter(TaskDefine.task_id == instance.task_id
                                                            ).one()
                instance.command = job_define.job_command(instance.etl_day)
                """
            elif instance.task_type == State.TASK_EXTRACT:
                cron_conf = session.query(CronConf).filter(CronConf.type == State.TASK_EXTRACT,
                                                           CronConf.enable == State.Enabled,
                                                           CronConf.task_id == State.ROOT_TASK,
                                                           ).one()
                instance.command = cron_conf.cron_command(" -r {} -d {}".format(instance.sub_task_id, instance.etl_day))
                """
            else:
                err =" not suppport task type {}".format(instance.task_type)
                break

        if err is not None:
            instance = None
        return err, instance

    @provide_session
    def start_running(self, retry=False, session=None):
        err = None
        now = datetime.now()
        update_value = {}
        update_value[TaskInstance.heartbeat] = now
        update_value[TaskInstance.begin_time] = now
        update_value[TaskInstance.command] = self.command
        update_value[TaskInstance.hostname] = self.hostname
        update_value[TaskInstance.status] = State.RUNNING
        update_value[TaskInstance.worker_retry] = self.worker_retry
        filters = []
        filters.extend([TaskInstance.id == self.id, TaskInstance.etl_day == self.etl_day])
        if retry:
            filters.append(TaskInstance.status.in_([State.FAILED, State.TIMEOUT]))
        else:
            filters.append(TaskInstance.status == State.QUEUED)
        cnt = session.query(TaskInstance)\
            .filter(*filters).update(update_value, synchronize_session='fetch')
        session.commit()
        if cnt == 0:
            err = "is_retry:{}, {}, the instance is running, can not run".format(retry, self)
        return err

    @provide_session
    def stop_running(self, status, session=None):
        err = None
        now = datetime.now()
        update_value = {}
        update_value[TaskInstance.end_time] = now
        update_value[TaskInstance.status] = status
        cnt = session.query(TaskInstance) \
            .filter(TaskInstance.id == self.id,
                    TaskInstance.etl_day == self.etl_day).update(update_value)
        session.commit()
        if cnt == 0:
            err = "task {} run at {} whit instancet {}, is running, can not rerun ".format(self.task_id, self.etl_day, self.id)
        return err

    @provide_session
    def create_retry_instance(self, timer_out=-1, max_schedule=0, session=None):
        """
        create_retry_instance, when need schedule, crecate a new instance,
        :param timer_out: worker execute timeout seconds, -1 means never timeout
        :param session:
        :return:
        """
        task_instance = []
        # we don't need retry schedule
        if timer_out < 0 or max_schedule <= 0:
            return task_instance

        # only create worker node heartbeat fialed instance, other issue should handle by worker self
        now = datetime.now()

        # onle re schedule time limit is 24 hours
        max_schedule_at = now - timedelta(hours=24)

        # heartbeat time
        need_heartbeat_at = now - timedelta(seconds=timer_out)
        instance_list = session.query(TaskInstance).filter(TaskInstance.status.in_([State.RUNNING, State.PREPAREED])) \
            .filter(func.cast(TaskInstance.heartbeat, DateTime) < func.cast(need_heartbeat_at, DateTime)) \
            .filter(func.cast(TaskInstance.scheduler_time, DateTime) > func.cast(max_schedule_at, DateTime)) \
            .all()

        task_ids = []
        for instance in instance_list:
            if (instance.scheduler_retry + 1) > max_schedule:
                log.logger.info("{} reach the max retry schedule {} times, stop schedule".format(instance, max_schedule))
                continue
            retry_instance = TaskInstance(
                etl_day=instance.etl_day,
                task_id=instance.task_id,
                sub_task_id=instance.sub_task_id,
                name=instance.name,
                task_type=instance.task_type,
                module=instance.module,
                status=State.QUEUED,
                scheduler_time=now,
                scheduler_retry=instance.scheduler_retry + 1,
                worker_retry=instance.worker_retry,
            )
            session.add(retry_instance)
            task_ids.append(retry_instance.task_id)
            session.query(TaskInstance).filter(TaskInstance.id == instance.id)\
                .update({TaskInstance.status: State.ABANDONNED})

        session.commit()

        # refresh
        if len(task_ids) > 0:
            task_instance = session.query(TaskInstance).filter(TaskInstance.task_id.in_(task_ids)) \
                .filter(func.cast(TaskInstance.scheduler_time, DateTime) == func.cast(now, DateTime)) \
                .all()
        return task_instance

    @provide_session
    def kill_instance(self, task_id, date_list, session=None):
        """
        kill_instance
        :param task_id: rule_id, or task_id
        :param data_list: date list
        :return: killed instacne count
        """

        # kill_instance
        update_value = {}
        update_value[TaskInstance.status] = State.KILLED
        filters = []
        filters.append(TaskInstance.etl_day.in_(date_list))
        filters.append(TaskInstance.task_id == task_id)
        status_list = [State.WAITINGDEP, State.QUEUED, State.RUNNING, State.FAILED, State.TIMEOUT]
        filters.append(TaskInstance.status.in_(status_list))
        cnt = session.query(TaskInstance) \
            .filter(*filters).update(update_value, synchronize_session='fetch')
        session.commit()
        return cnt

class TaskDependency(Base):
    """
    TaskDependency
    """
    __tablename__ = "task_dependency"
    id = Column(BigInteger, primary_key=True)
    task_id = Column(String(250))
    dependency_task_id = Column(String(250))

    @provide_session
    def is_met_dependency(self, task_id, execute_date, session=None):
        """
        is_met_dependency, fetch interval is task met dep， most of time
        is call downstream_met_dependency
        :param task_id:
        :param execute_date: execute_date YYYY-MM-DD
        :param session:
        :return: met result, detail info
        """
        met = False
        err = None
        for loop in ["loop"]:
            _ = loop
            up_job_list = self._upstream_list(task_id, session=session)
            if len(up_job_list) > 0:
                finished, unfinish = TaskInstance().is_job_finished(execute_date, up_job_list, session=session)
                if not finished:
                    if err is not None:
                        err = "{}, unfinished job task {}".format(err, unfinish)
                    else:
                        err = "unfinished job task {}".format(unfinish)
                    break
            met = True
        return met, err

    @provide_session
    def downstream_met_dependency(self, task_id, execute_date, session=None):
        """
        downstream_met_dependency called on recv kafka task result
        :param task_id:
        :param execute_date: execute_date YYYY-MM-DD
        :param session:
        :return:
        """
        met_list = []
        down_list = self._downstream_list(task_id, session=session)
        for task_id in down_list:
            met_dep, msg = self.is_met_dependency(task_id=task_id,
                                                 execute_date=execute_date,
                                                 session=session)
            if met_dep:
                met_list.append(task_id)
            else:
                log.logger.info("{} success, downstream not met:{}".format(task_id, msg))

        return met_list

    @provide_session
    def get_all_upstream(self, task_id, session=None):
        """
        get_all_upstream
        :param task_id:
        :param session:
        :return:
        """
        result = []
        job_list = self._upstream_list(task_id, session=session)
        job_list = filter(lambda x: x != State.FAKE_JOB, job_list)
        if len(job_list) > 0:
            result.extend(job_list)
            for job_id in job_list:
                tmp_result = self.get_all_upstream(job_id, session=session)
                if len(tmp_result) > 0:
                    result.extend(tmp_result)
        return result

    @provide_session
    def _upstream_list(self, task_id, session=None):
        """
        get task_id dependency list
        :param task_id: task_id
        :param session:
        :return: upstream_list job_task_list
        """
        qry = session.query(TaskDependency)\
            .filter(TaskDependency.task_id == task_id).all()
        job_task_list = [dep.dependency_task_id for dep in qry]
        return job_task_list


    @provide_session
    def get_all_downstream(self, task_id, session=None):
        """
        get_all_downstream
        :param task_id:
        :param session:
        :return:
        """
        result = []
        job_list = self._downstream_list(task_id, session=session)
        if len(job_list) > 0:
            result.extend(job_list)
            for job_id in job_list:
                tmp_result = self.get_all_downstream(job_id, session=session)
                if len(tmp_result) > 0:
                    result.extend(tmp_result)
        return result

    @provide_session
    def _downstream_list(self, task_id, session=None):
        """
        downstream_list get the task list who is depend on task_id
        :param task_id: task_id
        :param session:
        :return: downstream task list
        """
        qry = session.query(TaskDependency) \
            .filter(TaskDependency.dependency_task_id == task_id).all()
        return [dep.task_id for dep in qry]

    @provide_session
    def refresh(self, dep_list, session=None):
        """
        重置所有依赖
        :param session:
        :return:
        """
        pass

    @provide_session
    def migrateDep(self, job_id=None, delete=False, session=None):
        """
        迁移依赖关系
        :param session:
        :return:
        """

        sql = "select * from schedule_job_reference where status=1 "
        if job_id is not None:
            sql = "{} and job_id={}".format(sql, job_id)
        refrence_list = session.execute(sql).fetchall()
        ids_list = []
        job_to_task_dict = {}
        for refrence in refrence_list:
            if refrence.job_id not in ids_list:
                ids_list.append(refrence.job_id)
                job_to_task_dict[refrence.job_id] = "init"
            if refrence.reference_job_id not in ids_list:
                ids_list.append(refrence.reference_job_id)
                job_to_task_dict[refrence.reference_job_id] = "init"

        new_job_list = session.query(TaskDefine.job_id, TaskDefine.task_id).filter(TaskDefine.job_id.in_(ids_list)).all()
        for job in new_job_list:
            if job.job_id in job_to_task_dict:
                job_to_task_dict[job.job_id] = job.task_id
        dep_list = []
        for refrence in refrence_list:
            dep = TaskDependency(
                task_id=job_to_task_dict[refrence.job_id],
                dependency_task_id=job_to_task_dict[refrence.reference_job_id]
            )
            if dep.task_id == "init" or dep.dependency_task_id == "init":
                # this is garbage data, don't migrate
                continue
            dep_list.append(dep)
        if delete:
            session.query(TaskDependency).delete()
            session.commit()

        if len(dep_list) > 0:
            session.bulk_save_objects(dep_list)
        session.commit()

    @provide_session
    def fetchDependency(self, task_id=None, session=None):
        """
        fetchDependency
        :param task_id:
        :param session:
        :return: task dep list
        """
        query = session.query(TaskDependency)
        if task_id is not None:
            query = query.filter(TaskDependency.task_id == task_id)
        qry = query.all()
        dep={}
        for t in qry:
            dep.setdefault(t.task_id, set()).add(t.dependency_task_id)
        return dep

    @provide_session
    def getCircleDep(self, task_id=None, session=None):
        """
        is met circle dep
        :param task_id:
        :param session:
        :return: bool, circle_dep_list
        """
        circle_dep_path = {}
        query = session.query(TaskDependency)
        if task_id is not None:
            query = query.filter(TaskDependency.task_id == task_id)
        qry = query.all()
        dep={}
        for t in qry:
            dep.setdefault(t.task_id, set()).add(t.dependency_task_id)
        circled = {}
        for t in dep:
            dep_c = CircleCheck(t, dep, circled)
            if dep_c.is_circle():
                circled.setdefault(t, []).extend(dep_c.path)
                circle_dep_path[t] = (' -> '.join(dep_c.path))
        print "\n".join([ v for _, v in circle_dep_path.items()])


class CircleCheck(object):
    def __init__(self, id, d, circled):
        self.dep_mapping = d
        self.start = id
        self.path = []
        self.circled = circled

    def is_circle(self):
        return self._check(self.start)
    def _check(self, id):
        if id in self.circled:
            self.path.extend(self.circled[id])
            return True
        self.path.append(id)
        if id not in self.dep_mapping:
            return False
        for up_id in list(self.dep_mapping[id]):
            if up_id == self.start:
                self.path.append(self.start)
                return True
            else:
                return self._check(up_id)
        self.path.pop()
        return False



class CronLog(Base):
    """
    crontab 日志表
    """
    __tablename__ = "cron_log"

    id = Column(BigInteger, primary_key=True)
    task_id = Column(String(250))
    name = Column(String(512))
    module = Column(Integer)
    running_type = Column(String(32))
    status = Column(String(32))
    etl_day = Column(String(32))
    begin_time = Column(DateTime)
    end_time = Column(DateTime)
    command = Column(String(512))

    @provide_session
    def create_cron_log(self, cron_conf, etl_day, rt=State.NORMAL, session=None):
        """
        create_cron_log
        :param task: CronConf
        :param session:
        :return: cron_log
        """
        #cron_conf = CronConf().getCronTaskById("fakefadfaf_task2", session)
        cron_log = CronLog(
            task_id=cron_conf.task_id,
            name=cron_conf.name,
            module=cron_conf.module,
            running_type=rt,
            status=State.RUNNING,
            etl_day=etl_day,
            begin_time=datetime.now(),
        )
        session.add(cron_log)
        session.commit()
        return cron_log

    @provide_session
    def update_cron_status(self, status, session=None):
        """
        update_cron_status
        :param status: execute status
        :return: None
        """
        self.status = status
        if status == State.SUCCESS or status == State.FAILED:
            self.end_time = datetime.now()

        session.merge(self)
        session.commit()

class LoaderResult(Base):
    """
    LoaderResult
    """
    __tablename__ = "loader_result"

    id = Column(BigInteger, primary_key=True)
    hostname = Column(String(512))
    ip = Column(String(250))
    statements = Column(Text)
    dumped_file = Column(String(512))
    rule_id = Column(BigInteger)
    last_id = Column(BigInteger)
    etl_day = Column(String(250))
    create_time = Column(DateTime)

    @provide_session
    def get_dumped_file(self, rule_id, etl_day, session=None):
        qry = session.query(LoaderResult).filter(LoaderResult.rule_id == rule_id, LoaderResult.etl_day == etl_day)\
            .order_by(LoaderResult.id.desc())
        last = qry.first()
        return last


    @provide_session
    def get_dumped_file_by_id(self, file_id, session=None):
        qry = session.query(LoaderResult).filter(LoaderResult.id == file_id)
        file = qry.first()
        return file

    def gen_command(self, dest_file):
        command = "rsync {user}@{host}:{src_file} {dest_file}".format(user="etl",
                                                                      host=self.ip,
                                                                      src_file=self.dumped_file,
                                                                      dest_file=dest_file)
        return command


class StatResult(Base):
    """
    SchedulerResult
    """
    __tablename__ = "stat_result"

    id = Column(BigInteger, primary_key=True)
    etl_day = Column(String(32))
    extract_total_count = Column(Integer)
    extract_finish_count = Column(Integer)
    extract_success_count = Column(Integer)
    extract_status = Column(String(32))
    extract_notice = Column(SmallInteger)
    job_total_count = Column(BigInteger)
    job_finish_count = Column(Integer)
    job_success_count = Column(BigInteger)
    job_status = Column(String(32))
    job_notice = Column(SmallInteger)
    modify_time = Column(DateTime)

    @provide_session
    def init_stat(self, etl_day, session=None):
        """
        init_stat
        :param etl_day:
        :param session:
        :return:
        """
        extract_count = 0
        job_count = session.query(func.count(TaskDefine.task_id))\
            .filter(TaskDefine.enable == State.Enabled).scalar()

        stat = StatResult(
            etl_day=etl_day,
            extract_total_count=extract_count,
            extract_finish_count=0,
            extract_success_count=0,
            extract_status=State.RUNNING,
            extract_notice=0,
            job_total_count=job_count,
            job_finish_count=0,
            job_success_count=0,
            job_status=State.RUNNING,
            job_notice=0,
        )
        session.add(stat)
        session.commit()
        return stat

    @provide_session
    def update_stat(self, etl_day, session=None):
        """
        update_stat
        :param etl_day: YYYY-MM-DD
        :param session:
        :return (bool): is_all_finished
        """
        all_finished = True
        stat = None
        notice_msg = None
        try:
            stat = session.query(StatResult).filter(StatResult.etl_day == etl_day).one()
        except NoResultFound:
            self.init_stat(etl_day)
            stat = session.query(StatResult).filter(StatResult.etl_day == etl_day).one()

        # extract statuss
        if stat.extract_status == State.SUCCESS:
            if stat.extract_notice == 0:
                stat.extract_notice = 1
                notice_msg = " all rules extract success, etl_day: {}, total_count: {}"\
                    .format(stat.etl_day, stat.extract_success_count)
        else:
            all_finished = False
            stat.extract_finish_count, stat.extract_success_count = TaskInstance().\
                get_instance_result(etl_day, State.TASK_EXTRACT, session=session)
            if stat.extract_success_count == stat.extract_total_count:
                stat.extract_status = State.SUCCESS

        # job status
        if stat.job_status == State.SUCCESS:
            if stat.job_notice == 0:
                stat.job_notice = 1
                notice_msg = " all scheduler job execute success, etl_day:{}, total_count: {}".\
                    format(stat.etl_day, stat.job_total_count)
        else:
            all_finished = False
            stat.job_finish_count, stat.job_success_count = TaskInstance(). \
                get_instance_result(etl_day, State.TASK_JOB, session=session)
            if stat.job_total_count == stat.job_success_count:
                stat.job_status = State.SUCCESS
        session.merge(stat)
        session.commit()

        # create compat job
        if stat.extract_status == State.SUCCESS:
            TaskInstance().create_fake_task_instance(etl_day, session=session)
        if notice_msg is not None:
            process_utils.Alert(notice_msg)
        return all_finished

    @provide_session
    def incremental_update(self, instance_id, session=None):
        """
        incremental_update, when instance executing end upate the result
        :param instance_id: instance id
        :param session:
        :return:
        """
        instance = session.query(TaskInstance).filter(TaskInstance.id == instance_id).one()
        stat = session.query(StatResult).filter(StatResult.etl_day == instance.etl_day).one()
        if instance.task_type == State.TASK_EXTRACT and \
                        stat.extract_status != State.SUCCESS:
            if instance.status in State.FINISHED_STATUS:
                stat.extract_finish_count += 1

            if instance.status == State.SUCCESS:
                stat.extract_success_count += 1

            if stat.extract_success_count == stat.extract_total_count:
                stat.extract_status = State.SUCCESS

        if instance.task_type == State.TASK_JOB and \
                        stat.job_status != State.SUCCESS:
            if instance.status in State.FINISHED_STATUS:
                stat.job_finish_count += 1

            if instance.status == State.SUCCESS:
                stat.job_success_count += 1

            if stat.job_total_count == stat.job_success_count:
                stat.job_status = State.SUCCESS
        session.merge(stat)
        session.commit()

    @provide_session
    def get_result(self, etl_day, session=None):
        """
        get_result
        :param etl_day: YYYY-MM-DD
        :param session:
        :return: StatResult
        """
        stat = session.query(StatResult).filter(StatResult.etl_day == etl_day).one()
        return stat
