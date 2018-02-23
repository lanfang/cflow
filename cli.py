# -*- coding: utf-8 -*-

import sys
import os, datetime
import subprocess

import daemon
from daemon.pidfile import TimeoutPIDLockFile
import signal
import threading
import traceback
import argparse

import time
import psutil
from utils import log

from collections import namedtuple
import config
import scheduler as module_scheduler
import worker
import utils.process_utils as process_utils

from utils import db, log
import ha
def initServer(logfilie, mysql_conn):
    db.configure_orm(mysql_conn)
    fd = log.setup_logging(logfilie)
    #kafka_utils.setup_kafka(config.G_Conf.Common.Broker)
    return fd

def sigint_handler(sig, frame):
    print "{} signal {}, frame {}".format(datetime.datetime.now(), sig, frame)
    process_utils.kill_process_tree(log.logger, None)
    sys.exit(0)

def sigquit_handler(sig, frame):
    """Helps debug deadlocks by printing stacktraces when this gets a SIGQUIT
    e.g. kill -s QUIT <PID> or CTRL+\
    """
    print("Dumping stack traces for all threads in PID {}".format(os.getpid()))
    id_to_name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for thread_id, stack in sys._current_frames().items():
        code.append("\n# Thread: {}({})"
                    .format(id_to_name.get(thread_id, ""), thread_id))
        for filename, line_number, name, line in traceback.extract_stack(stack):
            code.append('File: "{}", line {}, in {}'
                        .format(filename, line_number, name))
            if line:
                code.append("  {}".format(line.strip()))
    print("\n".join(code))


def setup_locations(dir="./", file="default_file"):
    if not os.path.exists(dir):
        os.mkdir(dir)
    pid = "{}/{}.pid".format(dir, file)
    stdout = "{}/{}.stdout".format(dir, file)
    stderr = "{}/{}.stderr".format(dir, file)
    log = "{}/{}.log".format(dir, file)
    return pid, stdout, stderr, log

def scheduler(args):
    config.setupConf(args.cfg_file)
    logfile = "{}/{}".format(config.G_Conf.Scheduler.LogDir, config.G_Conf.Scheduler.LogFile)
    if args.ha:
        logfile = "{}.ha".format(logfile)

    initServer(logfile, config.G_Conf.Common.MysqlConn)
    main_scheduler = module_scheduler.MainScheduler(
        broker_url=config.G_Conf.Common.Broker,
        fetch_interval=config.G_Conf.Scheduler.FetchInterval,
        woker_timeout=config.G_Conf.Scheduler.WorkerTimeOut,
        retry_push_times=config.G_Conf.Scheduler.RetryQueueimes)

    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    signal.signal(signal.SIGQUIT, sigquit_handler)
    if args.ha:
        #haserver = ha.HaServer(host="etcd.in.codoon.com", port=2379)
        """
        start a main process for health checking, sub process for scheduler
        """
        haserver = ha.HaServer()
        haserver.run()
    else:
        main_scheduler.run()

def executer(args):
    config.setupConf(args.cfg_file)
    initServer("{}/{}".format(config.G_Conf.Worker.LogDir, config.G_Conf.Worker.LogFile), config.G_Conf.Common.MysqlConn)
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    signal.signal(signal.SIGQUIT, sigquit_handler)
    task_worker = worker.Worker(
        parallelism=config.G_Conf.Worker.Parallelism,
        heartbeat_interval=config.G_Conf.Worker.HearteatInterval,
        woker_timeout=config.G_Conf.Worker.TaskTimeOut,
        retry_times=config.G_Conf.Worker.Retry)
    task_worker.run()

def version(args):
    print("v" + config.__version__)


def dep(args):
    """
    auto generate script dependency
    """
    config.setupConf(args.cfg_file)
    initServer("{}/{}".format(config.G_Conf.Scheduler.LogDir, "dep.log"), config.G_Conf.Common.MysqlConn)

    if args.query:
        module_scheduler.Admin().query_dep(args.job_id, args.date)
    else:
        if os.path.isfile(args.script):
            gen_dep(args.script)
        elif os.path.isdir(args.script):
            for root, dirs, files in os.walk(args.script, followlinks=True):
                for f in files:
                    filepath = os.path.join(root, f)
                    if not os.path.isfile(filepath):
                        continue
                    mod_name, file_ext = os.path.splitext(
                        os.path.split(filepath)[-1])
                    if file_ext != '.py':
                        continue
                    gen_dep(filepath)

def gen_dep(script_file):
    """
    generate dependency from script file
    :param file:
    :return:
    """
    sql_file = "/tmp/script_dep.sql"
    file = open(script_file)
    script = file.read()
    sep = "'''"
    step = len(sep)
    sql_list = []
    start = 0
    while True:
        start = script.find(sep, start)
        if start < 0:
            break
        start += step
        end = script.find(sep, start)
        if end < 0:
            break
        sql_list.append(script[start:end])
        start = end + step
    result = []
    if len(sql_list) > 0:
        ret_sql = ";".join(sql_list)
        output = open(sql_file, 'w')
        output.write(ret_sql)
        output.close()
        cmd = "/usr/bin/java -jar /Users/codoon/bi_work/demo/sqlparser.jar -sqlfile {}".format(sql_file)
        for table in subprocess.check_output(cmd, shell=True).split("\n"):
            if len(table) > 0:
                tmp = table.split(":")
                if len(tmp) == 2 and tmp[1] == "select":
                    result.append(tmp[0])
    print "{} -> {}".format(script_file, ", ".join(result))


def migrate(args):
    """
    migrate dependency , extract last_id
    :param args:
    :return:
    """
    config.setupConf(args.cfg_file)
    initServer("{}/{}".format(config.G_Conf.Scheduler.LogDir, "migrate.log"), config.G_Conf.Common.MysqlConn)
    if args.dependency:
        module_scheduler.Admin().migrate_dependency(None, args.delete)

    if args.last_id:
        if not isValiddate(args.date):
            print "date format is YYYY-MM-DD, give {}".format(args.date)
            return
        d = args.date.replace("-", "")
        module_scheduler.Admin().migrate_lastid(d)

def isValiddate(currentDay=None, format='%Y-%m-%d'):
    try:
        time.strptime(currentDay, format)
        return True
    except:
        return False

def is_valid_run_args(args):
    """
    checkt args valid and return date need to run
    :param args:
    :return:
    """
    date_list = []
    err = None
    for loop in ["loop"]:
        _ = loop
        if args.job_id is None or len(args.job_id) == 0:
            err = "the job_id is empty"
            break
        if args.date:
            if not isValiddate(args.date):
                err = "time date format is YYYY-MM-DD, give {}".format(args.date)
                break
            date_list.append(args.date)
        elif args.start_date and args.end_date:
            if not isValiddate(args.start_date):
                err = "time start_date format is YYYY-MM-DD, give {}".format(args.start_date)
                break

            if not isValiddate(args.end_date):
                err = "time end_date format is YYYY-MM-DD, give {}".format(args.end_date)
                break
            s_f = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
            e_f = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
            distance = e_f - s_f
            if distance.days <= 0:
                err = "the start_date {} is after end_date {}, " \
                      "if range time, the start_date must before end_date".format(args.start_date, args.end_date)
            else:
                for d in range(distance.days + 1):
                    date_list.append((s_f + datetime.timedelta(days=d)).strftime('%Y-%m-%d'))
        else:
            err = "must give a single date or range date time with format YYYY-MM-DD \n" \
                  "if range time, the start_date must before end_date"
            break
    return err, date_list

def do_all_job(args):
    """
    do_all_job run all scheduler job
    :param args:
    :return:
    """
    if not isValiddate(args.date):
        err = "time date format is YYYY-MM-DD, give {}".format(args.date)
        print err
        return

    print "run all schedule_job with date: {}".format(args.date)
    config.setupConf(args.cfg_file)
    initServer("{}/{}".format(config.G_Conf.Scheduler.LogDir, "do_all_job.log"), config.G_Conf.Common.MysqlConn)
    msg = module_scheduler.Admin().run_all_job([args.date])
    print "run success, {}".format(msg)

def do_all_extract(args):
    """
    do_all_extract
    :param args:
    :return:
    """
    if not isValiddate(args.date):
        err = "time date format is YYYY-MM-DD, give {}".format(args.date)
        print err
        return
    msg = "run all extract with date: {}".format(args.date)
    if args.dest_table is not None:
        msg = "{}, dest_table: {}".format(msg, args.dest_table)
    print msg

    config.setupConf(args.cfg_file)
    initServer("{}/{}".format(config.G_Conf.Scheduler.LogDir, "do_all_extract.log"), config.G_Conf.Common.MysqlConn)
    msg = module_scheduler.Admin().run_all_extract([args.date], dest_table=args.dest_table)
    print "run success, {}".format(msg)

def run(args):
    """
    rerun task
    :param args:
    :return:
    """
    err, date_list = is_valid_run_args(args)
    if err:
        print err
        return
    print "run task  {} with date: {}".format(args.job_id, " ".join(date_list))
    config.setupConf(args.cfg_file)
    initServer("{}/{}".format(config.G_Conf.Scheduler.LogDir, "rerun.log"), config.G_Conf.Common.MysqlConn)
    msg = module_scheduler.Admin().rerun_task(args.job_id, date_list,
                                                 up_and_down=args.with_up_down,
                                                 run_up=args.with_upstream,
                                                 run_down=args.with_downstream,
                                              force=args.force)
    print "run success, {}".format(msg)

def kill(args):
    err, date_list = is_valid_run_args(args)
    if err:
        print err
        return
    print "kill task {} with date: {}".format(args.job_id, " ".join(date_list))
    config.setupConf(args.cfg_file)
    initServer("{}/{}".format(config.G_Conf.Scheduler.LogDir, "kill.log"), config.G_Conf.Common.MysqlConn)
    cnt = module_scheduler.Admin().kill_task(args.job_id, date_list)
    print "kill {} instance".format(cnt)


Arg = namedtuple(
    'Arg', ['flags', 'help', 'action', 'default', 'nargs', 'type', 'choices', 'metavar'])
Arg.__new__.__defaults__ = (None, None, None, None, None, None, None)


class CLIFactory(object):
    args = {
        'force': Arg(
            ('-f', '--force'),
            'Ignore previous task instance state, rerun regardless if task already '
            'succeeded/failed',
            'store_true'),

        'etcd': Arg(
            ('-e', '--etcd'),
            type=str,
            help='get json config from etcd, default http://etcd.in.codoon.com:2379, etcd config Have a higher priority',
            default='http://etcd.in.codoon.com:2379'),

        'cfg_file': Arg(
            ('-c', '--cfg_file'),
            type=str,
            help='server config is json format, cfg_file is a optional arg '
                 'parse config strategy:'
                 'a、parse config from cfg_file arg '
                 'b、if cfg_file arg is none, parse default file: /codoon/config/cflow/cflow_conf.json '
                 'c、if default file is not exist, parse from etcd.in.codoon.com:2379, key: /config/cflow/online '
                 'else server quit',
            default='/codoon/config/cflow/cflow_conf.json'),

        'job_id': Arg(
            ('-j','--job_id',),
            help='job_id is extract_rule.rule_id or schedule_job_define.task_id',
            type=str),
        'ha': Arg(
            ('-s','--ha'),
            'start a health check process, auto switch to master/slave based on etcd election',
            'store_true'),
        'with_upstream': Arg(
            ('-up','--with_upstream'),
            'run a task and its upstream task(exclude extract task)',
            'store_true'),
        'with_downstream': Arg(
            ('-down','--with_downstream'),
            'run a task and its downstream task',
            'store_true'),
        'with_up_down': Arg(
            ('-up_down','--with_up_down'),
            'run a task and its downstream task',
            'store_true'),
        'date': Arg(
            ('-d', '--date'),
            type=str,
            help=' single date with format YYYY-MM-DD'
        ),
        'start_date': Arg(
            ('-st', '--start_date'),
            type=str,
            help='run task with begin time: start_date, format YYYY-MM-DD'
        ),
        'end_date': Arg(
            ('-et', '--end_date'),
            type=str,
            help='run task with end time: end_date, format YYYY-MM-DD'
        ),

        'dependency': Arg(
            ('-dep','--dependency'),
            'migrate schedule_job_reference to task_dependency, '
            'if need delete task_dependency, with args --delete',
            'store_true'
        ),
        'delete': Arg(
            ('-del','--delete'),
            'delete task_dependency data, when migrate ',
            'store_true'
        ),
        'check': Arg(
            ('-ck','--check'),
            'check is exist circled dependency',
            'store_true'
        ),
        'last_id': Arg(
            ('-lsi','--last_id'),
            'migrate extract_log.last_id to extract_rule.last_id',
            'store_true'
        ),
        'script': Arg(
            ('-scr','--script'),
            help=' gen the script dependency',
            type=str,
        ),
        'save': Arg(
            ('-save','--save'),
            'write the dependency to mysql',
            'store_true'
        ),
        'query': Arg(
            ('-q', '--query'),
            'query task dependency, need args: --job_id, --date',
            'store_true'
        ),
        'dest_table': Arg(
            ('-tb', '--dest_table'),
            help='extract all the all rules where extract_rule.dest_table=args.dest_table',
            type=str,
        ),

    }

    subparsers = (
        {
            'func': scheduler,
            'help': 'start a scheduler instance',
            'args': ('cfg_file', 'ha')
        }, {
            'func': executer,
            'help': 'start a job worker',
            'args': ('cfg_file',)
        }, {
            'func': version,
            'help': 'show the version',
            'args': ('cfg_file',)
        },{
            'func': dep,
            'help': 'generate script dependency, this command is not recommended',
            'args': ('cfg_file', 'script', 'save', 'query', 'job_id', 'date')
        },{
            'func': run,
            'help': 'rerun task',
            'args': ('cfg_file', 'job_id','date', 'start_date', 'end_date', 'force','with_upstream', 'with_downstream','with_up_down'),
        },{
            'func': migrate,
            'help': 'migrate old scheduler data to cflow',
            'args': ('cfg_file', 'dependency', 'delete', 'last_id', 'date')
        },
        {
            'func': do_all_job,
            'help': 'running all scheduler job with date: args.date',
            'args': ('cfg_file', 'date')
        },{
            'func': kill,
            'help': 'kill task instance(waiting_dep, queued, running)',
            'args': ('cfg_file', 'job_id','date', 'start_date', 'end_date'),
        },
    )
    subparsers_dict = {sp['func'].__name__: sp for sp in subparsers}

    @classmethod
    def get_parser(cls):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(
            help='sub-command help', dest='subcommand')
        subparsers.required = True

        subparser_list = cls.subparsers_dict.keys()
        for sub in subparser_list:
            sub = cls.subparsers_dict[sub]
            sp = subparsers.add_parser(sub['func'].__name__, help=sub['help'])
            for arg in sub['args']:
                arg = cls.args[arg]
                kwargs = {
                    f: getattr(arg, f)
                    for f in arg._fields if f != 'flags' and getattr(arg, f)}
                #print kwargs
                sp.add_argument(*arg.flags, **kwargs)
            sp.set_defaults(func=sub['func'])
        return parser


def get_parser():
    return CLIFactory.get_parser()