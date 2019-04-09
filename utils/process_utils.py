# -*- coding: utf-8 -*-


import psutil
import getpass
import imp
import os
import signal
import subprocess, socket
import sys
import warnings
import threading
import requests
import traceback
import datetime

# When killing processes, time to wait after issuing a SIGTERM before issuing a
# SIGKILL.
TIME_TO_WAIT_AFTER_SIGTERM = 5


def kill_using_shell(pid, signal=signal.SIGTERM):
    process = psutil.Process(pid)
    # Use sudo only when necessary - consider SubDagOperator and SequentialExecutor case.
    if process.username() != getpass.getuser():
        # args = ["sudo", "kill", "-{}".format(int(signal)), str(pid)]
        args = ["kill", "-{}".format(int(signal)), str(pid)]
    else:
        args = ["kill", "-{}".format(int(signal)), str(pid)]
    # PID may not exist and return a non-zero error code
    subprocess.call(args)


def kill_process_tree(logger, pid):
    """
    Kills the process and all of the descendants. Kills using the `kill`
    shell command so that it can change users. Note: killing via PIDs
    has the potential to the wrong process if the process dies and the
    PID gets recycled in a narrow time window.

    :param logger: logger
    :type logger: logging.Logger
    """
    try:
        root_process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        logger.warn("PID: {} does not exist".format(pid))
        return

    # Check child processes to reduce cases where a child process died but
    # the PID got reused.
    descendant_processes = [x for x in root_process.children(recursive=True)
                            if x.is_running()]

    if len(descendant_processes) != 0:
        logger.warn("Terminating descendant processes of {} PID: {}"
                    .format(root_process.cmdline(),
                            root_process.pid))
        temp_processes = descendant_processes[:]
        for descendant in temp_processes:
            logger.warn("Terminating descendant process {} PID: {}"
                        .format(descendant.cmdline(), descendant.pid))
            try:
                kill_using_shell(descendant.pid, signal.SIGTERM)
            except psutil.NoSuchProcess:
                descendant_processes.remove(descendant)

        logger.warn("Waiting up to {}s for processes to exit..."
                    .format(TIME_TO_WAIT_AFTER_SIGTERM))
        try:
            psutil.wait_procs(descendant_processes, TIME_TO_WAIT_AFTER_SIGTERM)
            logger.warn("Done waiting")
        except psutil.TimeoutExpired:
            logger.warn("Ran out of time while waiting for "
                        "processes to exit")
        # Then SIGKILL
        descendant_processes = [x for x in root_process.children(recursive=True)
                                if x.is_running()]

        if len(descendant_processes) > 0:
            temp_processes = descendant_processes[:]
            for descendant in temp_processes:
                logger.warn("Killing descendant process {} PID: {}"
                            .format(descendant.cmdline(), descendant.pid))
                try:
                    kill_using_shell(descendant.pid, signal.SIGTERM)
                    descendant.wait()
                except psutil.NoSuchProcess:
                    descendant_processes.remove(descendant)
            logger.warn("Killed all descendant processes of {} PID: {}"
                        .format(root_process.cmdline(),
                                root_process.pid))
    else:
        logger.debug("There are no descendant processes to kill")


def kill_descendant_processes(logger, pids_to_kill=None):
    """
    Kills all descendant processes of this process.

    :param logger: logger
    :type logger: logging.Logger
    :param pids_to_kill: if specified, kill only these PIDs
    :type pids_to_kill: list[int]
    """
    # First try SIGTERM
    this_process = psutil.Process(os.getpid())

    # Only check child processes to ensure that we don't have a case
    # where a child process died but the PID got reused.
    descendant_processes = [x for x in this_process.children(recursive=True)
                            if x.is_running()]
    if pids_to_kill:
        descendant_processes = [x for x in descendant_processes
                                if x.pid in pids_to_kill]

    if len(descendant_processes) == 0:
        logger.debug("There are no descendant processes that can be killed")
        return
    logger.warn("Terminating descendant processes of {} PID: {}"
                .format(this_process.cmdline(),
                        this_process.pid))

    temp_processes = descendant_processes[:]
    for descendant in temp_processes:
        try:
            logger.warn("Terminating descendant process {} PID: {}"
                        .format(descendant.cmdline(), descendant.pid))
            descendant.terminate()
        except psutil.NoSuchProcess:
            descendant_processes.remove(descendant)

    logger.warn("Waiting up to {}s for processes to exit..."
                .format(TIME_TO_WAIT_AFTER_SIGTERM))
    try:
        psutil.wait_procs(descendant_processes, TIME_TO_WAIT_AFTER_SIGTERM)
        logger.warn("Done waiting")
    except psutil.TimeoutExpired:
        logger.warn("Ran out of time while waiting for "
                    "processes to exit")
    # Then SIGKILL
    descendant_processes = [x for x in this_process.children(recursive=True)
                            if x.is_running()]
    if pids_to_kill:
        descendant_processes = [x for x in descendant_processes
                                if x.pid in pids_to_kill]

    if len(descendant_processes) > 0:
        for descendant in descendant_processes:
            logger.warn("Killing descendant process {} PID: {}"
                        .format(descendant.cmdline(), descendant.pid))
            try:
                descendant.kill()
                descendant.wait()
            except psutil.NoSuchProcess:
                pass
        logger.warn("Killed all descendant processes of {} PID: {}"
                    .format(this_process.cmdline(),
                            this_process.pid))

# don't get the implement of compare and swap
class counter():
    def __init__(self):
        self.ref_count = 0
        self.lock = threading.Lock()

    def ref(self, v):
        ok = True
        self.lock.acquire()
        if (self.ref_count + 1) > v:
            ok = False
        else:
            self.ref_count += 1
        self.lock.release()
        return ok, self.ref_count

    def unref(self):
        self.lock.acquire()
        if self.ref_count > 0:
            self.ref_count -= 1
        self.lock.release()

    def getCount(self):
        n = 0
        self.lock.acquire()
        n = self.ref_count
        self.lock.release()
        return n

ref_counter = counter()

def getServerIp():
    """

    :return:ip list
    """
    ip_str = ""
    ret = 0
    try:
        ip_str = subprocess.check_output(['hostname', '--all-ip-addresses']).replace(' \n','')
    except subprocess.CalledProcessError as e:
        ret = e.returncode
    if ret != 0:
        def get_host_ip():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(('8.8.8.8', 80))
                ip = s.getsockname()[0]
            finally:
                s.close()
            return ip
        ip_str = get_host_ip()
    return_ip = ip_str
    ip_list = ip_str.split(" ")
    for ip in ip_list:
        if ip.startswith("10."):
            return_ip = ip
            break
    return return_ip


#g_alarm_user_list = ["17052", "16116", "15011", "16019", "16013", "16172", "16101", "16151"]
g_alarm_user_list = ["16101"]
g_alarm_url = ""
g_alarm_token = ""

node = "{}-{}".format(socket.gethostname(), getServerIp())
def Alert(msg):
    import json
    stack_info = traceback.format_exc()
    msg = "{}\n{}".format(msg, stack_info)
    try:
        response = requests.post(
            url=g_alarm_url,
            params={
                "accessToken": g_alarm_token,
            },
            headers={
                "Content-Type": "application/json; charset=utf-8",
            },
            data=json.dumps({
                "users": g_alarm_user_list,
                "body": ("[{}][cflow-{}]: ".format(datetime.datetime.now(), node) + msg)
            })
        )
    except Exception as e:
        print "发送告警信息失败 execption:{}, err:{} ".format(type(e), e.message)
