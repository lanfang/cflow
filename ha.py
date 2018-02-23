# -*- coding: utf-8 -*-

import etcd
import uuid
import re
import time
import sys,os
import subprocess, psutil


from utils import log
import utils.process_utils as process_utils

class EtcdMutex(object):
    def __init__(self, client, key, val, ttl):
        self.key = key
        self.val = val
        self.ttl = ttl
        self.client = client

    def Lock(self):
        err = None
        try:
            ret = self.client.write(self.key, self.val, prevExist=True, prevValue=self.val, ttl=self.ttl)
            #log.logger.info("lock result {}".format(ret))
        except Exception as e:
            err = e.message
        return err

    def TryLock(self):
        err = None
        try:
            ret = self.client.write(self.key, self.val, prevExist=False, ttl=self.ttl)
            #log.logger.info("try lock result {}".format(ret))
        except Exception as e:
            err = e.message
        return err

    def UnLock(self):
        err = None
        try:
            ret = self.client.delete(self.key, prevValue=self.val)
            #log.logger.info("unlock result {}".format(ret))
        except Exception as e:
            err = e.message
        return err


MASTER = "master"
SLAVE = "slave"
TTL = 20
slaveCheckReg = re.compile(r'Key already exists')

def getKey(keyname="bi_scheduler", group_name="bi"):
    return "/lock/{}/{}".format(keyname, group_name)

def getVal():
    return uuid.uuid4().hex

class HaServer(object):
    """
    ha server based on etcd election
    """
    def __init__(self, host="etcd.in.codoon.com", port=2379):
        self.status = SLAVE
        self.client = etcd.Client(host=host, port=port)
        self.mutex = EtcdMutex(self.client,  getKey(), getVal(), TTL)
        self.master_lock_failed = 0
        self.process = None
        self.on_slave_wait = 10
    def run(self):
        interval = TTL/3
        if interval < 3:
            interval = 3
        while True:
            if self.status == MASTER:
                err = self.mutex.Lock()
                if err is not None:
                    err_msg = "server as {} checking failed, {}".format(self.status, err)
                    log.logger.error(err_msg)
                    self.master_lock_failed = self.master_lock_failed + 1
                    if self.master_lock_failed == 3:
                        self.mutex.UnLock()
                        self.onSlave(err_msg)
                        continue
                else:
                    log.logger.info("server as {} checking success".format(self.status))

                if self.process:
                    return_code = self.process.poll()
                    if return_code is not None:
                        err_msg = "subprocess exited with return with code: {}, switch to slave".format(return_code)
                        log.logger.info(err_msg)
                        self.onSlave(err_msg)
            else:
                err = self.mutex.TryLock()

                if err is None:
                    self.onMaster("no master server")
                else:
                    if slaveCheckReg.match(err):
                        log.logger.info("server as {} checking success".format(self.status))
                    else:
                        log.logger.error("server as {} checking failed, {}".format(self.status, err))

            time.sleep(interval)

    def onMaster(self, err_msg):
        self.terminate()
        self.master_lock_failed = 0
        self.status = MASTER
        cmd = ["python",]
        for v in sys.argv:
            if v != "--ha" and v != "-s":
                cmd.append(v)
        full_cmd = " ".join(cmd)
        msg = "{}, switch to master, Running Command [{}]".format(err_msg, full_cmd)
        process_utils.Alert(msg)
        log.logger.info(msg)
        self.process = subprocess.Popen(
            full_cmd,
            shell=True
        )

    def onSlave(self, err_msg):
        self.master_lock_failed = 0
        self.status = SLAVE
        msg = "{}, switch to slave, waiting {} seconds try master again".format(err_msg, self.on_slave_wait)
        log.logger.info(msg)
        process_utils.Alert(msg)
        self.terminate()
        time.sleep(self.on_slave_wait)

    def terminate(self):
        if self.process:
            process_utils.kill_process_tree(log.logger, None)#os.getpid())


if __name__ == "__main__":
    log.setup_logging("test_ha.log")
    server = HaServer(host='127.0.0.1')
    server.run()



