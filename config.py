# -*- coding: utf-8 -*-
import sys
import json
import etcd

G_Conf = None
class ServerConf(object):
    """
    根据json配置文件生成对应结构体
    """
    def __init__(self, data):
        for name, value in data.items():
            setattr(self, name, self._wrap(value))

    def _wrap(self, value):
        if isinstance(value, (tuple, list, set, frozenset)):
            return type(value)([self._wrap(v) for v in value])
        else:
            return ServerConf(value) if isinstance(value, dict) else value

def setupConf(localFile=None):
    json_cfg = None
    err = "get config err:"
    try:
        if localFile is not None:
            with open(localFile, 'r') as varfile:
                var = varfile.read()
                json_cfg = json.loads(var)
    except Exception as e:
        err = "{}, {}\n".format(err, e)

    if json_cfg is None:
        try:
            client = etcd.Client(host="etcd.in.codoon.com", port=2379)
            json_value = client.read("/config/cflow/online").value
            json_cfg = json.loads(json_value)
        except Exception as e:
            err = "{}, {}".format(err, e.message)

    if json_cfg is None:
        print err
        sys.exit(1)

    global G_Conf
    G_Conf = ServerConf(json_cfg)

if __name__ == '__main__':
    setupConf()
    print G_Conf.Common.MysqlConn
    print G_Conf.Worker.Parallelism


__version__ =  "0.0.1"
