# -*- coding: utf-8 -*-

from kafka import KafkaConsumer, KafkaProducer, errors
import json
import socket
import time
import datetime

import process_utils

TOPIC_DISPATCHER = "bi_scheduler_dispatch"
TOPIC_TASK_RESULT = "bi_task_result"
TOPIC_SYNC_FILE = "bi_sync_file"

producer = None
worker_consumer = None
scheduler_consumer = None
sync_file_consumer = None

def object2dict(obj):
    d = {}
    d['__class__'] = obj.__class__.__name__
    d['__module__'] = obj.__module__
    d.update(obj.__dict__)
    return d

def dict2object(d):
    if'__class__' in d:
        class_name = d.pop('__class__')
        module_name = d.pop('__module__')
        module = __import__(module_name)
        class_ = getattr(module,class_name)
        args = dict((key.encode('ascii'), value) for key, value in d.items())
        inst = class_(**args)
    else:
        inst = d
    return inst

class TaskBeginMsg(object):
    def __init__(self,instance_id, task_id, execute_date):
        """
        TaskBeginMsg
        :param instance_id:
        :param task_id:
        :param execute_date:
        """
        self.instance_id = instance_id
        self.task_id = task_id
        self.execute_date = execute_date
    def __repr__(self):
        return " task {} run  at {} with instacne id {} "\
            .format(self.task_id, self.execute_date, self.instance_id)


class TaskOverMsg(object):
    def __init__(self, instance_id, task_id, status, execute_date):
        """
        :param instance_id: 任务实例ID
        :param task_id: 任务ID
        """
        self.instance_id = instance_id
        self.task_id = task_id
        self.status = status
        self.execute_date = execute_date

    def __repr__(self):
        return " task {} run at {} with instance id {}, result {}"\
            .format(self.task_id, self.execute_date, self.instance_id, self.status)

class SyncFileMsg(object):
    def __init__(self, file_id):
        """
        SyncFileMsg
        :param file_id: file_id
        """
        self.file_id = file_id

    def __repr__(self):
        return " file_id:{}".format(self.file_id)


broker_url = "localhost:9092"
def setup_kafka(broker="localhost:9092", init_sync=False):
    global producer, worker_consumer, scheduler_consumer, sync_file_consumer
    broker_url = broker
    producer = KafkaProducer(bootstrap_servers=broker)
    worker_consumer = KafkaConsumer(TOPIC_DISPATCHER,
                                    bootstrap_servers=broker,
                                    auto_offset_reset="latest",
                                    group_id="cflow_woker",
                                    consumer_timeout_ms=10 * 60 * 1000,
                                    enable_auto_commit=True,
                                    session_timeout_ms=60 * 1000,
                                    max_poll_records=16,
                                    request_timeout_ms=70 * 1000
                                    )
    if init_sync:
        sync_file_consumer = KafkaConsumer(TOPIC_SYNC_FILE,
                                           bootstrap_servers=broker,
                                           auto_offset_reset="latest",
                                           group_id="sync_file_{}".format(socket.gethostname()),
                                           consumer_timeout_ms=30 * 60 * 1000,
                                           enable_auto_commit=True,
                                           session_timeout_ms=60 * 1000,
                                           max_poll_records=16,
                                           request_timeout_ms=70 * 1000
                                        )

    scheduler_consumer = KafkaConsumer(TOPIC_TASK_RESULT,
                                       bootstrap_servers=broker,
                                       auto_offset_reset="latest",
                                       group_id="cflow_scheduler",
                                       consumer_timeout_ms=1 * 60 * 1000,
                                       enable_auto_commit=True,
                                       session_timeout_ms=60 * 1000,
                                       max_poll_records=16,
                                       request_timeout_ms=70 * 1000
                                       )

def reset_producer():
    global producer
    try:
        producer = KafkaProducer(bootstrap_servers=broker_url)
    except Exception as e:
        print "{} {}".format(type(e), e.message)
        pass

wait_seconds = 5
def PushMsgWithRetry(topic, msg_obj, retry = 10):
    """
    if producer raise exception, retry push ,until success
    :param topic: kafka topic
    :param msg_obj: msg instance
    :param retry: retry times, default -1 unlimit
    :return:
    """
    if producer is None:
        setup_kafka()
    cnt = 0
    err = None
    while True:
        cnt = cnt + 1
        try:
            msg = json.dumps(msg_obj, default=lambda obj: obj.__dict__)
            future = producer.send(topic, msg)
            future.get(timeout=10)
        except Exception as e:
            #reset_producer()
            err = "{} msg {}".format(type(e), e.message)
        if err is None:
            break
        elif (retry < 0) or (retry >= 0 and cnt < retry):
            print "{}: push to kafka topic {}, msg {},retry: {}/{} waiting {} seconds, err {}, retry again"\
                .format(datetime.datetime.now(), topic, msg_obj, cnt, retry, wait_seconds * cnt, err)
            time.sleep(wait_seconds * cnt)
            continue
        else:
            print "{}: push to kafka topic {}, msg {} reach the max retry times {}, with err {}"\
                .format(datetime.datetime.now(), topic, msg_obj, retry, err)
    if err is not None:
        process_utils.Alert(err)
    return err
