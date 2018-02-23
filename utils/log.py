# -*- coding: utf-8 -*-

import logging
from logging.handlers import TimedRotatingFileHandler
LOG_FORMAT = (
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
LOG_FORMAT_WITH_PID = (
    '[%(asctime)s][pid:%(process)d,tid:%(thread)d]{%(filename)s:%(lineno)d(%(funcName)s)} %(levelname)s - %(message)s')
LOG_FORMAT_WITH_THREAD_NAME = (
    '[%(asctime)s] {%(filename)s:%(lineno)d} %(threadName)s %(levelname)s - %(message)s')
SIMPLE_LOG_FORMAT = '%(asctime)s %(levelname)s - %(message)s'
LOGGING_LEVEL = logging.INFO
SQL_CONN_LOGGING_LEVEL = logging.INFO
#logger default value
logger = logging.getLogger()

def setup_logging(filename):
    global logger
    formatter = logging.Formatter(LOG_FORMAT_WITH_PID)
    logger = logging.getLogger("scheduler")
    handler = TimedRotatingFileHandler(filename, "MIDNIGHT", backupCount=7)
    handler.suffix = "%Y%m%d"
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(LOGGING_LEVEL)

    db_handler = TimedRotatingFileHandler("{}.{}".format(filename, "mysql"), "MIDNIGHT", backupCount=7)
    db_handler.suffix = "%Y%m%d"
    db_handler.setLevel(SQL_CONN_LOGGING_LEVEL)
    db_handler.setFormatter(formatter)
    #logging.basicConfig()
    sql_logger = logging.getLogger('sqlalchemy')
    #sql_logger.propagate = False
    sql_logger.addHandler(db_handler)
    sql_logger.setLevel(SQL_CONN_LOGGING_LEVEL)
    return [handler.stream, db_handler.stream]

"""
def info(msg, *args, **kwargs):
    if logger is not None:
        logger.info(msg, *args, **kwargs)

def debug(msg, *args, **kwargs):
    if logger is not None:
        logger.debug(msg, *args, **kwargs)

def warning(msg, *args, **kwargs):
    if logger is not None:
        logger.warning(msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    if logger is not None:
        logger.error(msg, *args, **kwargs)

def exception(msg, *args, **kwargs):
    if logger is not None:
        logger.exception(msg, *args, **kwargs)
def critical(msg, *args, **kwargs):
    if logger is not None:
        logger.critical(msg, *args, **kwargs)
"""