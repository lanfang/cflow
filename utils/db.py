# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

import logging

engine = None
Session = None

def configure_orm(mysql_conn, disable_connection_pool=False):
    global engine
    global Session
    engine_args = {}
    if disable_connection_pool:
        engine_args['poolclass'] = NullPool
    else:
        engine_args['pool_size'] = 32
        engine_args['max_overflow'] = 64
        engine_args['pool_recycle'] = 600

    engine = create_engine(mysql_conn, echo=False, **engine_args)
    Session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine))


def provide_session(func):
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        needs_session = False
        arg_session = 'session'
        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and \
            func_params.index(arg_session) < len(args)
        if not (arg_session in kwargs or session_in_args):
            needs_session = True
            session = Session
            kwargs[arg_session] = session
        result = func(*args, **kwargs)
        if needs_session:
            session.expunge_all()
            session.commit()
            session.close()
        return result
    return wrapper



