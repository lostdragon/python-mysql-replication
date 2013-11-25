# -*- coding: utf-8 -*-

__author__ = 'lost'

import ConfigParser
import pymysql
import os
import traceback
import re
import helper
import time

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    RowsEvent,
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
    TableMapEvent)


class Observer(object):
    def __init__(self):
        config = ConfigParser.ConfigParser()
        # Using same name config file
        config.read(__file__.replace('.py', '.conf'))
        # Using same dir logs
        self.log_path = os.path.realpath(os.path.dirname(__file__)) + '/'
        self.log_name = os.path.basename(__file__).split('.')[0]

        self.source_db = config.get('source_mysql', 'db')
        self.source_mysql_settings = {
            'host': config.get('source_mysql', 'host'),
            'port': config.getint('source_mysql', 'port'),
            'user': config.get('source_mysql', 'user'),
            'passwd': config.get('source_mysql', 'passwd'),
        }

        self.to_db = config.get('to_mysql', 'db')
        self.to_mysql_settings = {
            'host': config.get('to_mysql', 'host'),
            'port': config.getint('to_mysql', 'port'),
            'user': config.get('to_mysql', 'user'),
            'passwd': config.get('to_mysql', 'passwd'),
            'db': self.to_db,
        }
        self.db = helper.Database(self.to_mysql_settings)
        self.last_sql = ''
        self.primary_keys = dict()
        # log_bin progress file
        self.log_bin_file = self.log_path + self.log_name + '_bin.conf'

    def watch(self):
        # Run always
        while True:
            # Read log_file and log_pos
            if not os.path.isfile(self.log_bin_file):
                log_file = log_pos = None
                resume_stream = False
            else:
                config = ConfigParser.RawConfigParser()
                config.read(self.log_bin_file)
                log_file = config.get('log-bin', 'log_file')
                log_pos = config.getint('log-bin', 'log_pos')
                resume_stream = True
            try:
                stream = BinLogStreamReader(connection_settings=self.source_mysql_settings, blocking=True,
                                            only_events=[TableMapEvent, QueryEvent, DeleteRowsEvent,
                                                         WriteRowsEvent, UpdateRowsEvent],
                                            replicate_do_dbs=[self.source_db], log_file=log_file,
                                            log_pos=log_pos, resume_stream=resume_stream)
                # Process Feed
                for binlogevent in stream:
                    self.feed(binlogevent)
                    # Write log_file and log_pos
                    config = ConfigParser.RawConfigParser()
                    config.add_section('log-bin')
                    config.set('log-bin', 'log_file', stream.log_file)
                    config.set('log-bin', 'log_pos', stream.log_pos)
                    with open(self.log_bin_file, 'wb') as configfile:
                        config.write(configfile)

                stream.close()
            except Exception as e:
                self.error_log(str(e) + "\n" + traceback.format_exc())
                # Sleep 3 sec
                time.sleep(3)

    def feed(self, binlogevent):
        data = list()
        try:
            # only watch source_db
            if binlogevent.schema == self.source_db:
                # get primary keys
                if isinstance(binlogevent, TableMapEvent):
                    if binlogevent.table not in self.primary_keys:
                        sql = "SHOW KEYS FROM %s WHERE Key_name = 'PRIMARY'" % binlogevent.table
                        cursor = self.query(sql, return_cursor=True)
                        res = cursor.fetchall()
                        cols = list()
                        for col in res:
                            cols.append(col['Column_name'])
                        self.primary_keys[binlogevent.table] = cols

                # check change table
                elif isinstance(binlogevent, QueryEvent):
                    query = binlogevent.query
                    # replace new line to space
                    tmp = self.multiple_replace({'\r': ' ', '\n': '', '\r\n': ' '}, query.strip()).split(' ')
                    # query type
                    query_type = tmp[0].lower()
                    if query_type in ['alter', 'create', 'drop', 'truncate', 'rename']:
                        table = ''
                        matches = re.search("`" + self.source_db + "`\.`(.*)`", query)
                        if matches:
                            table = matches.group(1)
                            query = query.replace(matches.group(0), '`' + matches.group(1) + '`')
                        data = ['query', table, query]
                # check rows event
                elif isinstance(binlogevent, RowsEvent):
                    if isinstance(binlogevent, WriteRowsEvent):
                        data = ['insert', binlogevent.table, binlogevent.rows[0]['values']]
                    elif isinstance(binlogevent, UpdateRowsEvent):
                        data = ['update', binlogevent.table, binlogevent.rows[0]['after_values']]
                    elif isinstance(binlogevent, DeleteRowsEvent):
                        data = ['delete', binlogevent.table, binlogevent.rows[0]['values']]

                if data:
                    rowcount = self.process(data)
                    if rowcount:
                        self.msg_log('result = ' + str(rowcount) + "\n" + self.last_sql)
                    else:
                        self.msg_log('result = ' + str(rowcount) + "\n" + self.last_sql)
        except pymysql.Error as error:
            # 1062: Duplicate entry for PRIMARY
            # 1050: Table already exists
            # 1060: Duplicate column name
            # 1054: Unknown column
            # 1061: Duplicate key name
            #code, message = error.args
            #if code not in [1050, 1054, 1060, 1061, 1062]:
            # mysql error
            self.error_log(str(error) + "\n" + str(data) + "\n" + self.last_sql)
        except Exception as error:
            # other error
            self.error_log(str(error) + "\n" + str(data) + "\n" + traceback.format_exc())

    def process(self, data):
        query_type, table, params = data
        self.last_sql = ''
        sql, args = self.default_process(query_type=query_type, table=table, params=params)
        return self.query(sql=sql, args=args, query_type=query_type)

    def query(self, sql, args=None, query_type=None, return_cursor=False):
        if sql:
            # build sql
            if args is not None:
                if isinstance(args, (tuple, list)):
                    escaped_args = tuple(self.db.connection().escape(arg) for arg in args)
                elif isinstance(args, dict):
                    escaped_args = dict((key, self.db.connection().escape(val)) for (key, val) in args.items())
                else:
                    # If it's not a dictionary let's try escaping it anyways.
                    # Worst case it will throw a Value error
                    escaped_args = self.db.connection().escape(args)
                sql = sql % escaped_args

            self.last_sql = sql

            cur = self.db.query(sql=sql)
            self.db.connection().commit()
            if return_cursor:
                return cur
            else:
                if query_type in ('insert', 'update', 'delete'):
                    return cur.rowcount
                else:
                    return 1
        if return_cursor:
            return None
        else:
            return 0

    def default_process(self, query_type, table, params):
        sql = ''
        args = None
        for case in helper.Switch(query_type):
            if case('query'):
                sql = params.replace(self.source_db, self.to_db)
                args = None
                break
            if case('insert'):
                keys = list()
                values = list()
                for k, v in params.items():
                    keys.append('`' + str(k) + '`')
                    values.append('%(' + str(k) + ')s')
                sql = """INSERT INTO `%s` (%s) VALUES (%s);""" % (table, ', '.join(keys), ', '.join(values))
                args = params
                break
            if case('update'):
                sets = list()
                wheres = list()
                for k, v in params.items():
                    if k not in self.primary_keys[table]:
                        sets.append('`' + str(k) + '`' + '= %(' + str(k) + ')s')
                    else:
                        wheres.append('`' + str(k) + '`' + '= %(' + str(k) + ')s')
                sql = """UPDATE `%s` set %s WHERE %s;""" % (table, ', '.join(sets), 'AND '.join(wheres))
                args = params
                break
            if case('delete'):
                wheres = list()
                for k, v in params.items():
                    if k in self.primary_keys[table]:
                        wheres.append('`' + str(k) + '`' + '= %(' + str(k) + ')s')

                sql = """DELETE FROM `%s` WHERE %s;""" % (table, 'AND '.join(wheres))
                args = params
                break
        return [sql, args]

    @staticmethod
    def multiple_replace(dic, text):
        pattern = "|".join(map(re.escape, dic.keys()))
        return re.sub(pattern, lambda m: dic[m.group()], text)

    def error_log(self, msg):
        helper.log_by_date(msg, self.log_name, 'error', self.log_path)

    def msg_log(self, msg):
        helper.log_by_date(msg, self.log_name, 'msg', self.log_path)


if __name__ == '__main__':
    Observer().watch()