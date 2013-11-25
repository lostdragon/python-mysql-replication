__author__ = 'lost'

import pymysql
import sys


class Switch(object):
    """ Switch Class.

    Do Switch like PHP.

    Attributes:
        value: switch variable.

    """

    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration

    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args:
            self.fall = True
            return True
        else:
            return False


class Database(object):
    def __init__(self, connection_settings):
        self.__connection_settings = connection_settings
        self.__connection_settings["charset"] = "utf8"
        self.__connected_ctl = False

        self._ctl_connection_settings = dict()
        self._ctl_connection = False

    def close(self):
        if self.__connected_ctl:
            self._ctl_connection.close()
            self.__connected_ctl = False

    def connection(self):
        if not self.__connected_ctl:
            self.__connect_to_ctl()
        return self._ctl_connection

    def cursor(self):
        if not self.__connected_ctl:
            self.__connect_to_ctl()
        return self._ctl_connection.cursor()

    def __connect_to_ctl(self):
        self._ctl_connection_settings = dict(self.__connection_settings)
        self._ctl_connection_settings["cursorclass"] = \
            pymysql.cursors.DictCursor
        self._ctl_connection = pymysql.connect(**self._ctl_connection_settings)
        self.__connected_ctl = True

    def query(self, sql, args=None):
        for i in range(1, 3):
            try:
                if not self.__connected_ctl:
                    self.__connect_to_ctl()

                # reconnect
                self._ctl_connection.ping(reconnect=True)

                cur = self._ctl_connection.cursor()
                cur.execute(sql, args)
                return cur
            except pymysql.OperationalError as error:
                code, message = error.args
                # 2013: Connection Lost
                if code == 2013:
                    self.__connected_ctl = False
                    continue
                else:
                    raise error
            except IOError as error:
                code, message = error.args
                # 32: Broken pipe
                if code == 32:
                    self.__connected_ctl = False
                    continue
                else:
                    raise error


class LogInfo(object):
    def __init__(self, logfile="error.log"):
        try:
            import logging
            self.logger = None
            self.logger = logging.getLogger()
            self.hdlr = logging.FileHandler(logfile)
            formatter = logging.Formatter('[%(asctime)s]:  %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S')
            self.hdlr.setFormatter(formatter)
            self.logger.addHandler(self.hdlr)
            self.logger.setLevel(logging.DEBUG)
        except Exception as e:
            print e

    def output(self, loginfo, errorflag=0):
        try:
            if errorflag:
                self.logger.error('error: ' + loginfo)
            else:
                self.logger.error(loginfo)
        except Exception as e:
            print e

    def close(self):
        try:
            self.hdlr.close()
            self.logger.removeHandler(self.hdlr)
        except Exception as e:
            print e

#------------------------------
#error log function
#------------------------------


def error_log(errormsg, path=''):
    log = LogInfo(path + 'error.log')
    log.output(errormsg)
    log.close()


def log_msg(msg, path=''):
    log = LogInfo(path + 'msg.log')
    log.output(msg)
    log.close()


def log_by_date(msg, log_name='', log_type='msg', path=''):
    from datetime import date
    today = date.today()
    logfile = path + 'logs/%s_%s_%d_%d_%d.log' % (log_name, log_type, today.year, today.month, today.day)
    log = LogInfo(logfile)
    log.output(msg)
    log.close()

#------------------------------
#detailed error log function
#------------------------------


def error_log_detailed(errormsg):
    log = LogInfo('error.log')
    info = sys.exc_info()
    log.output(errormsg + ' : ' + str(info[0]) + ':' + str(info[1]))
    log.close()