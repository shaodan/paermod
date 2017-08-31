# coding=utf-8

import datetime
import socket


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Context():
    __metaclass__ = Singleton

    def __init__(self, ):
        # data
        self.db = None
        self.servers = None
        self.tasks = None
        self.server_table = None
        self.task_table = None

        # job queue
        self.running = None
        self.waiting = None

        # db config
        self.SQLITE3_DB = 'sqlite3.db'
        self.MONGO_DB = 'localhost:27017'
        
        # monitor config
        self.MONITOR_INTERVAL = 30
        
        # server config
        self.Master = socket.gethostname()
        self.MAX_TASK_PER_SERVER = 12
        self.MAX_CPU = 50.1
        self.MONITOR_INTERVAL = 30
        self.USER = 'kun'
        self.PASSWORD = 'wpw2016'
        
        # task config
        self.SOURCE_FILE_PATH = '/net/20/kun/source/'
        self.OUTPUT_FILE_PATH = '/net/20/kun/output/'
        # self.TASK_LOG_FILE = 'start_task.log'
        self.RUN_LOG = 'run.log'
        self.OUTPUT_TO_DIR = False
        self.OUTPUT_FILES = [('aermod.out', '.out'), ('ERRORS.OUT', '.error'), ('run.log', '.log')]

    def report(self):
        return {
            'server_count'  : len(self.servers),
            'task_total'    : len(self.tasks),
            'task_running'  : sum([len(s.state.running) for s in self.servers]),
            'task_waiting'  : len(self.waiting),
            'task_error'    : 0
        }

    def get_server(self, host):
        if host in self.server_table:
            return self.server_table[host]
        return None

    def get_task(self, name):
        if name in self.task_table:
            return self.task_table[name]
        return None

    def pop_task(self, pop=1):
        if not self.waiting:
            self.waiting = filter(lambda t: t.a, self.tasks)
        pro = []
        if len(self.waiting) <= pop:
            pro.extend(self.waiting)
            del self.waiting[:]
        else:
            for i in xrange(pop):
                pro.append(self.waiting.pop())
        return pro

    def get_running_task(self):
        return self.running

    def parse_time(self, timestr):
        if timestr.startswith('20'):
            # 2017年 01月 31日 星期二 18:56:51 CST
            time_partial = timestr.split(' ')
            year = int(time_partial[0][:4])
            month = int(time_partial[1][:2])
            day = int(time_partial[2][:2])
            hms = time_partial[4].split(':')
            hour = int(hms[0])
            minute = int(hms[1])
            second = int(hms[2])
            zone = time_partial[5]
            return datetime.datetime(year, month, day, hour, minute, second)
        else:
            # Mon Jan 30 19:00:14 CST 2017
            return datetime.datetime.strptime(timestr, '%a %b %d %H:%M:%S %Z %Y')
        
    def dt_to_str(self, dt):
        if not dt:
            return ''
        day = dt.days
        mins, sec = divmod(dt.seconds, 60)
        hour, mins = divmod(mins, 60)
        return '%dD %02d:%02d:%02d' % (day, hour, mins, sec)

if __name__ == '__main__':
    context = Context()
    print context.servers

