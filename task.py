#coding: utf-8

import datetime
import os
import shutil
from context import AppContext

class Task(object):
    context = AppContext()
    SOURCE_FILE_PATH = '/net/20/kun/source/'
    OUTPUT_FILE_PATH = '/net/20/kun/output/'
    TASK_LOG_FILE = 'start_task.log'
    # state: 0: new 1:running 2:finished-OK 3:finished-Error
    STATE_NEW = 0
    STATE_RUNNIG = 1
    STATE_OK = 2
    STATE_ERROR = 3
    STATE_STOP = 4
    state_str = ['Waiting', 'Running', 'OK', 'Error', 'Stopped']

    def __init__(self, pollutant, date, hour):
        self.pollutant = pollutant
        self.date = date
        self.hour = '0'+str(hour) if hour < 10 else str(hour)
        self.name = "%s_%s_%s" % (self.pollutant, self.date, self.hour)
        self.server = None
        self.path = ''
        self.pid = 0
        self.state = Task.STATE_NEW
        self.start_time = None
        self.end_time = None
        self.run_time = None

    def register(self, server):
        self.server = server
        server.add_task(self)
        self.path = server.workspace + self.name
        return self

    def start(self):
        self.state = Task.STATE_NEW
        self.prepare()
        #return self
        run_command = "cd %s && nohup ./run.sh > run.log &" % self.path
        print run_command
        output = self.server.run_bg(run_command)
        self.start_time = datetime.datetime.now()
        self.state = Task.STATE_RUNNIG
        self.save()
        return self

    def clean(self):
        if self.state==Task.STATE_NEW:
            return
        self.copy_output_files()
        if self.state==Task.STATE_RUNNIG:
            self.check_finished()
        shutil.rmtree(self.path)

    def prepare(self):
        if os.path.isdir(self.path):
            # shutil.copy2(os.getcwd()+"/run.sh", self.path)
            return
        os.mkdir(self.path)
        for f in self.get_common_files():
            shutil.copy2(f, self.path)
        shutil.copy2(self.get_inp(), self.path+"/aermod.inp")
        for mp in self.get_MP():
            shutil.copy2(mp, self.path)
        shutil.copy2(self.get_source(), self.path+"/source")
        # shutil.copy2(self.get_HOUREMIS(), self.path+"/HOUREMIS")
        shutil.copy2(os.getcwd()+"/run.sh", self.path)

    def change_state(self, new_state):
        if self.state == new_state:
            return
        self.state = new_state
        self.context.db.update({'name': self.name}, {'$set': {'state':self.state}})

    def save(self):
        self.context.db.db.tasks.update({'name': self.name},
                                        {'$set': {'state'   : self.state,
                                                  'start_at': self.start_time,
                                                  'end_at'  : self.end_time,
                                                  'server'  : self.server.host if self.server else ''}
                                        })

    def check_finished(self):
        if self.state==Task.STATE_ERROR or self.state==Task.STATE_OK:
            return
        logfile = self.context.OUTPUT_FILE_PATH+self.name+'.log'
        if not os.path.exists(logfile):
            return
        has_error = False
        start_time = None
        end_time = None
        with open(logfile, 'r') as log:
            for line in log:
                if line.endswith('start!\n'):
                    start_time = self.context.parse_time(line[:-10])
                elif line.endswith('end!\n'):
                    end_time = self.context.parse_time(line[:-8])
                elif line.lower().find('error')>0:
                    has_error = True
        if has_error:
            self.state = Task.STATE_ERROR
        else:
            self.state = Task.STATE_OK
        # 早期有的log没有记录结束时间，
        # 方法1 通过os.path.mtime(logfile)失败，因为中间有一次整体拷贝，ctime被重置了
        # 方法2 留空或者用平均时间代替
        if end_time:
            self.run_time = end_time - start_time
            if self.start_time:
                self.end_time = self.start_time + self.run_time
            elif self.server and self.server.state.time_delta:
                self.start_time += self.server.state.time_delta
                self.end_time = self.start_time + self.run_time
        self.save()

    def report(self):
        return {'name':self.name,
                'server': self.server.host,
                'start': str(self.start_time),
                'rtime': str(self.run_time),
                'state': Task.state_str[self.state],
        }

    def copy_run_files(self):
        source_files = {
            'aermod' : 'aermod',
            'aermod.inp/{BC_0103_01}.inp': 'aermod.inp',
            'source' : 'source',
            'receptor' : 'receptor',
            'MP/{0103}/MP.PFL' : 'MP.PFL',
            'MP/{0103}/MP.SFC' : 'MP.SFC',
            'HOUREMIS/{BC_0103}' : 'HOUREMIS'
        }
        source_files2 = {
            'aermod' : 'aermod',
            '{BC_0103_01}/aermod.inp': 'aermod.inp',
            'source' : 'source',
            'receptor' : 'receptor',
            'MP/{0103}/MP.PFL' : 'MP.PFL',
            'MP/{0103}/MP.SFC' : 'MP.SFC',
            '{}/HOUREMIS' : 'HOUREMIS'
        }
        sources = []
        targets = []
        for source in os.listdir(Task.SOURCE_FILE_PATH):
            if os.path.isfile(Task.SOURCE_FILE_PATH+source):
                sources
            if os.path.isdir(Task.SOURCE_FILE_PATH+source):
                pass

    def copy_output_files(self):
        output_files = [('/run.log', '.log'), ('/ERRORS.OUT', '.error'), ('/aermod.out', '.out')]
        for s, t in output_files:
            source = self.path + s
            target = Task.OUTPUT_FILE_PATH + self.name + t
            # todo: prevent file duplicated copy
            if os.path.exists(source): #and not os.path.exists(target):
                shutil.copy2(source, target)

    def get_common_files(self):
        files = map(lambda f: Task.SOURCE_FILE_PATH+f, ["aermod", "receptor"])
        return files

    def get_inp(self):
        # inp = "%sinps/Linerun_%s_%s/aermod_%s.inp" % (Task.SOURCE_FILE_PATH, self.pollutant, self.date, self.hour)
        inp = "%sinps/aermod_%s_%s_%s.inp" % (Task.SOURCE_FILE_PATH, self.pollutant, self.date, self.hour)
        return inp

    def get_MP(self):
        pfl = Task.SOURCE_FILE_PATH+"MPs/13"+self.date+"/MP.PFL"
        sfc = Task.SOURCE_FILE_PATH+"MPs/13"+self.date+"/MP.SFC"
        return [pfl, sfc]

    def get_source(self):
        return "%ssources/source_%s_%s" % (Task.SOURCE_FILE_PATH, self.pollutant, self.hour)

    def get_HOUREMIS(self):
        # todo NO2 -> NOX
        pollutant = self.pollutant if self.pollutant != "NO2" else "NOX"
        HOUREMIS = "%sHOUREMIS/HOUREMIS_%s_%s" % (Task.SOURCE_FILE_PATH, pollutant, self.date)
        return HOUREMIS


if __name__ == '__main__':
    t = Task("BC", "0403", "09")
    print t.get_inp()
    print t.get_MP()
    print t.get_HOUREMIS()
