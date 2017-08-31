#coding: utf-8

import datetime
import time
import os
import shutil
from context import Context

context = Context()

class Task(object):
    # TODO: Enum, move logic to context
    NAME_TEMPLATE = '{pollutant}_{date}_{situation}_{hour}'
    STATE_NEW = 0
    STATE_RUNNING = 1
    STATE_OK = 2
    STATE_ERROR = 3
    STATE_STOP = 4
    STATE_START = 5
    STATE_CLEAN = 6
    state_str = ['Waiting', 'Running', 'OK', 'Error', 'Stopped', 'Starting', 'Cleaning']
    # TODO: new state(starting & cleaning)

    def __init__(self, pollutant, date, hour, situation):
        self.pollutant = pollutant
        self.date = date
        self.hour = '0'+str(hour) if hour < 10 else str(hour)
        self.situation = situation
        self.name = "%s_%s_%s_%s" % (self.pollutant,self.situation, self.date, self.hour)
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
        #TODO state starting! only when finish mark state as running
        self.state = Task.STATE_NEW
        self.prepare()
        #return self
        run_command = "cd %s && nohup ./run.sh > run.log &" % self.path
        # print run_command
        output = self.server.run_bg(run_command)
        self.start_time = datetime.datetime.now()
        self.state = Task.STATE_RUNNING
        self.save()
        return self
    
    def stop(self, pid=0):
        if self.state != Task.STATE_RUNNING:
            return 'Task not Running'
        if self.pid < 1:
            return 'Task pid error'
        # TODO: check pid is true and matches task name
        if pid < 1:
            pid = self.pid
        while not self.server.updated:
            time.sleep(1)
        stop_command = 'kill -9 ' + str(pid)
        sign = self.server.run(stop_command)
        # TODO: check stop signal
        self.state = Task.STATE_STOP
        self.save()
        return sign

    def clean(self):
        if self.state==Task.STATE_NEW:
            return
        self.copy_output_files()
        if self.state==Task.STATE_RUNNING:
            self.check_finished()
        shutil.rmtree(self.path)

    def prepare(self):
        if os.path.isdir(self.path):
            # shutil.copy2(os.getcwd()+"/run.sh", self.path)
            return
        os.mkdir(self.path)
        self.copy_run_files()

    def change_state(self, new_state):
        if self.state == new_state:
            return
        self.state = new_state
        context.db.update({'name': self.name}, {'$set': {'state':self.state}})

    def save(self):
        context.db.db.tasks.update({'name': self.name },
                                   {'$set': {'state'   : self.state,
                                             'start_at': self.start_time,
                                             'end_at'  : self.end_time,
                                             'server'  : self.server.host if self.server else ''
                                            },
                                   }, upsert=True
                                  )

    def check_finished(self):
        if self.state==Task.STATE_ERROR or self.state==Task.STATE_OK:
            return
        logfile = context.OUTPUT_FILE_PATH+self.name+'.log'
        if not os.path.exists(logfile):
            return
        has_error = False
        start_time = None
        end_time = None
        with open(logfile, 'r') as log:
            for line in log:
                if line.endswith('start!\n'):
                    start_time = context.parse_time(line[:-10])
                elif line.endswith('end!\n'):
                    end_time = context.parse_time(line[:-8])
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
        return {
            'name'  : self.name,
            'server': self.server.host if self.server else '',
            'start' : str(self.start_time)[:19],
            'rtime' : context.dt_to_str(self.run_time),
            'state' : Task.state_str[self.state],
            'pid'   : self.pid
        }

    def copy_run_files(self):
        run_files = [
            ('aermod'     , 'aermod'),
            ('receptor'   , 'receptor'),
            ('aermod.inp' , 'inps/{pollutant}_{date}_{hour}.inp'),
            ('source'     , 'sources/{pollutant}_{situation}_{hour}'),
            ('MP.PFL'     , 'MPs/13{date}.PFL'),
            ('MP.SFC'     , 'MPs/13{date}.SFC'),
        ]
        for t, s in run_files:
            s = context.SOURCE_FILE_PATH + s
            target = '%s/%s' % (self.path, t)
            source = s.format(**vars(self))
            # py2 doesn't support string.format_map
            # source = s.format_map(vars(self))
            # print 'copying from %s to %s' % (source, target)
            shutil.copy2(source, target)
        shutil.copy2(os.getcwd()+"/run.sh", self.path)

    def copy_output_files(self):
        output_files = [
            ('aermod.out', '.out'),
            ('ERRORS.OUT', '.err'),
            ('run.log', '.log'),
        ]
        for s, t in output_files:
            source = '%s/%s'  % (self.path, s)
            target = '%s%s%s' % (context.OUTPUT_FILE_PATH, self.name, t)
            # todo: prevent file duplicated copy
            if os.path.exists(source): #and not os.path.exists(target):
                shutil.copy2(source, target)


class Task2(Task):
        
    # TODO: task type
    def __init__(self, ttype, attr):
        # encode
        self.__dict__.update(attr)
        self.pollutant = pollutant
        self.date = date
        self.hour = '0'+str(hour) if hour < 10 else str(hour)
        self.situation = situation
        self.name = "%s_%s_%s_%s" % (self.pollutant, self.date, self.hour, self.situation)
        self.server = None
        self.path = ''
        self.pid = 0
        self.state = Task.STATE_NEW
        self.start_time = None
        self.end_time = None
        self.run_time = None
                
    
class TaskFactory(object):
    
    def createTask(self, t):
        if isinstance(t, dict):
            name = t['name'].encode
            server = t['server'].encode()
            start_time = t['start_at']
            end_time = t['end_at']
            state = int(t['state'])
            p, d, h, s = (name.split('_') + [''])[0:4]
            h = int(h)
            if server:
                task.register(self.server_table[server])
            task_list.append(task)
        elif isinstance(t, tuple):
            p, d, h, s = t
            start_at = None
            end_time = None
        else:
            return None
        task = Task(p, d, h, s)
        task.start_time = start_time
        task.end_time = end_time
        return task
    
    def createTaskFromTaskType(self, name, attrs, name_template, run_files, out_files):
        '''
        attrs : "[pollutant, date, hour, situation]"
        name_template: "{p}_{d}_{h}_{s}"
        run_files: "source:sources/{p}_{h}_{s}", aermod.inp:inps/{p}_{d}_{h}.inp,..."
        out_files: "aermod.out:{name}.out, run.log:{name}.log"
        '''
        ttype = TaskType(a, n, r, o)
        task = ttype.createTask(name)
        # https://github.com/r1chardj0n3s/parse
        # parse.parse(template, )

    
if __name__ == '__main__':
    t = Task("BC", "0403", 9)
    # print t.get_inp()
    # print t.get_MP()
    # print t.get_HOUREMIS()
