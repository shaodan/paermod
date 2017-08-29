# coding=utf-8

import sys
import gevent
import threading
from multiprocessing.dummy import Pool as ThreadPool
import time
from datetime import datetime
from server import Server, LocalServer
from task import Task
from context import Context
import data


context = Context()


class Monitor(threading.Thread):
    def __init__(self, log=False, keep_alive=True):
        super(Monitor, self).__init__(name='Monitor Thread')
        self.servers = context.servers
        self.interval = context.MONITOR_INTERVAL
        self.log = log
        self.keep_alive = keep_alive
        self.lock = threading.Lock()
        # self.mark = len(self.servers)

    def handler(self, s):
        s.monitor()
        if self.log:
            self.lock.acquire()
            print '%s: %0.2f%% Tasks: %d' % (s.host, s.state.cpu, len(s.state.running))
            print map(lambda t:t.name, s.state.running)
            self.lock.release()
        # s.clean()
        # s.state.updated = True
        
    def stop(self):
        self.keep_alive = False

    def run(self):
        # self.log = True
        # for s in self.servers:
        #     self.handler(s)
        # return
        
        ## asynchronization
        # 1. gevent coroutine
        # gevent.joinall([gevent.spawn(s.monitor) for s in servers])
        # 2. ThreadPool
        self.pool = ThreadPool(len(self.servers))
        while 1:
            print '>>>> Update Server State: ' + str(datetime.now())
            # self.mark = len(self.servers)
            self.pool.map(self.handler, self.servers)
            for i in xrange(5):
                if not self.keep_alive:
                    print 'monitor stoping...'
                    # pool must close before join(wait workers finish)
                    self.pool.close()
                    self.pool.join()
                    print 'monitor stoped!'
                    return
                time.sleep(self.interval/5)
            context.running = []
            for s in context.servers:
                context.running.extend(s.state.running)
            #TODO 避免重复启动
            context.waiting = filter(lambda t:t.state==Task.STATE_NEW, context.waiting)
            print 'Waiting Tasks: ', len(context.waiting)
            print 'Running Tasks: ', len(context.running)
            self.task_queue()

    def task_queue(self):
        if not len(context.waiting):
            return
        free = {}
        total_free = 0
        for s in self.servers:
            free_core = int(s.state.cores*(context.MAX_CPU - s.state.cpu)/100.0)
            free_task = context.MAX_TASK_PER_SERVER - len(s.state.running)
            free_min = min(free_task, free_core)
            if free_min > 0:
                free[s.host] = free_min * s.weight
                total_free += free_min
            #for t in context.pop_task(free_min):
                #print "Start Task %s on %s" % (t.name, s.host)
                #t.register(s).start()
        # total_free = sum(free.values())
        for t in context.waiting:
            if total_free == 0:
                print "No free Server, Keep waiting..."
                break
            # todo: task固定server，减少文件拷贝
            if False and t.server:
                s = t.server
                if s.host in free and free[s.host] > 0:
                    print "Start Task %s on %s" % (t.name, s.host)
                    t.start()
                    total_free -= 1
                    free[s.host] -= 1
                else:
                    print " Task %s is waiting for Server %s" % (s.host, t.name)
            else:
                s = max(free, key=free.get)
                s = context.get_server(s)
                print "Start Task %s on %s" % (t.name, s.host)
                t.register(s).start()
                total_free -= 1
                free[s.host] -= s.weight
        context.waiting = filter(lambda t:t.state==Task.STATE_NEW, context.waiting)
        
    def stop_task(self, task):
        pass
        # task = context.get_task(task)
        # if task:
        #     return task.stop()
        # return 'Task Not Found'


if __name__ == '__main__':
    if len(sys.argv) == 1:
        context.servers = [LocalServer()]
    elif sys.argv[1] == 'all':
        # load servers and tasks from db
        # data.DBMockup()
        db = data.DBMongo()
    else:
        context.servers = [Server(sys.argv[1])]

    # case 1
    Monitor(log=True, keep_alive=False).handler(context.servers[0])

    # case 2
    #Monitor(log=True, keep_alive=False).start()

    # case 3
    # m = Monitor(context, log=True)
    # m.start()
    # signal = raw_input()
    # if signal == 'q':
    #     m.keep_alive = False
