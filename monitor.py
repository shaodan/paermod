# coding=utf-8

import sys
import gevent
import threading
from multiprocessing.dummy import Pool as ThreadPool
import time
from datetime import datetime
from server import Server, LocalServer
from task import Task
from context import AppContext
import data


class Monitor(threading.Thread):
    def __init__(self, interval=30, log=False, stop=False):
        super(Monitor, self).__init__(name='Monitor Thread')
        self.context = AppContext()
        self.servers = self.context.servers
        self.interval = interval
        self.log = log
        self.stop = stop
        self.lock = threading.Lock()

    def handler(self, s):
        s.monitor()
        if self.log:
            print '%s: %0.2f%% Tasks: %d' % (s.host, s.state.cpu, len(s.state.running))
            print map(lambda t:t.name, s.state.running)
        s.clean()
        # self.lock.acquire()
        # self.context.running.extend(s.state.running)
        # self.lock.release()

    def run(self):
        # self.log = True
        # for s in self.servers[]:
        #     self.handler(s)
        # return
        ## asynchronization
        # 1. gevent coroutine
        # gevent.joinall([gevent.spawn(s.monitor) for s in servers])
        # 2. ThreadPool
        self.pool = ThreadPool(len(self.servers))
        while 1:
            print '>>>> Update Server State: ' + str(datetime.now())
            # for s in self.servers:
            #     self.handler(s)
            self.pool.map(self.handler, self.servers)
            for i in xrange(5):
                if self.stop:
                    self.pool.close()
                    self.pool.join()
                    return
                time.sleep(self.interval/5)
            print 'Waiting List: ', [w.name for w in self.context.waiting]
            self.context.running = []
            for s in self.context.servers:
                self.context.running.extend(s.state.running)
            print 'Running Tasks: ', len(self.context.running)
            self.task_queue()

    def task_queue(self):
        if not len(self.context.waiting):
            return
        free = {}
        total_free = 0
        for s in self.servers:
            free_core = int(s.state.cores*(self.context.MAX_CPU - s.state.cpu)/100.0)
            free_task = self.context.MAX_TASK_PER_SERVER - len(s.state.running)
            free_min = min(free_task, free_core)
            if free_min > 0:
                free[s.host] = free_min * s.weight
                total_free += free_min
            #for t in self.context.pop_task(free_min):
                #print "Start Task %s on %s" % (t.name, s.host)
                #t.register(s).start()
        # total_free = sum(free.values())
        for t in self.context.waiting:
            if total_free == 0:
                print "No free Server, Keep waiting..."
                break
            # todo: 123
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
                s = self.context.get_server(s)
                print "Start Task %s on %s" % (t.name, s.host)
                t.register(s).start()
                total_free -= 1
                free[s.host] -= s.weight
        self.context.waiting = filter(lambda t:t.state==Task.STATE_NEW, self.context.waiting)


if __name__ == '__main__':
    context = AppContext()
    if len(sys.argv) == 1:
        context.servers = [LocalServer()]
    elif sys.argv[1] == 'all':
        # load servers and tasks from db
        # data.DBMockup()
        db = data.DBMongo()
    else:
        context.servers = [Server(sys.argv[1])]

    # case 1
    Monitor(log=True, stop=True).handler(context.servers[0])

    # case 2
    #Monitor(log=True, stop=True).start()

    # case 3
    # m = Monitor(context, log=True)
    # m.start()
    # signal = raw_input()
    # if signal == 'q':
    #     m.stop = True
