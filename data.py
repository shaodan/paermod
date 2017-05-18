# coding=utf-8

import sqlite3
from pymongo import MongoClient
from task import Task
from server import Server, LocalServer
from context import AppContext

class DB(object):

    def __init__(self):
        self.context = AppContext()
        self.db = None
        self.servers = None
        self.server_table = None
        self.tasks = None
        self.task_table = None
        self.load_data()

    def load_data(self):
        self.fetch_data()
        self.context.db = self
        self.context.servers = self.servers
        self.context.tasks = self.tasks
        self.context.server_table = self.server_table
        self.context.task_table = self.task_table

        self.context.waiting = filter(lambda t:t.state==Task.STATE_NEW, self.context.tasks)
        # 一开始的task没有记录state，需要检查output下面的log进行判断
        #for task in self.context.waiting:
        #    task.check_finished()
        #self.context.waiting = filter(lambda t:t.state==Task.STATE_NEW, self.context.waiting)

class DBMongo(DB):

    def fetch_data(self):
        if not self.db:
            client = MongoClient('localhost', 27017)
            self.db = client['aermod']
        server_list = []
        for s in self.db['servers'].find():
            host = s['host'].encode()
            workspace = s['workspace'].encode()
            if host == self.context.Master:
                server_list.append(LocalServer(workspace))
            else:
                server_list.append(Server(host, workspace))
        self.servers = server_list
        self.server_table = {server.host: server for server in server_list}

        task_list = []
        for t in self.db['tasks'].find():
            name = t['name'].encode()
            if name.startswith('NO'):
                continue
            server = t['server'].encode()
            start_time = t['start_at']
            end_time = t['end_at']
            state = t['state']
            p, d, h = name.split('_')
            task = Task(p, d, int(h))
            task.state = state
            task.start_time = start_time if start_time else None
            task.end_time = end_time if end_time else None
            if server:
                task.register(self.server_table[server])
            task_list.append(task)

        self.tasks = task_list
        self.task_table = {task.name: task for task in task_list}


class DBMockup(DB):

    def fetch_data(self):
        master = 'sheet20'
        user = 'kun'
        password = 'wpw2016'
        master_disk = '/net/20/kun/AERMOD/'
        slavers = ['sheet16', 'sheet17', 'sheet18', 'sheet19', 'sheet21']
        disks = ['/net/12', '/net/17', '/net/18', '/net/19', '/net/21_2']
        spaces = [d+"/kun/" for d in disks]
        pollutants = ["BC", "CO", "NO2", "PM"]
        dates = ["0103", "0403", "0703", "1003"]
        hours = range(1, 25)
        self.servers = [LocalServer(master_disk)] + [Server(h, d) for h, d in zip(slavers, spaces)]
        self.server_table = {server.host: server for server in self.servers}
        self.tasks  = [Task(p, d, h) for p in pollutants for d in dates for h in hours]
        self.task_table = {task.name: task for task in self.tasks}


def get_finished_tasks():
    outputs = os.listdir(Task.OUTPUT_FILE_PATH)

def get_runnig_tasks():
    tasks = []
    for server in get_server_table().values():
        tasks.extend(server.state.running_tasks)
    return tasks




class SqliteClient():
    def __init__(self):
        self.conn = sqlite3.connect(AppContext().SQLITE3_DB)
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def create_table(self, table, ):
        # self.cursor.execute('create table user (id varchar(20) primary key, name varchar(20))')
        self.cursor.execute('create table task (name varchar(10) primary key, ')

    def authenticate(self, ):
        pass


if __name__ == '__main__':
    # insert_servers()
    # insert_tasks()
    d = DB()
    # d.fetch_from_mockup()

