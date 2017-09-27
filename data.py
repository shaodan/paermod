# coding=utf-8

import sqlite3
from pymongo import MongoClient
from task import Task, TaskFactory
from server import ServerFactory
from context import Context


context = Context()
sf = ServerFactory()
tf = TaskFactory()


class DB(object):

    def __init__(self):
        self.db = None
        self.context = context
        self.servers = None
        self.server_table = None
        self.tasks = None
        self.task_table = None

    def load_data(self):
        self.fetch_data()
        context.db = self
        context.servers = self.servers
        context.tasks = self.tasks
        context.server_table = self.server_table
        context.task_table = self.task_table
        
        # 一开始的task没有记录state，需要检查output下面的log进行判断
        # for task in context.waiting:
        #     task.check_finished()
        context.waiting = filter(lambda t:t.state==Task.STATE_NEW, context.tasks)
        
    def fetch_data(self):
        raise NotImplementedError("Class DB is NOT instanceable!")
        
    def get_finished_tasks(self):
        outputs = os.listdir(Task.OUTPUT_FILE_PATH)

    def get_runnig_tasks(self):
        tasks = []
        for server in get_server_table().values():
            tasks.extend(server.state.running_tasks)
        return tasks


class DBMongo(DB):
    
    def __init__(self, collection='aermod'):
        super(self.__class__, self).__init__()
        client = MongoClient('localhost', 27017)
        # TODO: binding collection to task group
        self.db = client[collection]

    def fetch_data(self):
        #TODO auto-reload data by a timer
        server_list = []
        for s in self.db.servers.find():
            new_server = sf.createServer(s)
            server_list.append(new_server)
        self.servers = server_list
        self.server_table = {server.host: server for server in server_list}

        task_list = []
        for t in self.db.tasks.find():
            name = t['name'].encode()
            if name.startswith('NO') or name.startswith('CO'):
                continue
            server = t['server'].encode()
            start_time = t['start_at']
            end_time = t['end_at']
            state = int(t['state'])
            p, s, d, h = name.split('_')
            task = Task(p, d, int(h), s)
            task.state = state
            task.start_time = start_time if start_time else None
            task.end_time = end_time if end_time else None
            if server: # and task.state==Task.STATE_RUNNING
                task.register(self.server_table[server])
            task_list.append(task)

        self.tasks = task_list
        self.task_table = {task.name: task for task in task_list}
    
    def create_table(self, table):
        self.db.add_collection(table)
        
    def add_tasks(self, tasks):
        context.db = self
        for t in tasks:
            t.save()
        
    def add_servers(self, servers):
        context.db = self
        for s in servers:
            self.db.servers.insert_one(
                {
                    'host'      : s.host,
                    'workspace' : s.workspace,
                    'weight'    : s.weight
                }
            )
    

class DBMockup(DB):
    
    def __init__(self):
        super(self.__class__, self).__init__()
        self.fetch_data()

    def fetch_data(self):
        hosts = ['sheet20', 'sheet16', 'sheet17', 'sheet18', 'sheet19', 'sheet21']
        spaces = ['/net/20/kun/AERMOD/', '/net/12/kun/', '/net/17/kun/', '/net/18/kun/', '/net/19/kun/', '/net/21_2/kun/']
        weights = [3, 1, 1, 1, 1, 3]
        # pollutants = ["BC", "CO", "NO2", "PM"]
        pollutants = ["BC", "PM"]
        dates = ["0103", "0403", "0703", "1003"]
        hours = range(1, 25)
        # situations = ['wkd', 'wke', 'apec', 'jam']
        situations = ['wkd', 'wke']
        self.servers = [sf.createServer(t) for t in zip(hosts, spaces, weights)]
        self.server_table = {server.host: server for server in self.servers}
        self.tasks  = [Task(p, d, h, s) for p in pollutants for s in situations for d in dates for h in hours]
        # self.tasks = [tf.createTask((p, d, h, s)) for p in pollutants for s in situations for d in dates for h in hours]
        self.task_table = {task.name: task for task in self.tasks}


class DBSqlite(DB):
    
    def fetch_data(self):
        if not self.db:
            self.conn = sqlite3.connect(context.SQLITE3_DB)
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
    # d = DB()
    dbm = DBMockup()
    dbg = DBMongo()
    dbg.add_tasks(dbm.tasks)

