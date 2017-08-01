# coding=utf-8

from task import Task
from server import Server, LocalServer
import data

def task_run():
    s = LocalServer('/net/20/kun/AERMOD/')
    # s = Server("sheet21", "/net/21_2/kun/")
    # s = Server("sheet19", "/net/19/kun/")
    # s = Server("sheet18", "/net/18/kun/")
    # s = Server("sheet17", "/net/17/kun/")
    # s = Server("sheet16", "/net/12/kun/")
    # for h in range(8, 13):
    #     t = Task("CO", "0703", h)
    #     t.register(s).start()
    t = Task("BC", "0103", 1)
    t.register(s).start()

def task_run2():
    db = data.DBMongo()
    waiting = [ 'CO_0703_08','CO_0703_11','CO_0703_12',
                "CO_1003_09"]
    resource = ['sheet21']*1 + ['sheet20']*3
    for t, s in zip(waiting, resource):
        task = db.context.get_task(t)
        server = db.context.get_server(s)
        task.register(server).start()


if __name__ == '__main__':
    task_run()
    # task_run2()

