# coding=utf-8

from multiprocessing.dummy import Pool as ThreadPool
from task import Task
from server import Server, LocalServer
from pollutant import Pollutant
import data
import web


def monitor():
    pass

def web_server():
    PORT = 8000
    web_server = web.WebServer(PORT)
    web_server.start()

def main():
    '''
    1 Initialization
        1.1 Initialize Servers
            1.1.1 start server monitor
        1.2 Initialize tasks
            1.2.1 attach tasks to servers, try to assign same pollutants to same server
            1.2.2 copy data/program/config files
            1.2.3 ssh nohup run program shell
        1.3 Task monitor:
            1.3.1 check task status: running|finished|error
            1.3.2 if running do nothing
            1.3.3 if finished, first copy log and output files,FINISHED MARK, then goto 1.2
            1.3.4 if error mark task and report, try to restart

    List of Server: store the whole cloud
        Server : store ssh-configs
                 store server status, include cpu loads and task status
                 maintain ssh connection sessions
                 store running tasks
    List of Tasks : store total tasks need to work out
        Task : store task status
               Finished/Error/Runing mark

    '''
    # servers = [ Server(s) for s in hosts]
    # for server in servers:
    #     server.start_monitor()
    # tasks = [Taks(p, d, h) for p in pollutants for d in dates for h in hours]

    web_server()

if __name__ == '__main__':
    main()

