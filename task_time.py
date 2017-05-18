#coding: utf-8

import datetime
import time
import os
import collections
from pymongo import MongoClient
import data
from task import Task

client = MongoClient('localhost', 27017)
db = client['aermod']


def parse_start_log():
    # servers = collections.defaultdict(list)
    with open(Task.TASK_LOG_FILE, 'r') as start_log:
        for line in start_log:
            host, task_name = line.rstrip().split(':')
            if host == 'localhost':
                host = 'sheet20'
            # servers[host].append(task_name)
            db.tasks.update({'name': task_name.lstrip()}, {'$set': {'server' : host}})

def parse_running():
    servers = data.get_all_servers()
    for server in servers:
        server.report(detail=True)

def parse_output():
    path = '/net/sht14/kun/output/'
    outputs = os.listdir(path)
    output_set = set(outputs)
    for output in outputs:
        task_name, suffix = output.split('.')
        if suffix == 'out':
            if task_name+'.log' not in output_set:
                print task_name + 'no log file'
        elif suffix == 'log':
            if task_name+'.out' not in output_set:
                print task_name + 'no out file'
                continue
            # if os.path.getsize(path+task_name+'.out') < 40000000L:
                # print task_name + 'out file too small'
            err, stime, etime = parse_run_log(path+output)
            if err:
                state = Task.STATE_ERROR
            else:
                state = Task.STATE_OK
            # print state, stime, etime
            db.tasks.update({'name': task_name}, {'$set': {'state' : state, 'start_at': stime, 'end_at': etime}})

def parse_server():
    path = '/net/sht14/kun/output/'
    outputs = os.listdir(path)
    output_set = set(outputs)
    for output in outputs:
        task_name, suffix = output.split('.')
        if suffix == 'log':
            with open(path+output, 'r') as log:
                for line in log:
                    if line.startswith('2017'):
                        t = db.tasks.find({'name': task_name})[0]
                        server = t['server'].encode()
                        if len(server) == 0:
                            print task_name
                            db.tasks.update({'name': task_name}, {'$set': {'server': 'sheet21'}})
                        break


def parse_run_log(file):
    # english right
    file1 = '/net/sht14/kun/output/CO_0703_07.log'
    # chinese right
    file2 = '/net/sht14/kun/output/PM_0703_03.log'

    # error case 1
    file3 = '/net/sht14/kun/output/CO_0103_10.log'
    # error case 2
    file4 = '/net/sht14/kun/output/NO2_0103_01.log'

    # incomplete
    file5 = '/net/sht14/kun/output/CO_0103_23.log'
    file6 = '/net/sht14/kun/output/BC_0103_10.log'

    start_time = None
    end_time = None
    has_error = False
    with open(file, 'r') as log:
        for line in log:
            if line.endswith('start!\n'):
                start_time = line[:-10]
            elif line.endswith('end!\n'):
                end_time = line[:-8]
            elif line.lower().find('error')>0:
                has_error = True
    # print start_time
    start_time = parse_time(start_time)
    if end_time:
        # print end_time
        end_time = parse_time(end_time)
        delta = end_time - start_time
        # print delta
        # print has_error
    return has_error, start_time, end_time

def parse_time(timestr):
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

if __name__ == '__main__':
    # parse_run_log()
    # parse_running()
    # parse_start_log()
    # parse_output()
    parse_server()
