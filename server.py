#coding: utf-8

import subprocess
import datetime
import socket
import sys
import os
import paramiko
import shutil
from task import Task
from context import AppContext


class Server(object):
    context = AppContext()

    def __init__(self, host, workspace=None):
        self.local = (host == '')
        self.host = host if not self.local else 'sheet20'
        self.workspace = workspace
        self.available = False
        self._ssh = None
        self.setup()
        self.tasks = {}
        self.state = ServerState(self)

    def setup(self):
        # establish ssh connection
        if not self.local:
            self.connect()
        # check work dir
        if self.workspace is None:
            return
        try:
            if not os.path.isdir(self.workspace):
                os.mkdirs(self.workspace)
        except Exception as e:
            print("setup workspace fail: " + self.workspace)
            print(e.message)
            self.workspace = None

    def connect(self):
        if self._ssh is None:
            self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self._ssh.connect(self.host)
            self.available = True
        except Exception as e:
            print(self.host + " connecting fail...")
            print(e.message)

    def is_connected(self):
        transport = self._ssh.get_transport() if self._ssh else None
        return transport and transport.is_active()

    def clean(self):
        if self.workspace is None:
            return
        dirs = os.listdir(self.workspace)
        running_tasks = set([t.name for t in self.state.running])
        for task_name in dirs:
            task = self.context.get_task(task_name)
            if task:
                if task_name not in running_tasks:
                    task.clean()
            else:
                print 'clean : ' + self.workspace + task_name
                shutil.rmtree(self.workspace+task_name)

    def shutdown(self):
        # shutil.rmtree(self.workspace)
        self._ssh.close()

    def monitor(self):
        self.state.update()
        # for task_name in self.state.running:
        #     pass
        # self.context.get_task(task_name).register(self) for task_name in self.state.state.running

    def report(self, with_task=False):
        state = self.state.to_json()
        state['host'] = self.host
        if with_task:
            state['running'] = [t.report() for t in self.state.running]
        return state

    def add_task(self, task):
        if task.name not in self.tasks:
            self.tasks[task.name] = task

    def copy(self, source, target):
        if not os.path.exists(source) or os.path.exists(target):
            return False
        if self.local:
            # NFS copy
            shutil.copy2(source, target)
        else:
            # SCP copy
            self.sftp = self._ssh.open_sftp()
            self.sftp.put(source, target)
        # todo: check copy sucess
        if state:
            raise IOError('copy file error')

    def run(self, command):
        if not self.is_connected():
            self.connect()
        try:
            i, o, e = self._ssh.exec_command(command)
            err = e.read()
            if err != '' and err.rstrip() != "Usage: pwdx pid...":
                print(self.host +": command-"+ command + " error-" + err)
            output = o.read()
        except socket.error as e:
            # Crap, it's closed. Perhaps reopen and retry?
            print(self.host + " disconnected")
            print(e.message)
            print(self.host + " try to reconnect...")
            try:
                self._ssh.connect(self.host)
            except Exception as e:
                print("reconnect fail...")
                return
        return output.rstrip()

    def run_batch(self, *args):
        command = ';'.join(args)
        output = self.run(command)
        return output.split('\n')

    def run_bg(self, command):
        if not self.is_connected():
            self.connect()
        tran = self._ssh.get_transport()
        chan = tran.open_session()
        # stdout write to buffer file
        # f = chan.makefile()
        chan.exec_command(command)
        # f.read()
        # f.close()

class LocalServer(Server):

    def __init__(self, workspace=None):
        super(LocalServer, self).__init__('', workspace)

    def setup(self):
        self.py_version = sys.version_info

    def run(self, command):
        if self.py_version < (2, 4):
            raise "must use python 2.5 or greater"
        elif self.py_version < (2, 7):
            # python 2.4-2.6
            p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
        else:
            # python 2.7 or python 3
            p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out = p.stdout.read()
        return out.rstrip()

    def run_bg(self, command):
        # command = "nohup " + command + " &"
        # self.run(command)
        os.system(command)


class ServerState(object):
    core_command = "grep -c ^processor /proc/cpuinfo"
    time_command = "date"
    cpu_command = "mpstat 2 1 | awk 'NR==5 {print $3}'"
    mem_command = "free -m | awk 'NR==2{print $4, $2}'"
    disk_command = "df -h %s | awk '{print $4, $2, $5}'"
    # task_command = "armids=$(pgrep aermod);pwdx $armids;ps -o pid,lstart,etime $armids | awk 'NR>1'"
    # task_command = "armids=$(pgrep aermod); paste <(pwdx $armids) <(ps -o pid,lstart,etime $armids|awk 'NR>1')"
    pwdx_command = "pwdx $(pgrep aermod)"
    pime_command = "ps -o pid,lstart,etime $(pgrep aermod) | awk 'NR>1'"
    task_time = "ps -o lstart,etime %s | awk 'NR>1'"

    kill_run_sh = "kill -9 $(ps -o ppid $(pgrep aermod)| awk 'NR>1')"
    kill_aermod = "kill -9 $(pgrep aermod)"

    def __init__(self, server):
        self.server = server
        self.cores = None
        self.cpu = 0.0
        self.mem = 0.0
        self.disk = 0.0  # free disk space in G
        self.running = []
        self.last_update = None
        self.time_delta = None

    def update(self):
        master_time = datetime.datetime.now()
        if self.last_update is None:
            results = self.server.run_batch(ServerState.core_command, ServerState.time_command)
            self.cores = int(results[0])
            local_time = self.server.context.parse_time(results[1])
            self.time_delta = master_time - local_time
        self.last_update = master_time
        results = self.server.run_batch(ServerState.cpu_command, ServerState.pwdx_command)
        # self.mem = self.server.run(ServerState.mem_command)
        # self.disk = self.server.run(ServerState.disk_command % (self.server.workspace))

        self.cpu = float(results[0])
        # task_count = len(results)/2
        # pwdxs = results[1:task_count+1]
        # psids = results[task_count+1:]
        # for pwdx, psid in zip(pwdxs, psids):
        self.running = []
        for pwdx in results[1:]:
            pid, path = pwdx.split(':')
            task_name = path[-10:]
            task = self.server.context.get_task(task_name)
            self.running.append(task)

            if task.state==Task.STATE_NEW: # or task.server != self.server:
                result = self.server.run(ServerState.task_time % pid)
                timestr = ' '.join(result.split()[:-1])
                stime = datetime.datetime.strptime(timestr, '%a %b %d %H:%M:%S %Y')
                task.pid = int(pid)
                task.register(self.server)
                task.state = Task.STATE_RUNNIG
                task.start_time = self.time_delta + stime
                task.save()
            task.run_time = master_time - task.start_time
            # psid = psid.split()
            # timestr = ' '.join(psid[1:-1])
            # stime = datetime.datetime.strptime(timestr, '%a %b %d %H:%M:%S %Y')
            # day, hms = psid[-1].split('-')
            # h,m,s=hms.split(':')
            # task.run_time = datetime.timedelta(days=int(day), hours=int(h), minutes=int(m), seconds=int(s))



    def to_json(self):
        if not self.last_update:
            pass
            #self.update()
        return { 'cores'        : self.cores,
                 'cpu_load'     : '%.2f%%' % self.cpu,
                 'tasks_count'  : len(self.running),
                 'tasks'        : map(lambda t:t.name, self.running),
                 'last_update'  : str(self.last_update)
               }


if __name__ == '__main__':
    server = Server('sheet19', '/net/19/kun/')
    print server.report()
