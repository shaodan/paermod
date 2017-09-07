#coding: utf-8

import subprocess
import datetime
import socket
import sys
import os
import paramiko
import shutil
from task import Task
from context import Context


context = Context()

class Server(object):
    
    def __init__(self, host, workspace=None, weight=1):
        self.host = host
        self.workspace = workspace
        self.weight = weight
        self.available = True
        self._ssh = None
        self.setup()
        self.tasks = {}
        self.state = ServerState(self)
        self.updated = False
        self.system = ''

    def setup(self):
        # establish ssh connection
        self.connect()
        # check work dir
        if self.workspace is None:
            print 'server %s does NOT have workspace' % self.host
            return
        try:
            if not os.path.isdir(self.workspace):
                os.mkdir(self.workspace)
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
            self.available = False
            print(self.host + " connecting fail...")
            print(e.message)

    def is_connected(self):
        transport = self._ssh.get_transport() if self._ssh else False
        return transport and transport.is_active()

    def clean(self):
        if self.workspace is None:
            return
        dirs = os.listdir(self.workspace)
        running_tasks = set([t.name for t in self.state.running])
        for name in dirs:
            task = context.get_task(name)
            if task:
                #TODO: if task is waiting/restarting
                if name not in running_tasks:
                    task.clean()
            else:
                print 'clean task : %s%s' % (self.workspace, name)
                shutil.rmtree(self.workspace+name)

    def shutdown(self):
        # shutil.rmtree(self.workspace)
        self._ssh.close()

    def monitor(self):
        self.state.update()
        # for task_name in self.state.running:
        #     pass
        # context.get_task(task_name).register(self) for task_name in self.state.state.running

    def report(self, with_task=False):
        data = { 
            'host'         : self.host,
            'cores'        : self.state.cores,
            'cpu_load'     : '%.2f%%' % self.state.cpu,
            'tasks_count'  : len(self.state.running),
            'tasks'        : map(lambda t:t.name, self.state.running),
            'last_update'  : str(self.state.last_update)[:19],
            'system'       : self.system
        }
        if with_task:
            data['running'] = [t.report() for t in self.state.running]
        return data

    def add_task(self, task):
        if task.name not in self.tasks:
            self.tasks[task.name] = task

    def copy(self, source, target):
        if not os.path.exists(source) or os.path.exists(target):
            return False
        if self.is_remote_dir(target):
            # NFS copy
            shutil.copy2(source, target)
        else:
            # SCP copy
            self.sftp = self._ssh.open_sftp()
            self.sftp.put(source, target)
        # todo: check copy sucess
        if state:
            raise IOError('copy file error')
            
    def is_remote_dir(self, path):
        return path.find(':') > 0

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
        
    def kill_all_task(self):
        return self.run(ServerState.kill_aermods)
    
    def kill_by_pid(self):
        return self.run(ServerState.kill_pid+str(pid))

        
class LocalServer(Server):

    def __init__(self, host, workspace=None, weight=1):
        super(LocalServer, self).__init__(host, workspace, weight)
        self.py_version = sys.version_info

    def connect(self):
        pass

    def run(self, command, wait=True):
        out = ''
        if self.py_version < (2, 4):
            raise "must use python 2.5 or greater"
        elif self.py_version < (2, 7):
            # python 2.4-2.6
            p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if wait:
                out, err = p.communicate()
        else:
            # python 2.7 or python 3
            p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            if wait:
                out = p.stdout.read()
        return out.rstrip()

    def run_bg(self, command):
        # command = "nohup " + command + " &"
        self.run(command, wait=False)
        # os.system(command)
        
        
class ServerFactory(object):
    
    def createServer(self, c):
        if isinstance(c, dict):
            host = c['host'].encode()
            workspace = c['workspace'].encode()
            weight = c['weight']
        elif isinstance(c, tuple):
            host, workspace, weight = c
        if host == context.Master:
            return LocalServer(host, workspace, weight)
        return Server(host, workspace, weight)


class ServerState(object):
    core_command = "grep -c ^processor /proc/cpuinfo"
    time_command = "date"
    sys_command = 'lsb_release -d'
    cpu_command = "mpstat 2 1 | awk 'NR==5 {print $3}'"
    mem_command = "free -m | awk 'NR==2{print $4, $2}'"
    disk_command = "df -h %s | awk '{print $4, $2, $5}'"
    # task_command = "armids=$(pgrep aermod);pwdx $armids;ps -o pid,lstart,etime $armids | awk 'NR>1'"
    # task_command = "armids=$(pgrep aermod); paste <(pwdx $armids) <(ps -o pid,lstart,etime $armids|awk 'NR>1')"
    pwdx_command = "pwdx $(pgrep aermod)"
    pime_command = "ps -o pid,lstart,etime $(pgrep aermod) | awk 'NR>1'"
    task_time = "ps -o lstart,etime %s | awk 'NR>1'"

    kill_run_sh = "kill -9 $(ps -o ppid $(pgrep aermod)| awk 'NR>1')"
    kill_aermods = "killall -9 aermod"
    kill_pid = 'kill -9 '


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
        self.server.updated = False
        master_time = datetime.datetime.now()
        if self.last_update is None:
            results = self.server.run_batch(ServerState.core_command, ServerState.time_command, ServerState.sys_command)
            self.cores = int(results[0])
            local_time = context.parse_time(results[1])
            self.server.system = results[2][13:]
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
            # task_name = path[-10:]
            task_name = path.split('/')[-1]
            task = context.get_task(task_name)
            task.pid = int(pid)
            self.running.append(task)

            if task.state==Task.STATE_NEW: # or task.server != self.server:
                result = self.server.run(ServerState.task_time % pid)
                timestr = ' '.join(result.split()[:-1])
                stime = datetime.datetime.strptime(timestr, '%a %b %d %H:%M:%S %Y')
                # task.pid = int(pid)
                task.register(self.server)
                task.state = Task.STATE_RUNNING
                task.start_time = self.time_delta + stime
                task.save()
            task.run_time = master_time - task.start_time
            # psid = psid.split()
            # timestr = ' '.join(psid[1:-1])
            # stime = datetime.datetime.strptime(timestr, '%a %b %d %H:%M:%S %Y')
            # day, hms = psid[-1].split('-')
            # h,m,s=hms.split(':')
            # task.run_time = datetime.timedelta(days=int(day), hours=int(h), minutes=int(m), seconds=int(s))
        self.server.updated = True

if __name__ == '__main__':
    server = Server('sheet19', '/net/19/kun/')
    print server.report()
