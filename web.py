# coding:utf-8

import SimpleHTTPServer
import SocketServer
import BaseHTTPServer
import socket
import cgi
import sys
from os import curdir, sep
import json
from server import Server, LocalServer
from task import Task
from context import Context
from monitor import Monitor
import data


'''
reference article:
http://pymotwcn.readthedocs.io/en/latest/documents/BaseHTTPServer.html
'''
class MyHandler(SimpleHTTPServer.SimpleHTTPRequestHandler, object):

    def do_GET(self):
        # print self.headers
            # print '='*10
        context = Context()
        if self.path.startswith("/state"):
            output = {'data' : context.report()}
            self.jsonify(output)
        elif self.path.startswith("/servers") and not self.path.startswith("/servers."):
            output = {'data' : [s.report() for s in context.servers]}
            self.jsonify(output)
        elif self.path.startswith("/running"):
            running = []
            for s in context.servers:
                running.extend(s.state.running)
            output = {'data' : [t.report() for t in running]}
            self.jsonify(output)
        elif self.path.startswith("/server/"):
            host = self.path[8:]
            server = context.server_table[host]
            output = {'data' : server.report(with_task=True)}
            self.jsonify(output)
        elif self.path.startswith('/action/'):
            action = self.path[8:]
            params = self.path.split('?')[1].split('&')
            params = dict(pair.split('=') for pair in params)
            print action
            print params
            return self.ok()
            # start, kill, download, upload
            actions = {'start': self.start_task, 'kill': self.kill_task}
            if action in actions:
                func = actions[action]
                func(params)
        else:
            return super(MyHandler, self).do_GET()

    def start_task(self, args):
        task = context.get_task(args['task'])
        server = context.get_server(args['server'])
        task.register(server).start()
        return self.ok()

    def kill_task(self, args):
        task = context.get_task(args['task'])
        code = task.kill()
        if code:
            return self.ok()
        return self.error()

    def route(self):
        return {
            '/servers'      : self.get_servers,
            '/server/{host}': self.get_server_by_host,
            '/tasks'        : self.get_tasks,
            '/task/{name}'  : self.get_task_by_name,
            '/'             : None,
        }[self.path](args)

    def jsonify(self, ouput):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.wfile.write("\r\n")
        output = json.dumps(ouput, indent=4)
        self.wfile.write(output)

    def ok(self, message=None):
        self.send_response(200)
        if message:
            self.wfile.write("\r\n")
            self.wfile.write('ok ' + message)

    def error(self, err_code, message=None):
        self.send_error(err_code, message)
        
    # def log_message(self, format, *args):
    #     sys.stderr.write("%s - - [%s] %s\n" %
    #                  (self.address_string(),
    #                   self.log_date_time_string(),
    #                   format%args))
        
    def log_request(self, code='-', size='-'):
        # quite request log
        return
        self.log_message('"%s" %s %s', self.requestline, str(code), str(size))

    # def log_error(self, format, *args):
    #     self.log_message(format, *args)


class ThreadedHTTPServer(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):
    """Handle requests in a separate thread."""
    allow_reuse_address = True

    def server_bind(self):
         self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
         self.socket.bind(self.server_address)


class WebServer(object):
    def __init__(self, port=8000):
        self.port = port
        self.httpd = ThreadedHTTPServer(("", port), MyHandler)
        data.DBMongo()
        self.context = Context()
        self.m = Monitor()
        self.m.start()

    def start(self):
        try:
            print 'Starting server on port %d, use <Ctrl-C> to stop' % self.port
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            print '^C received, shutting down the web server'
            self.stop()
     
    def stop(self):
        self.httpd.socket.close()
        self.m.stop()


class StaticHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path=="/":
            self.path="/index.html"
        try:
            #Check the file extension required and
            #set the right mime type
            sendReply = False
            if self.path.endswith(".html"):
                mimetype='text/html'
                sendReply = True
            if self.path.endswith(".jpg"):
                mimetype='image/jpg'
                sendReply = True
            if self.path.endswith(".gif"):
                mimetype='image/gif'
                sendReply = True
            if self.path.endswith(".js"):
                mimetype='application/javascript'
                sendReply = True
            if self.path.endswith(".css"):
                mimetype='text/css'
                sendReply = True
            if sendReply == True:
                #Open the static file requested and send it
                f = open(curdir + sep + self.path)
                self.send_response(200)
                self.send_header('Content-type',mimetype)
                self.end_headers()
                self.wfile.write(f.read())
                f.close()
            return
        except IOError:
            self.send_error(404,'File Not Found: %s' % self.path)


class JSONRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.wfile.write("\r\n")
        # if self.path == '/state':
        state  = LocalServer('/net/20/kun/AERMOD/').get_state()
        output = json.dumps(state)
        self.wfile.write(output)

    def do_POST(self):
        if self.path == "/success":
            response_code = 200
        elif self.path == "/error":
            response_code = 500
        else:
            try:
                response_code = int(self.path[1:])
            except Exception:
                response_code = 201

        try:
            self.send_response(response_code)
            self.wfile.write('Content-Type: application/json\r\n')
            self.wfile.write('Client: %s\r\n' % str(self.client_address))
            self.wfile.write('User-agent: %s\r\n' % str(self.headers['user-agent']))
            self.wfile.write('Path: %s\r\n' % self.path)
            self.end_headers()

            form = cgi.FieldStorage(
                    fp=self.rfile,
                    headers=self.headers,
                    environ={'REQUEST_METHOD':'POST','CONTENT_TYPE':self.headers['Content-Type'],})
            self.wfile.write('{\n')
            first_key=True
            for field in form.keys():
                    if not first_key:
                        self.wfile.write(',\n')
                    else:
                        self.wfile.write('\n')
                        first_key=False
                    self.wfile.write('"%s":"%s"' % (field, form[field].value))
            self.wfile.write('\n}')

        except Exception as e:
            self.send_response(500)

if __name__ == '__main__':
    web_server = WebServer(3003)
    web_server.start()
