# coding=utf-8

import threading
import requests
import logging
from flask import Flask, request, render_template, Response, json, jsonify
from context import Context
from monitor import Monitor
import data


log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
api_server = 'http://localhost:3003/'
app = Flask(__name__, template_folder='site/templates', static_folder='site/static')
# static_path=None, static_url_path=None, static_folder=’static’, template_folder=’templates’, instance_path=None, instance_relative_config=False, root_path=None
# app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

def start_server(port=3000):
    db = data.DBMongo()
    db.load_data()
    app.context = Context()
    app.monitor = Monitor()
    app.monitor.start()
    app.run(host='', port=port, threaded=True, debug=False)
    app.monitor.stop()
    app.monitor.join()

# @app.route('/<string:page_name>/')
# def static_page(page_name):
#     return app.send_static_file('../templates/index.html')
#     return render_template('%s.html' % page_name)

@app.route('/')
@app.route('/index.html')
def index():
    return render_template('index.html', title='Home')

@app.route('/servers.html', methods = ['GET'])
def servers_page():
    return render_template('servers.html', title='Servers')

@app.route('/tasks.html', methods = ['GET'])
def tasks_page():
    arg_state = request.args.get('state', 1)
    return render_template('tasks.html', title='Tasks', state=arg_state)

@app.route('/config.html', methods = ['GET'])
def config_page():
    return render_template('config.html', title='Config')

@app.route('/state', methods = ['GET'])
def get_state():
    # data = requests.get(api_server+'state').json()
    data = {'data' : app.context.report()}
    # js = json.dumps(data, indent=4)
    # resp = Response(js, status=200, mimetype='application/json')
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/servers', methods = ['GET'])
def get_servers():
    # data = requests.get(api_server+'servers').json()
    data = {'data' : [s.report() for s in app.context.servers]}
    resp = jsonify(data)
    return resp

@app.route('/server/<host>', methods = ['GET'])
def get_server_by_host(host):
    # data = requests.get(api_server+'server/'+host).json()
    server = app.context.get_server(host)
    data = {'data' : server.report(with_task=True)}
    # resp = jsonify(data)
    # return resp
    return render_template('server.html', title=host, server=data['data'])

@app.route('/tasks', methods = ['GET'])
def get_tasks():
    arg_state = request.args.get('state', 1, int)
    if arg_state==1:
        running = []
        for s in app.context.servers:
            running.extend(s.state.running)
        data = {'data' : [t.report() for t in running]}
        # data = requests.get(api_server+'running').json()
    elif arg_state==0:
        data = {'data' : [t.report() for t in app.context.waiting]}
        # data = requests.get(api_server+'waiting').json()
    else:
        data = {'data' : [t.report() for t in app.context.tasks]}
        # data = requests.get(api_server+'taskall').json()
    resp = jsonify(data)
    return resp

@app.route('/task/<name>/<action>', methods = ['PUT'])
def take_action(name, action):
    resp = jsonify(name + action)
    # app.monitor.stop_task()
    task = app.context.get_task(name)
    code = task.stop()
    resp = jsonify(code)
    return resp

if __name__ == "__main__":
    start_server(3000)
