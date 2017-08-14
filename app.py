# coding=utf-8

import threading
import requests
from flask import Flask, request, render_template, Response, json, jsonify
from context import Context
from monitor import Monitor
import data


app = Flask(__name__, template_folder='site/templates', static_folder='site/static')
# static_path=None, static_url_path=None, static_folder=’static’, template_folder=’templates’, instance_path=None, instance_relative_config=False, root_path=None
# app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

def start_server(port=8000):
    # db = data.DBMongo()
    # app.context = Context()
    # app.monitor = Monitor()
    # app.monitor.start()
    app.run(host='', port=port, threaded=False, debug=True)
    # app.monitor.stop()

# @app.route('/<string:page_name>/')
# def static_page(page_name):
#     return render_template('%s.html' % page_name)

@app.route('/')
@app.route('/index.html')
def index():
    # return app.send_static_file('../templates/index.html')
    return render_template('index.html', title='Home')

@app.route('/state', methods = ['GET'])
def get_state():
    # data = {'data' : app.context.report()}
    data = requests.get('http://localhost:3003/state').json()
    # js = json.dumps(data, indent=4)
    # resp = Response(js, status=200, mimetype='application/json')
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/servers.html', methods = ['GET'])
def servers_page():
    return render_template('servers.html', title='Servers')

@app.route('/servers', methods = ['GET'])
def get_servers():
    data = {'data' : [s.report() for s in app.context.servers]}
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/server/<host>', methods = ['GET'])
def get_server_by_host(host):
    server = app.context.get_server(host)
    data = {'data' : server.report()}
    resp = jsonify(data)
    return resp

@app.route('/tasks.html', methods = ['GET'])
def tasks_page():
    return render_template('tasks.html', title='Tasks')

@app.route('/tasks', methods = ['GET'])
def get_tasks():
    task_filter = request.args.get('state', 1)
    data = {'data' : [t.report() for t in app.context.running]}
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/config.html', methods = ['GET'])
def config_page():
    return render_template('config.html', title='Config')

@app.route('/action/<action>', methods = ['PUT'])
def take_action(action):
    resp = jsonify(action)
    # app.monitor.stop_task()
    return resp

if __name__ == "__main__":
    start_server(3000)