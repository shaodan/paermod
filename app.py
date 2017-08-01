# coding=utf-8

import threading
from flask import Flask, render_template, Response, json, jsonify
from server import Server, LocalServer
from task import Task
from context import Context
from monitor import Monitor
import data


app = Flask(__name__)

def start_server(port=8000):
    # app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
    db = data.DBMongo()
    app.context = Context()
    app.monitor = Monitor()
    app.monitor.start()
    app.run(host='', port=port, threaded=False, debug=True)
    app.monitor.stop()

# @app.route('/<string:page_name>/')
# def static_page(page_name):
#     return render_template('%s.html' % page_name)

@app.route("/")
def index():
    return app.send_static_file('index.html')

@app.route('/state', methods = ['GET'])
def get_state():
    data = {'data' : app.context.report()}
    # js = json.dumps(data, indent=4)
    # resp = Response(js, status=200, mimetype='application/json')
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/servers', methods = ['GET'])
def get_server_lists():
    data = {'data' : [s.report() for s in app.context.servers]}
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/server/<server_name>', methods = ['GET'])
def get_server(server_name):
    return resp

@app.route('/running', methods = ['GET'])
def get_running_tasks():
    data = {'data' : [t.report() for t in app.context.running]}
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

@app.route('/action/', methods = ['POST'])
def do_action():
    resp = ''
    return resp

if __name__ == "__main__":
    start_server(3003)