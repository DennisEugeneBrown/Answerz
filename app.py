import json
from flask import Flask, send_from_directory
from flask_restful import Api, Resource, reqparse
from flask_cors import CORS  # comment this on deployment
from threading import Timer
import webbrowser

app = Flask(__name__, static_url_path='', static_folder='frontend/build')
app.config.from_json('config.json')

CORS(app)  # comment this on deployment
api = Api(app)


@app.route("/", defaults={'path': ''})
def serve(path):
    print(app.static_folder)
    return send_from_directory(app.static_folder, 'index.html')


@app.route("/columns", defaults={'path': ''})
def get_columns(path):
    columns = []
    for dim in app.config['DATAMAP']['calls']['Dimensions']:
        if isinstance(dim['field'], str):
            columns.append(dim['field'].title())
        else:
            columns.extend([val.title() for val in dim['field'].values()])
    return json.dumps(list(set(columns)))


@app.route("/tables", defaults={'path': ''})
def get_tables(path):
    tables = [table.title() for table in app.config['DATAMAP'].keys()]
    return json.dumps(list(set(tables)))


from api.Answerz import Answerz

api.add_resource(Answerz, '/answerz')


def open_browser():
    webbrowser.open_new('http://127.0.0.1:1234')

Timer(1, open_browser).start()
