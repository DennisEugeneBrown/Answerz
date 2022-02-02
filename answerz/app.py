import json
import webbrowser
from threading import Timer
from flask_restful import Api
from flask import send_from_directory

from answerz import api

app = api.create_app()

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


from answerz.api.Answerz import Answerz

api.add_resource(Answerz, '/answerz')


def open_browser():
    webbrowser.open_new('http://127.0.0.1:1234')


# Timer(1, open_browser).start()
