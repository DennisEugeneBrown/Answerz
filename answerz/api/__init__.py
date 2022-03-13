import os
from flask import Flask
from flask_cors import CORS  # comment this on deployment


def create_app():
    app = Flask(__name__, static_url_path='', static_folder=os.path.join('../view_OLD/build'))
    app.config.from_json('../config.json')

    CORS(app)  # comment this on deployment

    return app
