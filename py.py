import os
from flask import Flask, request
from answerz_server import AnswerzProcessor

app = Flask(__name__)
app.config.from_json('config.json')


@app.route('/')
def my_form():
    return '<form method="POST">\
            <input name = "text" >\
            <input type = "submit" >\
            </form >'


@app.route('/', methods=['POST'])
def my_form_post():
    text = request.form['text']
    ap = AnswerzProcessor(
        app.config['DATAMAP'], app.config['DB'], app.config['LUIS'])
    result, _ = ap.run_query(text)
    return result


if __name__ == '__main__':
    app.run(host='0.0.0.0')
