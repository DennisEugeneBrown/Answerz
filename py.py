import os
from json2html import *
from flask import Flask, request
from answerz_server import AnswerzProcessor

app = Flask(__name__)
app.config.from_json('config.json')

ap = AnswerzProcessor(
    app.config['DATAMAP'], app.config['DB'], app.config['LUIS'])

main_html = '<form method="POST">\
            <input name = "text" style="width: 600px" placeholder="{}">\
            <input type = "submit" >\
            </form >'


@app.route('/')
def my_form():
    return main_html.format('')


@app.route('/', methods=['POST'])
def my_form_post():
    text = request.form['text']
    print(text)
    result, sql = ap.run_query(text)
    result = json2html.convert(json=result)
    return main_html.format(text) + '<br>' + result + '<br><br>' + sql


if __name__ == '__main__':
    app.run(host='0.0.0.0')
