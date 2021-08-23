import os
import sys
from json2html import *
from flask import Flask, request
from answerz_server import AnswerzProcessor

app = Flask(__name__, instance_path="/home/yuzo-san/IdeaProjects/Answerz/instance")
app.config.from_json('config.json')

ap = AnswerzProcessor(
    app.config['DATAMAP'], app.config['DB'], app.config['LUIS'])

main_html = '<form method="POST">\
            <input name = "text" style="width: 600px" placeholder="{}">\
            <input value = "Submit Query" type = "submit" name="submit_button">\
            </form >'


@app.route('/')
def my_form():
    return main_html.format('')


@app.route('/', methods=['POST'])
def my_form_post():
    text = request.form['text']
    # action = request.form['action']
    # if action == 'Submit Query':
    print(request.form['submit_button'])
    if request.form['submit_button'] == 'Submit Query':
        # result, sql = ap.run_query(text)
        results = ap.run_query(text)
        html = main_html.format(text)
        sql_lower = []
        for result, sql in results:
            if sql.lower() in sql_lower:
                continue
            sql_lower.append(sql.lower())
            result = json2html.convert(json=result)
            button = '<form method="POST">\
                <input value="{}" type="submit" name="submit_button">\
                </form>'.format(sql)
            html += '<br>' + sql + '<br><br>' + result + '<br><br>'
        return html
    else:
        return ''


if __name__ == '__main__':
    if len(sys.argv) > 1:
        app.run(host=sys.argv[1])
    else:
        app.run(host='127.0.0.1', port=5555)
