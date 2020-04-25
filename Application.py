from flask import Flask, send_file, jsonify  # import flask
from datetime import timedelta
from flask import Flask, make_response, request, current_app
from functools import update_wrapper
# from flask_cors import CORS

from AnalysisQuery1 import query1_output, query1_plot
from AnalysisQuery10 import query10_output, query10_plot
from AnalysisQuery11 import query11_output, query11_plot
from AnalysisQuery12 import query12_output, query12_plot
from AnalysisQuery2 import query2_output, query2_plot
from AnalysisQuery3 import query3_output, query3_plot
from AnalysisQuery4 import query4_output, query4_plot
from AnalysisQuery5 import query5_output, query5_plot
from AnalysisQuery6 import query6_output, query6_plot
from AnalysisQuery7 import query7_output, query7_plot
from AnalysisQuery8 import query8_output, query8_plot
from AnalysisQuery9 import query9_plot, query9_output
import os.path
from os import path

from googledrive import download_file_from_google_drive

app = Flask(__name__)  # create an app instance

# cors = CORS(app)

def crossdomain(origin=None, methods=None, headers=None, max_age=21600, attach_to_all=True, automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, str):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, str):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)

    return decorator


@app.route("/")
@crossdomain(origin='*')  # at the end point /
def hello():  # call method hello
    return "Hello World!"  # which returns "hello world"


@app.route('/api/TweetsAnalysis/query1Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry1():
    return query1_output()


@app.route('/api/TweetsAnalysis/query1Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry1():
    image_obj = query1_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query2Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry2():
    return query2_output()


@app.route('/api/TweetsAnalysis/query2Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry2():
    image_obj = query2_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query3Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry3():
    return query3_output()


@app.route('/api/TweetsAnalysis/query3Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry3():
    image_obj = query3_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query4Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry4():
    return query4_output()


@app.route('/api/TweetsAnalysis/query4Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry4():
    image_obj = query4_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query5Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry5():
    return query5_output()


@app.route('/api/TweetsAnalysis/query5Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry5():
    image_obj = query5_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query6Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry6():
    return query6_output()


@app.route('/api/TweetsAnalysis/query6Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry6():
    image_obj = query6_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query7Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry7():
    return query7_output()


@app.route('/api/TweetsAnalysis/query7Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry7():
    image_obj = query7_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query8Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry8():
    return query8_output()


@app.route('/api/TweetsAnalysis/query8Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry8():
    image_obj = query8_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query9Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry9():
    return query9_output()


@app.route('/api/TweetsAnalysis/query9Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry9():
    image_obj = query9_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query10Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry10():
    return query10_output()


@app.route('/api/TweetsAnalysis/query10Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry10():
    image_obj = query10_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query11Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry11():
    return query11_output()


@app.route('/api/TweetsAnalysis/query11Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry11():
    image_obj = query11_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


@app.route('/api/TweetsAnalysis/query12Output', methods=['GET'])
@crossdomain(origin='*')
def output_quiry12():
    return query12_output()

@app.route('/api/TweetsAnalysis/query12Plot', methods=['GET'])
@crossdomain(origin='*')
def plot_quiry12():
    image_obj = query12_plot()
    return send_file(image_obj,
                     attachment_filename='plot.png',
                     mimetype='image/png')


if __name__ == "__main__":  # on running python app.py
    app.run()  # run the flask app
    if not path.exists("tweetsfile.json"):
        print("file not exist..");
        file_id = 'tweets.json'
        destination = 'tweetsfile.json'
        download_file_from_google_drive(file_id, destination)
