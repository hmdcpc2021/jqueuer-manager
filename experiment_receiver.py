import json
import time
import ast
import random
import urllib.parse

from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from pprint import pprint

from parameters import backend_experiment_db
from experiment import Experiment


def add_experiment(experiment_json):
    """ Add an experiment """
    private_id = (
        str(int(round(time.time() * 1000))) + "_" + str(random.randrange(100, 999))
    )
    experiment_id = "exp_" + private_id

    experiment = Experiment(experiment_id, private_id, experiment_json)
    experiment_thread = Thread(target=experiment.start, args=())
    experiment_thread.start()

    experiments[experiment_id] = {"experiment": experiment, "thread": experiment_thread}
    return str(experiment_id) + " has been added & started successfully ! \n"


def del_experiment(experiment_json):
    """ Delete an experiment """
    service_name = experiment_json["service_name"]
    if backend_experiment_db.exists(service_name):
        backend_experiment_db.delete(service_name)
        return "Service {} removed from queue".format(service_name)
    return "Service {} not found in queue".format(service_name)


class HTTP(BaseHTTPRequestHandler):
    """ HTTP class
    Serve HTTP
    """

    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_GET(self):
        # Processing GET requests
        try:
            html_file = open("./index.html", "rb")
            response = html_file.read()
            html_file.close()
            self._set_headers()
            self.wfile.write(response)
            return
        except Exception as e:
            pass

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        # Processing POST requests
        content_length = None
        data_json = None
        data = None
        try:
            content_length = int(
                self.headers["Content-Length"]
            )  # <--- Gets the size of data
            data = self.rfile.read(int(content_length)).decode("utf-8")
            data_json = ast.literal_eval(data)
            pass
        except Exception as e:
            print("Error in parsing the content_length and packet data")
        data_back = ""

        if self.path == "/experiment/result":

            html_file = open("./" + data_json["id"] + ".html", "a")
            text = "<hr>Received from {} at {}: Params: {} ".format(
                str(self.client_address), str(time.time()), str(data_json)
            )
            html_file.write(text)
            html_file.close()
            data_back = "received"
        if self.path == "/experiment/add":
            data_back = add_experiment(data_json)
        elif self.path == "/experiment/del":
            data_back = del_experiment(data_json)

        self._set_headers()
        self.wfile.write(bytes(str(data_back), "utf-8"))


def start(experiments_arg, port=8081):
    """ Start the REST API """
    global experiments
    experiments = experiments_arg
    server_address = ("", port)
    httpd = HTTPServer(server_address, HTTP)
    print("Starting Experiment Manager HTTP Server..." + str(port))

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("***** Error in Experiment Manager HTTP Server *****")
        pass

    httpd.server_close()
    print(
        time.asctime(),
        "Experiment Manager Server Stopped - %s:%s" % (server_address, port),
    )
