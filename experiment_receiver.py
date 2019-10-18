import json
import time
import ast
import random
import urllib.parse
import docker

import subprocess
import monitoring
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from pprint import pprint

from parameters import backend_experiment_db, JOB_QUEUE_PREFIX, pushgateway_service_name
from experiment import Experiment


def add_experiment(experiment_json):
    """ Add an experiment """
    private_id = (
        str(int(round(time.time() * 1000))) + "_" + str(random.randrange(100, 999))
    )
    experiment_id = "exp_" + private_id
    if backend_experiment_db.exists(experiment_json["container_name"]):
        return "This container already has an experiment assigned to it - please delete first"

    experiment = Experiment(experiment_id, experiment_json)
    experiment_thread = Thread(target=experiment.start, args=())
    experiment_thread.start()

    experiments[experiment_id] = {"experiment": experiment, "thread": experiment_thread}
    return str(experiment_id) + " has been added & started successfully ! \n"


def del_experiment(delete_form):
    """ Delete an experiment """
    service_name = delete_form.get("container")
    try:
        subprocess.run(
            [
                "celery",
                "-A",
                "job_manager",
                "amqp",
                "queue.purge",
                JOB_QUEUE_PREFIX + service_name,
            ]
        )
    except Exception as e:
        print(e)
    if backend_experiment_db.exists(service_name):
        backend_experiment_db.delete(service_name)
        # Restart Pushgateway container
        client = None
        try:
            client = docker.from_env()
            for container in client.containers.list():
                container_service_name = (
                    container.labels.get("io.kubernetes.container.name")
                    or container.name
                )
                if pushgateway_service_name.lower() in container_service_name.lower():
                    container.restart()
        except Exception:
            return "Could not connect to Docker at /var/run/docker.sock"
	    # --------------
        return "Service {} removed from backend".format(service_name)
    return "Service {} not found in queue".format(service_name)

def record_worker_metrics(metric_info):
    """ Record metric received from worker """
    metric_type = metric_info["metric_type"]
    labels = metric_info["labels"]
    print(metric_type)
    print(labels)
    data_back = "Metric of type {} is received and recorded".format(metric_type)
    if metric_type.lower() == "add_worker":
        monitoring.add_worker(labels["node_id"],labels["experiment_id"],labels["service_name"])
    elif metric_type.lower() == "terminate_worker":
        monitoring.terminate_worker(labels["node_id"],labels["experiment_id"],labels["service_name"])
    elif metric_type.lower() == "run_job":
        monitoring.run_job(labels["node_id"],labels["experiment_id"],labels["service_name"],labels["qworker_id"],labels["job_id"])
    elif metric_type.lower() == "terminate_job":
        monitoring.terminate_job(labels["node_id"],labels["experiment_id"],labels["service_name"],labels["qworker_id"],labels["job_id"],labels["start_time"])
    elif metric_type.lower() == "job_failed":
        monitoring.job_failed(labels["node_id"],labels["experiment_id"],labels["service_name"],labels["qworker_id"],labels["job_id"],labels["fail_time"])
    elif metric_type.lower() == "run_task":
        monitoring.run_task(labels["node_id"],labels["experiment_id"],labels["service_name"],labels["qworker_id"],labels["job_id"],labels["task_id"])
    elif metric_type.lower() == "terminate_task":
        monitoring.terminate_task(labels["node_id"],labels["experiment_id"],labels["service_name"],labels["qworker_id"],labels["job_id"],labels["task_id"],labels["start_time"])
    elif metric_type.lower() == "task_failed":
        monitoring.task_failed(labels["node_id"],labels["experiment_id"],labels["service_name"],labels["qworker_id"],labels["job_id"],labels["task_id"],labels["fail_time"])
    else:
        data_back ="The metric of type {} didn't match with any known metric types".format(metric_type)
    return data_back

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
        elif self.path == "/experiment/metrics":
            data_back = record_worker_metrics(data_json)

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
