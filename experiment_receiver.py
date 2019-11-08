import json
import time
import ast
import random
import urllib.parse
import docker
import logging

import subprocess
import monitoring
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from pprint import pprint

from parameters import backend_experiment_db, JOB_QUEUE_PREFIX, pushgateway_service_name
from experiment import Experiment

num_nodes_to_scale_down = 0
logger = logging.getLogger(__name__)

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
    global num_nodes_to_scale_down
    """ Record metric received from worker """
    metric_type = metric_info["metric_type"]
    logger.info("Inside record_worker_metrics: The value of num_nodes_to_scale_down is: {0}".format(num_nodes_to_scale_down))
    data_back = "Metric of type {0} is received and recorded".format(metric_type)
    if metric_type.lower() == "add_worker":
        monitoring.add_worker(metric_info["node_id"],metric_info["experiment_id"],metric_info["service_name"])
    elif metric_type.lower() == "terminate_worker":
        monitoring.terminate_worker(metric_info["node_id"],metric_info["experiment_id"],metric_info["service_name"])
    elif metric_type.lower() == "run_job":
        monitoring.run_job(metric_info["node_id"],metric_info["experiment_id"],metric_info["service_name"],metric_info["qworker_id"],metric_info["job_id"])
    elif metric_type.lower() == "terminate_job":
        monitoring.terminate_job(metric_info["node_id"],metric_info["experiment_id"],metric_info["service_name"],metric_info["qworker_id"],metric_info["job_id"],metric_info["start_time"])
        if num_nodes_to_scale_down > 0:
            logger.info("terminate job - qworkerid: {0}, job_id: {1}".format(metric_info["qworker_id"],metric_info["job_id"]))
            data_back = "stop_worker"
            num_nodes_to_scale_down = num_nodes_to_scale_down -1 
    elif metric_type.lower() == "job_failed":
        monitoring.job_failed(metric_info["node_id"],metric_info["experiment_id"],metric_info["service_name"],metric_info["qworker_id"],metric_info["job_id"],metric_info["fail_time"])
        if num_nodes_to_scale_down > 0:
            logger.info("job failed - qworkerid: {0}, job_id: {1}".format(metric_info["qworker_id"],metric_info["job_id"]))
            data_back = "stop_worker"
            num_nodes_to_scale_down = num_nodes_to_scale_down -1
    elif metric_type.lower() == "run_task":
        monitoring.run_task(metric_info["node_id"],metric_info["experiment_id"],metric_info["service_name"],metric_info["qworker_id"],metric_info["job_id"],metric_info["task_id"])
    elif metric_type.lower() == "terminate_task":
        monitoring.terminate_task(metric_info["node_id"],metric_info["experiment_id"],metric_info["service_name"],metric_info["qworker_id"],metric_info["job_id"],metric_info["task_id"],metric_info["start_time"])
    elif metric_type.lower() == "task_failed":
        monitoring.task_failed(metric_info["node_id"],metric_info["experiment_id"],metric_info["service_name"],metric_info["qworker_id"],metric_info["job_id"],metric_info["task_id"],metric_info["fail_time"])
    else:
        data_back ="The metric of type {} didn't match with any known metric types".format(metric_type)
    return data_back

def inform_event(event_info):
    global num_nodes_to_scale_down
    """ Receive information about external events """
    event_type = event_info["event_type"]
    data_back = ""
    if(event_type.lower() == "scale_down"):
        if "num_nodes" in event_info:
            num_nodes_to_scale_down = event_info["num_nodes"]
        else:
            data_back = "Event of type {} must contain value for \"num_nodes\" parameter.".format(event_type)
    else:
        data_back = "Event of type {} does not match with any known events.".format(event_type)
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
        logger.info("Inside post. The path is: {}".format(self.path))
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
            logger.error("Error in parsing the content_length and packet data")
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
        elif self.path == "/experiment/inform":
            data_back = inform_event(data_json)

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
