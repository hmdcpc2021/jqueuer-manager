import json
import time
import ast
import random
import urllib.parse
import docker
import logging

import subprocess
import monitoring
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from threading import Thread, Lock
from pprint import pprint

from parameters import backend_experiment_db, JOB_QUEUE_PREFIX, pushgateway_service_name
from experiment import Experiment

logger = logging.getLogger(__name__)
jqueuer_lock = sem = threading.Semaphore()
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
    monitoring.start_experiment(experiment_id)
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
        monitoring.clear_lists()
        if monitoring.current_experiment_id != "":
            monitoring.delete_experiment()
        return "Service {} removed from backend".format(service_name)
    return "Service {} not found in queue".format(service_name)

def record_worker_metrics(metric_info):
    """ Record metric received from worker """
    metric_type = metric_info["metric_type"]
    list_active_nodes = get_current_active_nodes()
    logger.info("Metric type =>  {0} \n Metric info => {1}".format(metric_type, metric_info))
    logger.info("list_active_nodes => {0} \n list_nodes_to_scale_down => {1}".format(list_active_nodes, monitoring.list_nodes_to_scale_down))
    data_back = "Metric of type {0} is received and recorded".format(metric_type)
    if metric_type.lower() == "run_job":
        monitoring.run_job(metric_info["qworker_id"],metric_info["experiment_id"],metric_info["job_id"])
    elif metric_type.lower() == "terminate_retried_job":
        ret_val = monitoring.terminate_running_job(metric_info["qworker_id"],metric_info["experiment_id"],metric_info["job_id"])
        data_back = ret_val if ret_val != "" else data_back
    elif metric_type.lower() == "terminate_job":
        ret_val = monitoring.terminate_job(metric_info["qworker_id"],metric_info["experiment_id"],metric_info["job_id"],metric_info["start_time"])
        data_back = ret_val if ret_val != "" else data_back
    elif metric_type.lower() == "job_failed":
        ret_val = monitoring.job_failed(metric_info["qworker_id"],metric_info["experiment_id"],metric_info["job_id"],metric_info["fail_time"])
        data_back = ret_val if ret_val != "" else data_back
    elif metric_type.lower() == "run_task":
        monitoring.run_task(metric_info["qworker_id"],metric_info["experiment_id"],metric_info["job_id"],metric_info["task_id"])
    elif metric_type.lower() == "terminate_task":
        monitoring.terminate_task(metric_info["qworker_id"],metric_info["experiment_id"],metric_info["job_id"],metric_info["task_id"],metric_info["start_time"])
    elif metric_type.lower() == "task_failed":
        monitoring.task_failed(metric_info["qworker_id"],metric_info["experiment_id"],metric_info["job_id"],metric_info["task_id"],metric_info["fail_time"])
    else:
        data_back ="The metric of type {} didn't match with any known metric types".format(metric_type)
    return data_back

def inform_event(event_info):
    """ Receive information about external events """
    event_type = event_info["event_type"]
    data_back = ""
    if (event_type.lower() == "nodes_required"):
        if "num_nodes" in event_info:
            list_active_nodes = get_current_active_nodes()
            nodes_required = event_info["num_nodes"]
            diff = len(list_active_nodes) - nodes_required
            if diff> 0 and diff != len(monitoring.list_nodes_to_scale_down):
                select_nodes_for_scale_down (diff)
                monitoring.check_immediate_node_release()   
            elif diff <= 0: # Ignore, any past decisions, if they aren't yet executed.
                monitoring.list_nodes_to_scale_down.clear()
            logger.info("Inform_event: \n num_nodes => {0} \n list_active_nodes => {1} \n list_nodes_to_scale_down => {2}".format(nodes_required,list_active_nodes, monitoring.list_nodes_to_scale_down))
        else:
            data_back = "Event of type {} must contain value for \"num_nodes\" parameter.".format(event_type)
    else:
        data_back = "Event of type {} does not match with any known events.".format(event_type)
    return data_back

def get_current_active_nodes():
    list_current_nodes = []
    for act_worker in monitoring.list_active_workers:
        node_id = monitoring.getNodeID(act_worker)
        if node_id not in list_current_nodes:
            list_current_nodes.append(node_id)
    return list_current_nodes

def select_nodes_for_scale_down(num_to_scale_down):
    monitoring.list_nodes_to_scale_down.clear()
    # Check if there are idle workers
    if len(monitoring.list_active_workers) != len (monitoring.running_jobs):
        for w_key in monitoring.list_active_workers:
            node_id = monitoring.getNodeID(w_key)
            if w_key not in monitoring.running_jobs and node_id not in monitoring.list_nodes_to_scale_down:
                monitoring.list_nodes_to_scale_down.append(node_id)
            if len(monitoring.list_nodes_to_scale_down) == num_to_scale_down:
                break

    # The nodes with earliest job start time will be selected
    remaining_nodes = num_to_scale_down - len(monitoring.list_nodes_to_scale_down)
    for i in range(remaining_nodes):
        dict_keys = list(monitoring.running_jobs.keys())
        earliest = ""
        earliest_start_time = -1
        for j in range(len(monitoring.running_jobs)):
            w_key = dict_keys[j]
            start_time = monitoring.running_jobs[w_key]['start_time']
            node_id = monitoring.getNodeID(w_key)
            if node_id not in monitoring.list_nodes_to_scale_down:
                if earliest_start_time == -1:
                    earliest = w_key
                    earliest_start_time = start_time
                elif start_time < earliest_start_time:
                    earliest = w_key
                    earliest_start_time = start_time
        # add into the list of scale_down nodes, if it is not already there
        if earliest != "":
            e_node_id = monitoring.getNodeID(earliest)
            monitoring.list_nodes_to_scale_down.append(e_node_id)
                
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
        global jqueuer_lock
        # Processing POST requests
        cur_thread = threading.current_thread()
        logger.info("Thread arrived =>  {0} ".format(cur_thread.name))
        jqueuer_lock.acquire()
        cur_thread = threading.current_thread()
        logger.info("Thread entered into critical region  =>  {0}".format(cur_thread.name))
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
        cur_thread = threading.current_thread()
        logger.info("Thread leaving critical region =>  {0} ".format(cur_thread.name))
        jqueuer_lock.release()


def start(experiments_arg, port=8081):
    """ Start the REST API """
    global experiments
    experiments = experiments_arg
    server_address = ("", port)
    httpd = ThreadingHTTPServer(server_address, HTTP)
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
