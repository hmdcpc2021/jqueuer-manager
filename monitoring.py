""" Storing values in the monitoring system with prometheus """
import time
import sys
import copy
import logging

from prometheus_client import start_http_server, Gauge, Counter

logger = logging.getLogger(__name__)

# Number of jobs added
JQUEUER_JOB_ADDED = "jqueuer_job_added"
JQUEUER_TASK_ADDED = "jqueuer_task_added"
JQUEUER_EXPERIMENT_ADDING_TIMESTAMP = "jqueuer_experiment_adding_timestamp"
JQUEUER_EXPERIMENT_DEADLINE = "jqueuer_experiment_deadline"
JQUEUER_EXPERIMENT_TASK_DURATION = "jqueuer_single_task_duration"
    
job_added = Gauge(JQUEUER_JOB_ADDED, "Time when job added", ["experiment_id", "service_name", "job_id"])
task_added = Gauge(JQUEUER_TASK_ADDED, "Time when task added", ["experiment_id", "service_name", "job_id", "task_id"])
exp_added = Gauge(JQUEUER_EXPERIMENT_ADDING_TIMESTAMP, "Time when exp added", ["experiment_id", "service_name"])
exp_deadl = Gauge(JQUEUER_EXPERIMENT_DEADLINE, "Experiment deadline", ["experiment_id", "service_name"])
task_dur = Gauge(JQUEUER_EXPERIMENT_TASK_DURATION, "Experiment task duration", ["experiment_id", "service_name"])

# Keep track of experiment statistics

# Dictionary list of running jobs - key = worker_id, Value = {job_id,start_time}
running_jobs = {}
list_nodes_to_scale_down = []


# ---------------------------------------

def start(metric_server_port):
    start_http_server(metric_server_port)

def add_job(experiment_id, service_name, job_id):
    job_added.labels(experiment_id, service_name, job_id).set(time.time())

def add_task(experiment_id, service_name, job_id, task_id):
    task_added.labels(experiment_id, service_name, job_id, task_id).set(time.time())

def experiment_adding_timestamp(experiment_id, service_name, experiment_adding_timestamp):
    exp_added.labels(experiment_id, service_name).set(experiment_adding_timestamp)

def experiment_deadline(experiment_id, service_name, experiment_deadline):
    exp_deadl.labels(experiment_id, service_name).set(experiment_deadline)

def experiment_task_duration(experiment_id, service_name, single_task_duration):
    task_dur.labels(experiment_id, service_name).set(single_task_duration)

# J-queuer Agent metrics
node_counter = Gauge("jqueuer_worker_count", "JQueuer Worker", ["node_id","experiment_id","service_name","qworker_id"])
job_running_timestamp = Gauge("jqueuer_job_running_timestamp","jqueuer_job_running_timestamp",["node_id","experiment_id","service_name","job_id"])
job_running = Gauge("jqueuer_job_running","jqueuer_job_running",["node_id","experiment_id","service_name","qworker_id","job_id"])
job_started = Gauge("jqueuer_job_started","jqueuer_job_started",["node_id","experiment_id","service_name","qworker_id","job_id"])
job_accomplished_timestamp = Gauge("jqueuer_job_accomplished_timestamp","jqueuer_job_accomplished_timestamp",["node_id","experiment_id","service_name","job_id"])
job_accomplished_duration = Gauge("jqueuer_job_accomplished_duration","jqueuer_job_accomplished_duration",["node_id","experiment_id","service_name","job_id"])
job_accomplished = Gauge("jqueuer_job_accomplished","jqueuer_job_accomplished",["node_id","experiment_id","service_name","qworker_id","job_id"])
job_failed_timestamp = Gauge("jqueuer_job_failed_timestamp","jqueuer_job_failed_timestamp",["node_id","experiment_id","service_name","job_id"])
job_failed_duration = Gauge("jqueuer_job_failed_duration","jqueuer_job_failed_duration",["node_id","experiment_id","service_name","job_id"])
job_failed_ga = Gauge("jqueuer_job_failed","jqueuer_job_failed",["node_id","experiment_id","service_name","qworker_id","job_id"])
task_running_timestamp = Gauge("jqueuer_task_running_timestamp","jqueuer_task_running_timestamp",["node_id","experiment_id","service_name","job_id","task_id"]) 
task_running = Gauge("jqueuer_task_running","jqueuer_task_running",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
task_started = Gauge("jqueuer_task_started","jqueuer_task_started",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
task_accomplished_timestamp = Gauge("jqueuer_task_accomplished_timestamp","jqueuer_task_accomplished_timestamp",["node_id","experiment_id","service_name","job_id","task_id"])
task_accomplished_duration = Gauge("jqueuer_task_accomplished_duration","jqueuer_task_accomplished_duration",["node_id","experiment_id","service_name","job_id","task_id"])
task_accomplished = Gauge("jqueuer_task_accomplished","jqueuer_task_accomplished",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
task_failed_timestamp = Gauge("jqueuer_task_failed_timestamp","jqueuer_task_failed_timestamp",["node_id","experiment_id","service_name","job_id","task_id"])
task_failed_duration = Gauge("jqueuer_task_failed_duration","jqueuer_task_failed_duration",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
task_failed_ga = Gauge("jqueuer_task_failed","jqueuer_task_failed",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
idle_nodes = Gauge("jqueuer_idle_nodes","jqueuer_idle_nodes",["node_id","experiment_id"])
def add_worker(worker_id):
    global running_jobs

    worker_id = worker_id.split("@")[1]
    node_counter.labels(getNodeID(worker_id),getExperimentID(worker_id),getServiceName(worker_id),getContainerID(worker_id)).set(1)
    
def terminate_worker(worker_id):
    global running_jobs

    worker_id = worker_id.split("@")[1]
    node_counter.labels(getNodeID(worker_id),getExperimentID(worker_id),getServiceName(worker_id),getContainerID(worker_id)).set(0)
    # Terminate running jobs
    if worker_id in running_jobs:
        entry = running_jobs[worker_id]
        terminate_running_job(worker_id,entry["job_id"])
    
def run_job(qworker_id, job_id):
    start_time = time.time()
    job_running_timestamp.labels(getNodeID(qworker_id), getExperimentID(qworker_id),getServiceName(qworker_id),job_id).set(start_time)
    job_running.labels(getNodeID(qworker_id), getExperimentID(qworker_id),getServiceName(qworker_id),getContainerID(qworker_id),job_id).set(1)
    running_jobs[qworker_id]={'job_id':job_id, 'start_time':start_time}
    
def terminate_job(qworker_id, job_id, start_time):
    elapsed_time = time.time() - start_time
    node_id = getNodeID(qworker_id)
    experiment_id = getExperimentID(qworker_id)
    service_name = getServiceName(qworker_id)
    container_id = getContainerID(qworker_id)
    job_accomplished_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_accomplished_duration.labels(node_id,experiment_id,service_name,job_id).set(elapsed_time)
    job_accomplished.labels(node_id,experiment_id,service_name,container_id,job_id).set(1)
    return terminate_running_job(qworker_id,job_id)

def terminate_running_job(qworker_id, job_id):
    global running_jobs, list_nodes_to_scale_down
    job_running.labels(getNodeID(qworker_id), getExperimentID(qworker_id),getServiceName(qworker_id),getContainerID(qworker_id),job_id).set(0)
    del running_jobs[qworker_id]

    # check if node of the worker is idle and can be publish for release
    if len(list_nodes_to_scale_down) > 0:
        node_id = getNodeID(qworker_id)
        if node_id in list_nodes_to_scale_down and check_node_running_jobs(node_id) == False:
            idle_nodes.labels(node_id, getExperimentID(qworker_id)).set(1)
            node_counter.labels(getNodeID(qworker_id),getExperimentID(qworker_id),getServiceName(qworker_id),getContainerID(qworker_id)).set(0)
            list_nodes_to_scale_down.remove(node_id)
            return "stop_worker"
    return ""

def check_node_running_jobs(node_id):
    global running_jobs
    for w_id in running_jobs:
        if getNodeID(w_id) == node_id:
            return True
    return False             
        
def job_failed(qworker_id, job_id, fail_time):
    elapsed_time = time.time() - fail_time
    node_id = getNodeID(qworker_id)
    experiment_id = getExperimentID(qworker_id)
    service_name = getServiceName(qworker_id)
    container_id = getContainerID(qworker_id)
    job_failed_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_failed_duration.labels(node_id,experiment_id,service_name,job_id).set(elapsed_time)
    job_failed_ga.labels(node_id,experiment_id,service_name,container_id,job_id).set(1)
    return terminate_running_job(qworker_id,job_id)

def run_task(qworker_id, job_id, task_id):
    node_id = getNodeID(qworker_id)
    experiment_id = getExperimentID(qworker_id)
    service_name = getServiceName(qworker_id)
    container_id = getContainerID(qworker_id)
    task_running_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_running.labels(node_id,experiment_id,service_name,container_id,job_id,task_id).set(1)

def terminate_task(qworker_id, job_id, task_id, start_time):
    elapsed_time = time.time() - start_time
    node_id = getNodeID(qworker_id)
    experiment_id = getExperimentID(qworker_id)
    service_name = getServiceName(qworker_id)
    container_id = getContainerID(qworker_id)
    task_accomplished_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_accomplished_duration.labels(node_id,experiment_id,service_name,job_id,task_id).set(elapsed_time)
    task_accomplished.labels(node_id,experiment_id,service_name,container_id,job_id,task_id).set(1)
    task_running.labels(node_id,experiment_id,service_name,container_id,job_id,task_id).set(0)

def task_failed(qworker_id, job_id, task_id, fail_time):
    elapsed_time = time.time() - fail_time
    node_id = getNodeID(qworker_id)
    experiment_id = getExperimentID(qworker_id)
    service_name = getServiceName(qworker_id)
    container_id = getContainerID(qworker_id)
    task_failed_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_failed_duration.labels(node_id,experiment_id,service_name,container_id,job_id,task_id).set(elapsed_time)
    task_failed_ga.labels(node_id,experiment_id,service_name,container_id,job_id,task_id).set(1)
    task_running.labels(node_id,experiment_id,service_name,container_id,job_id,task_id).set(0)

# Get Worker ID
def getNodeID(worker_id):
    return worker_id.split("##")[0]


# Get Service Name
def getServiceName(worker_id):
    return worker_id.split("##")[1]


# Get Container ID
def getContainerID(worker_id):
    return worker_id.split("##")[2]

# Get Experiment ID
def getExperimentID(worker_id):
    return worker_id.split("##")[3]
