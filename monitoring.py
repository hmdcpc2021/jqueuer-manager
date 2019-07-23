""" Storing values in the monitoring system with prometheus """
import time
import sys

from prometheus_client import start_http_server, Gauge


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