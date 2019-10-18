""" Storing values in the monitoring system with prometheus """
import time
import sys

from prometheus_client import start_http_server, Gauge, Counter


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

# J-queuer Agent metrics
node_counter = Counter("jqueuer_worker_count", "JQueuer Worker", ["node_id","experiment_id","service_name"])
job_started_timestamp = Gauge("jqueuer_job_started_timestamp","jqueuer_job_started_timestamp",["node_id","experiment_id","service_name","job_id"])
job_running_timestamp = Gauge("jqueuer_job_running_timestamp","jqueuer_job_running_timestamp",["node_id","experiment_id","service_name","job_id"])
job_running = Gauge("jqueuer_job_running","jqueuer_job_running",["node_id","experiment_id","service_name","qworker_id","job_id"])
job_started = Gauge("jqueuer_job_started","jqueuer_job_started",["node_id","experiment_id","service_name","qworker_id","job_id"])
job_accomplished_timestamp = Gauge("jqueuer_job_accomplished_timestamp","jqueuer_job_accomplished_timestamp",["node_id","experiment_id","service_name","job_id"])
job_accomplished_duration = Gauge("jqueuer_job_accomplished_duration","jqueuer_job_accomplished_duration",["node_id","experiment_id","service_name","job_id"])
job_accomplished = Gauge("jqueuer_job_accomplished","jqueuer_job_accomplished",["node_id","experiment_id","service_name","qworker_id","job_id"])
job_failed_timestamp = Gauge("jqueuer_job_failed_timestamp","jqueuer_job_failed_timestamp",["node_id","experiment_id","service_name","job_id"])
job_failed_duration = Gauge("jqueuer_job_failed_duration","jqueuer_job_failed_duration",["node_id","experiment_id","service_name","job_id"])
job_failed_ga = Gauge("jqueuer_job_failed","jqueuer_job_failed",["node_id","experiment_id","service_name","qworker_id","job_id"])
task_started_timestamp = Gauge("jqueuer_task_started_timestamp","jqueuer_task_started_timestamp",["node_id","experiment_id","service_name","job_id","task_id"])
task_running_timestamp = Gauge("jqueuer_task_running_timestamp","jqueuer_task_running_timestamp",["node_id","experiment_id","service_name","job_id","task_id"]) 
task_running = Gauge("jqueuer_task_running","jqueuer_task_running",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
task_started = Gauge("jqueuer_task_started","jqueuer_task_started",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
task_accomplished_timestamp = Gauge("jqueuer_task_accomplished_timestamp","jqueuer_task_accomplished_timestamp",["node_id","experiment_id","service_name","job_id","task_id"])
task_accomplished_duration = Gauge("jqueuer_task_accomplished_duration","jqueuer_task_accomplished_duration",["node_id","experiment_id","service_name","job_id","task_id"])
task_accomplished = Gauge("jqueuer_task_accomplished","jqueuer_task_accomplished",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
task_failed_timestamp = Gauge("jqueuer_task_failed_timestamp","jqueuer_task_failed_timestamp",["node_id","experiment_id","service_name","job_id","task_id"])
task_failed_duration = Gauge("jqueuer_task_failed_duration","jqueuer_task_failed_duration",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])
task_failed_ga = Gauge("jqueuer_task_failed","jqueuer_task_failed",["node_id","experiment_id","service_name","qworker_id","job_id","task_id"])

def add_worker(node_id, experiment_id, service_name):
    node_counter.labels(node_id,experiment_id,service_name).inc()

def terminate_worker(node_id, experiment_id, service_name):
    node_counter.labels(node_id, experiment_id, service_name).dec()

def run_job(node_id, experiment_id, service_name, qworker_id, job_id):
    job_started_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_running_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_running.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(1)
    job_started.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(1)


def terminate_job(node_id, experiment_id, service_name, qworker_id, job_id, start_time):
    elapsed_time = time.time() - start_time
    job_accomplished_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_accomplished_duration.labels(node_id,experiment_id,service_name,job_id).set(elapsed_time)
    job_accomplished.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(1)
    job_running.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(0)

def job_failed(node_id, experiment_id, service_name, qworker_id, job_id, fail_time):
    elapsed_time = time.time() - fail_time
    job_failed_timestamp.labels(node_id,experiment_id,service_name,job_id).set(time.time())
    job_failed_duration.labels(node_id,experiment_id,service_name,job_id).set(elapsed_time)
    job_failed_ga.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(1)
    job_running.labels(node_id,experiment_id,service_name,qworker_id,job_id).set(0)

def run_task(node_id, experiment_id, service_name, qworker_id, job_id, task_id):
    task_started_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_running_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_running.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(1)
    task_started.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(1)

def terminate_task(node_id, experiment_id, service_name, qworker_id, job_id, task_id, start_time):
    elapsed_time = time.time() - start_time
    task_accomplished_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_accomplished_duration.labels(node_id,experiment_id,service_name,job_id,task_id).set(elapsed_time)
    task_accomplished.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(1)
    task_running.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(0)

def task_failed(node_id, experiment_id, service_name, qworker_id, job_id, task_id, fail_time):
    elapsed_time = time.time() - fail_time
    task_failed_timestamp.labels(node_id,experiment_id,service_name,job_id,task_id).set(time.time())
    task_failed_duration.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(elapsed_time)
    task_failed_ga.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(1)
    task_running.labels(node_id,experiment_id,service_name,qworker_id,job_id,task_id).set(0)