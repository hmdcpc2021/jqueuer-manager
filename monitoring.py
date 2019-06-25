""" Storing values in the monitoring system: prometheus using statsd"""
import time
import sys

from parameters import statsd

# Number of jobs added
JQUEUER_JOB_ADDED = "jqueuer_job_added"
JQUEUER_JOB_ADDED_TIMESTAMP = "jqueuer_job_added_timestamp"


def add_job(experiment_id, service_name, job_id):
    statsd.gauge(
        JQUEUER_JOB_ADDED_TIMESTAMP,
        time.time(),
        tags=[
            "experiment_id:%s" % experiment_id,
            "service_name:%s" % service_name,
            "job_id: %s" % job_id,
        ],
    )
    statsd.gauge(
        JQUEUER_JOB_ADDED,
        time.time(),
        tags=[
            "experiment_id:%s" % experiment_id,
            "service_name:%s" % service_name,
            "job_id: %s" % job_id,
        ],
    )


# Number of tasks added
JQUEUER_TASK_ADDED = "jqueuer_task_added"
JQUEUER_TASK_ADDED_TIMESTAMP = "jqueuer_task_added_timestamp"


def add_task(experiment_id, service_name, job_id, task_id):
    statsd.gauge(
        JQUEUER_TASK_ADDED_TIMESTAMP,
        time.time(),
        tags=[
            "experiment_id:%s" % experiment_id,
            "service_name:%s" % service_name,
            "job_id: %s" % job_id,
            "task_id: %s" % task_id,
        ],
    )
    statsd.gauge(
        JQUEUER_TASK_ADDED,
        time.time(),
        tags=[
            "experiment_id:%s" % experiment_id,
            "service_name:%s" % service_name,
            "job_id: %s" % job_id,
            "task_id: %s" % task_id,
        ],
    )


# Time when the experiment started
JQUEUER_EXPERIMENT_ADDING_TIMESTAMP = "jqueuer_experiment_adding_timestamp"

def experiment_adding_timestamp(
    experiment_id, service_name, experiment_adding_timestamp
):
    statsd.gauge(
        JQUEUER_EXPERIMENT_ADDING_TIMESTAMP,
        experiment_adding_timestamp,
        tags=["experiment_id:%s" % experiment_id, "service_name:%s" % service_name],
    )

# Time when the experiment must finish
JQUEUER_EXPERIMENT_DEADLINE = "jqueuer_experiment_deadline"

def experiment_deadline(
    experiment_id, service_name, experiment_deadline
):
    statsd.gauge(
        JQUEUER_EXPERIMENT_DEADLINE,
        experiment_deadline,
        tags=["experiment_id:%s" % experiment_id, "service_name:%s" % service_name],
    )

# Estimated time of average job
JQUEUER_EXPERIMENT_TASK_DURATION = "jqueuer_single_task_duration"

def experiment_task_duration(
    experiment_id, service_name, single_task_duration
):
    statsd.gauge(
        JQUEUER_EXPERIMENT_TASK_DURATION,
        single_task_duration,
        tags=["experiment_id:%s" % experiment_id, "service_name:%s" % service_name],
    )

