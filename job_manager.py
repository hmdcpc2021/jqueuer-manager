from __future__ import absolute_import, unicode_literals
from threading import Thread
import time
import sys

from celery import Celery
from celery.bin import worker
from kombu import Connection, Consumer
from events import WorkerStepEvent, GossipStepEvent

import parameters as _params
import job_operations

node_id = "id_1"

job_manager_queue_name = "job_queue_"


def init_job_manager():
    job_manager_app = Celery(
        "job_manager_app",
        broker=_params.broker(),
        backend=_params.backend(0),
        include=["job_operations"],
    )

    job_manager_app.conf.update(
        task_routes={"job_operations.add": {"queue": job_manager_queue_name}},
        task_default_queue="job_manager_default_queue",
        result_expires=3600,
        task_serializer="json",
        accept_content=["json"],
        worker_concurrency=1,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_default_exchange="job_manager_exchange",
        task_default_routing_key="job_manager_routing_key",
    )
    return job_manager_app


job_manager_app = init_job_manager()


def start_job_manager():
    # Initializing the job manager app
    job_manager_app = init_job_manager()
    job_manager_app.steps['worker'].add(WorkerStepEvent)
    job_manager_app.steps['consumer'].add(GossipStepEvent)
    
    # creating the worker with the job manager app
    job_manager_worker = worker.worker(app=job_manager_app)
    

    # Creating the options
    job_manager_options = {
        "hostname": "job_manager",
        "queues": [job_manager_queue_name],
        "loglevel": "INFO",
        "traceback": True,
    }

    # Launching the worker
    job_manager_worker.run(**job_manager_options)
