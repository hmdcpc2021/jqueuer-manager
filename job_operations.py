from celery import Celery

import parameters as _params


def init(service_name):
    """ Job Operations initialization """
    job_app = Celery(
        "job_app",
        broker=_params.broker(),
        backend=_params.backend(1),
        include=["job_operations"],
    )

    job_app.conf.update(
        task_routes={
            "job_operations.add": {"queue": _params.JOB_QUEUE_PREFIX + service_name}
        },
        task_default_queue="job_default_queue" + "_" + service_name,
        result_expires=3600,
        task_serializer="json",
        accept_content=["json"],
        worker_concurrency=1,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_default_exchange="job_exchange" + "_" + service_name,
        task_default_routing_key="job_routing_key" + "_" + service_name,
    )
    return job_app


job_app = init("")


@job_app.task(bind=True, acks_late=True)
def add(self, exp_id, job_queue_id, job):
    pass

