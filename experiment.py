import time
import math
import datetime
import random

import monitoring
import job_operations
from parameters import backend_experiment_db, JOB_QUEUE_PREFIX
from celery import subtask

logger = logging.getLogger(__name__)

class Experiment:

    """ Experiment Class
	Holds an experiment
	"""

    def __init__(self, experiment_id, private_id, experiment):
        """ init method """
        # Reset stating time
        self.experiment_adding_timestamp = self.time_now()
        self.jqueuer_task_added_count = 0
        self.jqueuer_job_added_count = 0
        # Assigning experiment ID
        self.experiment_id = experiment_id

        # Assigning the desired software from the experiment to a variable
        self.container_name = experiment["container_name"]

        # Replacing non-alphabetical characters in the desired software name with an underscore
        try:
            self.service_name = (
                self.container_name.replace("/", "_")
                .replace(":", "_")
                .replace(".", "_")
                .replace("-", "_")
                + "__"
                + private_id
            )
            self.add_service(self.service_name)
        except Exception as e:
            self.service_name = None
        self.experiment = experiment
        monitoring.experiment_adding_timestamp(
            self.experiment_id, self.service_name, self.experiment_adding_timestamp
        )

    def time_now(self):
        return datetime.datetime.now().timestamp()

    def add_service(self, service_name):
        """ Add the service name to the backend (redis) database """
        if backend_experiment_db.exists(service_name):
            return ""
        backend_experiment_db.hmset(service_name, {"experiment_id": self.experiment_id})

    def process_jobs(self):
        """ decide whether the jobs are stored in a list or an array """
        if isinstance(self.experiment["jobs"], list):
            self.process_job_list()
        else:
            self.process_job_array()
        self.task_per_job_avg = math.ceil(
            self.jqueuer_task_added_count / self.jqueuer_job_added_count
        )

    def process_job_list(self):
        """ process all jobs in the list """
        for job in self.experiment["jobs"]:
            try:
                job_params = job["params"]
            except Exception as e:
                job["params"] = self.experiment["params"]
            try:
                job_command = job["command"]
            except Exception as e:
                job["command"] = self.experiment["command"]

            self.add_job(job)

    def process_job_array(self):
        """ process job array """
        jobs = self.experiment["jobs"]
        try:
            job_params = jobs["params"]
        except Exception as e:
            jobs["params"] = self.experiment["params"]
        try:
            job_command = jobs["command"]
        except Exception as e:
            jobs["command"] = self.experiment["command"]

        for x in range(0, jobs["count"]):
            job_id = jobs["id"] + "_" + str(x)
            self.add_job(jobs, job_id)

    def add_job(self, job, job_id=None):
        """ Add a job (and its tasks) to the queue and update the monitoring counters """
        if not job_id:
            job_id = job["id"]

        self.add_tasks(job["tasks"], job_id)

        self.jqueuer_job_added_count += 1
        monitoring.add_job(self.experiment_id, self.service_name, job_id)

        job_queue_id = (
            "j_"
            + self.service_name
            + "_"
            + str(int(round(time.time() * 1000)))
            + "_"
            + str(random.randrange(100, 999))
        )

        chain = subtask(
            "job_operations.add", queue=JOB_QUEUE_PREFIX + self.service_name
        )
        chain.delay(self.experiment_id, job_queue_id, job)

    def add_tasks(self, tasks, job_id):
        """ Count the tasks in a job and update the counters """
        for task in tasks:
            self.jqueuer_task_added_count += 1
            monitoring.add_task(
                self.experiment_id, self.service_name, job_id, task["id"]
            )
    def start(self):
        """ Start the experiment """
        self.process_jobs()
        logger.info("-JQueuer- Added experiment ID {}".format(self.experiment_id))
        logger.info("-JQueuer- Will try to run on container: {}".format(self.container_name))
        

