# Storing values in the monitoring system: prometheus using statsd
#from prometheus_client import Counter, Gauge, Histogram
import time, sys
from parameters import statsd

# Number of jobs added
JQUEUER_JOB_ADDED 			= 'jqueuer_job_added'
JQUEUER_JOB_ADDED_TIMESTAMP = 'jqueuer_job_added_timestamp'
def add_job(experiment_id ,service_name, job_id):
	statsd.gauge(JQUEUER_JOB_ADDED_TIMESTAMP,
		time.time(),
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
			'job_id: %s' % job_id,
		]
	)
	statsd.gauge(JQUEUER_JOB_ADDED,
		time.time(),
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
			'job_id: %s' % job_id,
		]
	)

# Number of tasks added
JQUEUER_TASK_ADDED 			= 'jqueuer_task_added'
JQUEUER_TASK_ADDED_TIMESTAMP = 'jqueuer_task_added_timestamp'
def add_task(experiment_id ,service_name, job_id, task_id):
	statsd.gauge(JQUEUER_TASK_ADDED_TIMESTAMP,
		time.time(),
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
			'job_id: %s' % job_id,
			'task_id: %s' % task_id,
		]
	)
	statsd.gauge(JQUEUER_TASK_ADDED,
		time.time(),
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
			'job_id: %s' % job_id,
			'task_id: %s' % task_id,
		]
	)

# Time when the experiment started
JQUEUER_EXPERIMENT_ADDING_TIMESTAMP = 'jqueuer_experiment_adding_timestamp'
def experiment_adding_timestamp(experiment_id ,service_name, experiment_adding_timestamp):
	statsd.gauge(JQUEUER_EXPERIMENT_ADDING_TIMESTAMP,
		experiment_adding_timestamp,
		tags=[
			'experiment_id:%s' % experiment_id,
			'service_name:%s' % service_name,
		]
	)
