import time, math, datetime, random
from parameters import backend_experiment_db, JOB_QUEUE_PREFIX
from celery import subtask
import monitoring, job_operations

# Experiment Class
class Experiment:
	# Initialization function
	# experiment argument is json object
	def __init__(self, experiment_id, private_id, experiment):
		# Reset stating time
		self.experiment_adding_timestamp	= 	self.time_now()
		self.jqueuer_task_added_count = 0
		self.jqueuer_job_added_count = 0
		# Assigning experiment ID
		self.experiment_id 	= experiment_id

		# Assigning the desired software from the experiment to a variable
		self.container_name = experiment['container_name']

		# Replacing non-alphabetical characters in the desired software name with an underscore 
		try:
			self.service_name 	= self.container_name.replace("/","_").replace(":","_").replace(".","_").replace("-","_") + "__" + private_id
			self.add_service(self.service_name)
		except Exception as e:
			self.service_name 	= None
		self.experiment = experiment
		monitoring.experiment_adding_timestamp(self.experiment_id, self.service_name, self.experiment_adding_timestamp)

	def time_now(self):
		return datetime.datetime.now().timestamp()

	# Add the service name to the backend (redis) database
	def add_service(self, service_name):
		if (backend_experiment_db.exists(service_name)):
			return ""
		backend_experiment_db.set(service_name, 
			{'experiment_id':self.experiment_id})

	# decide whether the jobs are stored in a list or an array
	def process_jobs(self):
		if (isinstance(self.experiment['jobs'], list)):
			self.process_job_list()
		else:
			self.process_job_array()
		self.task_per_job_avg = math.ceil(self.jqueuer_task_added_count / self.jqueuer_job_added_count)

	# process all jobs in the list 
	def process_job_list(self):
		for job in self.experiment['jobs']:
			try:
				job_params = job['params'] 
			except Exception as e:
				job['params'] = self.experiment['params'] 
			try:
				job_command = job['command'] 
			except Exception as e:
				job['command'] = self.experiment['command']

			self.add_job(job)

	# process job array
	def process_job_array(self):
		jobs = self.experiment['jobs']
		try:
			job_params = jobs['params'] 
		except Exception as e:
			jobs['params'] = self.experiment['params'] 
		try:
			job_command = jobs['command'] 
		except Exception as e:
			jobs['command'] = self.experiment['command']

		for x in range(0,jobs['count']):
			job_id = jobs['id'] + "_" + str(x)
			self.add_job(jobs, job_id)

	# Add a job (and its tasks) to the queue and update the monitoring counters
	def add_job(self, job, job_id = None):
		if (not job_id):
			job_id = job["id"]

		self.add_tasks(job['tasks'], job_id)

		self.jqueuer_job_added_count += 1 
		monitoring.add_job(self.experiment_id, self.service_name, job_id)

		job_queue_id = "j_" + self.service_name +"_" + str(int(round(time.time() * 1000))) + "_" + str(random.randrange(100, 999))

		chain = subtask('job_operations.add', queue = JOB_QUEUE_PREFIX + self.service_name)
		chain.delay(self.experiment_id, job_queue_id, job)

	# Count the tasks in a job and update the counters
	def add_tasks(self, tasks, job_id):
		for task in tasks:
			self.jqueuer_task_added_count += 1
			monitoring.add_task(self.experiment_id, self.service_name, job_id, task['id'])

	# Start the experiment
	# Process the jobs and then start the autoscaling process
	def start(self):
		self.process_jobs()
