from threading import Thread

import job_manager
import experiment_receiver

from experiment import Experiment 
from parameters import http_server_port

experiments = {}

if __name__ == '__main__':
	# Starting the job manager
	job_manager_thread = Thread(target = job_manager.start_job_manager, args = ())
	job_manager_thread.start()

	# Starting the experiment receiver
	experiment_receiver_thread = Thread(target = experiment_receiver.start, args = (experiments,http_server_port))
	experiment_receiver_thread.start()
