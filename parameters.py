import os

# Experiment receiver configuration
http_server_port	= 8081
# MiCADO Master configuration
micado_master_ip = os.getenv('MICADO_SERVER', "micado")

# Prometheus configuration
prometheus_protocol = 'http'
prometheus_ip       = 'prometheus'
prometheus_port     = 9090

# Job Queue Prefix
JOB_QUEUE_PREFIX 	= 	'jqueue_service_'

# Backend configuration - Rabbitmq
broker_protocol		= 	'pyamqp'
broker_username 	= 	'admin'
broker_password 	= 	'mypass'
broker_server		=	'rabbit'
broker_port 		= 	5672

def broker():
	broker 	= 	broker_protocol + '://' + broker_username
	if (broker_password != ''):
		broker 	= broker + ':' + broker_password
	broker 	= 	broker + '@' + broker_server + ':' + str(broker_port) + '//'
	return broker


# Redis Backend configuration
import redis
backend_password                        =       'b4sic'
backend_protocol                        =       'redis'
backend_server                          =       'redis'
backend_port                            =       6379
backend_db                                      =       0
backend_experiment_db_id        =       10

backend_experiment_db = redis.StrictRedis(
        host=backend_server,
        port=backend_port,
        db=backend_experiment_db_id,
        password=backend_password,
        charset="utf-8",
        decode_responses=True
        )

def backend(db):
        backend = backend_protocol + '://:' + backend_password + '@' + backend_server + ':' + str(backend_port) + '/' + str(db)
        return backend



# Prometheus exporer configuration
STATSD_SERVER 	= 'statsd'
STATSD_PORT		= 9125
STATSD_OPTIONS = {
    'api_key':'jqueuer_api_key',
    'app_key':'jqueuer_app_key',
    'statsd_host': STATSD_SERVER,
    'statsd_port': STATSD_PORT
}

from datadog import initialize
initialize(**STATSD_OPTIONS)
from datadog import statsd
