from http.server import BaseHTTPRequestHandler, HTTPServer
import ssl
import urllib.parse, urllib.request, json, time, ast, random, requests
from pprint import pprint
from threading import Thread
from parameters import backend_experiment_db

import handle_tosca as tosca
from experiment import Experiment

TOSCA_PATH = "/etc/jqueuer/tosca-cs.yaml"

# Add an experiment
def add_experiment(experiment_json):
	private_id = str(int(round(time.time() * 1000))) + "_" + str(random.randrange(100, 999))
	experiment_id = "exp_" + private_id

	app_id, app_name = experiment_json.get("image_url").split("/")
	service_name = app_name.replace("/","_").replace(":","_") \
						   .replace(".","_").replace("-","_") + "__" + private_id
	jq_server_ip = urllib.request.urlopen('https://api.ipify.org').read().decode('utf8')
	submit_route = tosca.get_micado_url(experiment_json, 'POST')
	params = tosca.get_params(experiment_json)
	tosca_path = experiment_json.get('abs_tosca_path', TOSCA_PATH)

	manual_entries = {"JQUEUER_IP": jq_server_ip, "EXPID": experiment_id,
					  "SERVICE_NAME": service_name}
	experiment_json.update(manual_entries)

	tosca.generate_tosca(experiment_json, tosca_path)
	tosca.post_to_submitter(submit_route, app_id, params)

	experiment = Experiment(experiment_id, private_id, experiment_json)
	experiment_thread = Thread(target = experiment.start, args = ())
	experiment_thread.start()

	experiments[experiment_id] = {'experiment': experiment, 'thread': experiment_thread}
	return str(experiment_id) + " has been added & started successfully ! \n"

# Delete an experiment
def del_experiment(experiment_json):
	micado_master_ip = experiment_json.get("micado_ip")
	app_id = experiment_json.get("image_url").split("/")[0]
	delete_route = tosca.get_micado_url(experiment_json, 'DELETE')
	tosca.delete_to_submitter(delete_route, app_id)
	backend_experiment_db.flushall()

	return app_id + " and its infrastructure have been removed ! \n"
	#customer_service_name = experiment_json['service_name']
	#if (backend_experiment_db.exists(customer_service_name)):
	#	backend_experiment_db.delete(customer_service_name)
	#	return "Customer Service " + customer_service_name + " has been removed from the queue" + "\n"
	#return "Customer Service " + customer_service_name + " wasn't found in the queue" + "\n"

# HTTP Server Class
class HTTP(BaseHTTPRequestHandler):
	def _set_headers(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/html')
		self.end_headers()

	def do_GET(self):
		# Processing GET requests
		try:
			html_file = open('./index.html','rb')
			response = html_file.read()
			html_file.close()
			self._set_headers()
			self.wfile.write(response)
			return
		except Exception as e:
			pass

	def do_HEAD(self):
		self._set_headers()

	def do_POST(self):
		# Processing POST requests
		content_length= None
		data_json = None
		data = None
		try:
			content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
			data = self.rfile.read(int(content_length)).decode('utf-8')
			data_json = ast.literal_eval(data)
			pass
		except Exception as e:
			print("Error in parsing the content_length and packet data")

		if (self.path == '/experiment/result'):

			html_file = open('./' + data_json['id'] + '.html','a')
			text = '<hr>Received from {} at {}: Params: {} '.format(
				str(self.client_address),
				str(time.time()),
				str(data_json)
			)
			html_file.write(text)
			html_file.close()
			data_back = "received"
		if (self.path == '/experiment/add'):
			if data_json.get("micado_ip"):
				data_back = add_experiment(data_json)
			else:
				data_back = 'Please specify a "micado_ip" in your .json'
		elif (self.path == '/experiment/del'):
			data_back = del_experiment(data_json)

		self._set_headers()
		self.wfile.write(bytes(str(data_back), "utf-8"))


def start(experiments_arg, port=443, certfile='./server.pem'):
	# Starting the REST API Server
	global experiments
	experiments = experiments_arg
	server_address = ('', port)
	httpd = HTTPServer(server_address, HTTP)
	httpd.socket = ssl.wrap_socket (httpd.socket, certfile=certfile, server_side=True)
	print('Starting Experiment Manager HTTP Server...' + str(port))

	try:
		httpd.serve_forever()
	except KeyboardInterrupt:
		print("***** Error in Experiment Manager HTTP Server *****")
		pass

	httpd.server_close()
	print(time.asctime(), "Experiment Manager Server Stopped - %s:%s" % (server_address, port))
