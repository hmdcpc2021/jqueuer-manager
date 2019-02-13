import requests
import jinja2

TOSCA_OUT = 'tosca.yaml'

def get_micado_url(json_data, verb):
    """ Build the MiCADO url from JSON """
    if verb == 'POST':
        route = '/app/launch/'
    elif verb == 'DELETE':
        route = '/app/undeploy/'

    url_params = \
    {'micado_ip': json_data.get('micado_ip', 'micado'),
     'micado_port': json_data.get('micado_port', '443'),
     'micado_user': json_data.get('micado_user', 'admin'),
     'micado_pass': json_data.get('micado_pass', 'admin'),
     'submitter_api': \
      json_data.get('submitter_api', 'toscasubmitter/v1.0').strip("/"),
     'route': route}

    return "https://{micado_user}:{micado_pass}@{micado_ip}:{micado_port}/" \
           "{submitter_api}{route}".format(**url_params)

def get_params(json_data):
   return json_data.get('params', None)

def post_to_submitter(micado_url, app_id, params):
    """ Submit TOSCA ADT to the TOSCAsubmitter """    
    files = {'file': open(TOSCA_OUT,'rb')}
    data = {'id': app_id, 'params': params}
    r = requests.post(micado_url, files=files, data=data, verify=False)

def delete_to_submitter(micado_url, app_id):
    """ Submit TOSCA ADT to the TOSCAsubmitter """
    files = {'file': open(TOSCA_OUT, 'rb')}
    r = requests.delete(micado_url + app_id, files=files, verify=False)

def generate_tosca(json_data, tosca_path):
    """ Generate the TOSCA ADT """
    adt = ""

    with open(tosca_path) as tosca_in:
        adt = jinja2.Template(tosca_in.read())
    adt = adt.render(json_data)

    with open(TOSCA_OUT, 'w') as tosca_out:
        tosca_out.write(adt)
