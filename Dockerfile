FROM python:3.6-slim

COPY requirements.txt /jqueuer_manager/requirements.txt
COPY experiment.py /jqueuer_manager/experiment.py
COPY job_manager.py /jqueuer_manager/job_manager.py
COPY job_operations.py /jqueuer_manager/job_operations.py
COPY experiment_receiver.py /jqueuer_manager/experiment_receiver.py
COPY jqueuer_manager.py /jqueuer_manager/jqueuer_manager.py
COPY parameters.py /jqueuer_manager/parameters.py
COPY monitoring.py /jqueuer_manager/monitoring.py
COPY index.html /jqueuer_manager/index.html
COPY tosca-cs.yaml /etc/jqueuer/tosca.yaml
COPY handle_tosca.py /etc/jqueuer/handle_tosca.py
WORKDIR /jqueuer_manager/
RUN mkdir log
RUN mkdir data
RUN pip install -r requirements.txt
COPY pidbox.py /usr/local/lib/python3.6/site-packages/kombu/
ENTRYPOINT python3 jqueuer_manager.py
