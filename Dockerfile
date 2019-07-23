FROM python:3.6-slim

WORKDIR /var/lib/jqueuer-manager/
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . .
RUN mkdir log \
&& mkdir data \
&& rm requirements.txt

ENTRYPOINT python3 jqueuer_manager.py 