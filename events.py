from celery import bootsteps
from celery.events.state import Worker
import logging
import monitoring

logger = logging.getLogger(__name__)

class GossipStepEvent(bootsteps.StartStopStep):
    requires = {'celery.worker.consumer.gossip:Gossip'}

    def start(self, c):
        self.c = c
        self.c.gossip.on.node_join.add(self.on_node_join)
        self.c.gossip.on.node_leave.add(self.on_node_leave)
        self.c.gossip.on.node_lost.add(self.on_node_lost)
    def on_cluster_size_change(self, worker):
        cluster_size = len(list(self.c.gossip.state.alive_workers()))
        log_message = "Active workers cluster size => {0} \nList of Worker => (".format(cluster_size)
        still_exist = False
        for w in list(self.c.gossip.state.alive_workers()):
            log_message = log_message + "," + w.hostname
            if w.hostname==worker.hostname:
                still_exist = True
        logger.debug(log_message + ")")
        if still_exist == False:
            logger.info("Node lost update => Worker: {0}, isn't active and therefore terminated. ".format(worker.hostname))
            monitoring.terminate_worker(worker.hostname)        
    def on_node_join(self, worker):
        logger.info('Node join => {0}'.format(worker.hostname))
        monitoring.add_worker(worker.hostname)
    
    def on_node_leave(self, worker):
        logger.info('Node left => {0}'.format(worker.hostname))
        monitoring.terminate_worker(worker.hostname)
    
    def on_node_lost(self, worker):
        # may have processed heartbeat too late, so wake up soon
        # in order to see if the worker recovered.
        # self.c.timer.call_after(10.0, self.on_cluster_size_change)
        logger.info('Node lost => {0}'.format(worker.hostname))
        worker_id = worker.hostname.split("@")[1]
        if worker_id in monitoring.list_active_workers:
            self.c.timer.call_after(10.0, self.on_cluster_size_change, (worker,))