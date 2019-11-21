from celery import bootsteps
from celery.events.state import Worker
import logging

logger = logging.getLogger(__name__)

class WorkerStepEvent(bootsteps.StartStopStep):
    requires = {'celery.worker.components:Pool'}

    def __init__(self, worker, **kwargs):
        logger.info("WorkerStepEvent - Inside init {0!r}".format(kwargs))

    def create(self, worker):
        # this method can be used to delegate the action methods
        # to another object that implements ``start`` and ``stop``.
        return self
    
    def start(self, worker):
        logger.info('WorkerStepEvent - Called when the worker is started.')

    def stop(self, worker):
        logger.info('WorkerStepEvent - Called when the worker shuts down.')

    def terminate(self, worker):
        logger.info('WorkerStepEvent - Called when the worker terminates')

class GossipStepEvent(bootsteps.StartStopStep):
    requires = {'celery.worker.consumer.gossip:Gossip'}

    def start(self, c):
        self.c = c
        self.c.gossip.on.node_join.add(self.on_node_join)
        self.c.gossip.on.node_leave.add(self.on_node_leave)
        self.c.gossip.on.node_lost.add(self.on_node_lost)
        # self.tasks = [
        #     self.app.tasks['proj.tasks.add']
        #     self.app.tasks['proj.tasks.mul']
        # ]
        # self.last_size = None

    # def on_cluster_size_change(self, worker):
    #     cluster_size = len(list(self.c.gossip.state.alive_workers()))
    #     if cluster_size != self.last_size:
    #         for task in self.tasks:
    #             task.rate_limit = 1.0 / cluster_size
    #         self.c.reset_rate_limits()
    #         self.last_size = cluster_size
    
    def on_node_join(self, worker):
        w = (Worker)(worker)
        logger.info('GossipStepEvent - on_node_join - {0}'.format(w.hostname))
    
    def on_node_leave(self, worker):
        w = (Worker)(worker)
        logger.info('GossipStepEvent - on_node_leave - {0}'.format(w.hostname))
    
    def on_node_lost(self, worker):
        # may have processed heartbeat too late, so wake up soon
        # in order to see if the worker recovered.
        #self.c.timer.call_after(10.0, self.on_cluster_size_change)
        w = (Worker)(worker)
        logger.info('GossipStepEvent - on_node_lost - {0}'.format(w.hostname))