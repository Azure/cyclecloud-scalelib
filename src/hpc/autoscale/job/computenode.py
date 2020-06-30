from hpc.autoscale import hpclogging as logging
from hpc.autoscale.job.schedulernode import SchedulerNode as _SchedulerNode

logging.warning("hpc.autoscale.job.computenode is deprecated.")
logging.warning("Please use hpc.autoscale.job.schedulernode")


SchedulerNode = _SchedulerNode
