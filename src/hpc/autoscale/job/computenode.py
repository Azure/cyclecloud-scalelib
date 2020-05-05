import socket
import typing

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.node import Node


@hpcwrapclass
class SchedulerNode(Node):
    def __init__(self, hostname: str, resources: typing.Optional[dict] = None) -> None:
        resources = resources or ht.ResourceDict({})
        try:
            private_ip: typing.Optional[ht.IpAddress] = ht.IpAddress(
                socket.gethostbyname(hostname)
            )
        except Exception as e:
            logging.warning("Could not find private ip for %s: %s", hostname, e)
            private_ip = None

        Node.__init__(
            self,
            node_id=DelayedNodeId(ht.NodeName(hostname)),
            name=ht.NodeName(hostname),
            nodearray=ht.NodeArrayName("unknown"),
            bucket_id=ht.BucketId("unknown"),
            hostname=ht.Hostname(hostname),
            private_ip=private_ip,
            vm_size=ht.VMSize("unknown"),
            location=ht.Location("unknown"),
            spot=False,
            vcpu_count=1,
            memory=ht.Memory(0, "b"),
            infiniband=False,
            state=ht.NodeStatus("running"),
            power_state=ht.NodeStatus("running"),
            exists=True,
            placement_group=None,
            managed=False,
            resources=ht.ResourceDict(resources),
        )

    def __str__(self) -> str:
        return "Scheduler{}".format(Node.__str__(self))

    def __repr__(self) -> str:
        return "Scheduler{}".format(Node.__repr__(self))
