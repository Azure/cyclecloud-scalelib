import typing

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.node import Node


class SchedulerNode(Node):
    def __init__(self, hostname: str, resources: typing.Optional[dict] = None) -> None:
        resources = resources or ht.ResourceDict({})
        Node.__init__(
            self,
            node_id=DelayedNodeId(ht.NodeName(hostname)),
            name=ht.NodeName(hostname),
            nodearray=ht.NodeArrayName("unknown"),
            bucket_id=ht.BucketId("unknown"),
            hostname=ht.Hostname(hostname),
            private_ip=None,
            vm_size=ht.VMSize("unknown"),
            vm_family=ht.VMFamily("unknown"),
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
