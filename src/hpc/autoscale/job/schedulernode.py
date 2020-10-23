import socket
import typing

from immutabledict import ImmutableOrderedDict

from hpc.autoscale import hpclogging as logging
from hpc.autoscale import hpctypes as ht
from hpc.autoscale.codeanalysis import hpcwrapclass
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.node import Node


@hpcwrapclass
class SchedulerNode(Node):
    # used only internally for testing
    ignore_hostnames: bool = False

    def __init__(self, hostname: str, resources: typing.Optional[dict] = None) -> None:
        resources = resources or ht.ResourceDict({})
        private_ip: typing.Optional[ht.IpAddress]
        if SchedulerNode.ignore_hostnames:
            private_ip = None
        else:
            try:
                private_ip = ht.IpAddress(socket.gethostbyname(hostname))
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
            software_configuration=ImmutableOrderedDict({}),
            keep_alive=False,
        )

    def to_dict(self) -> typing.Dict:
        return {
            "hostname": self.hostname,
            "job_ids": list(self.assignments),
            "resources": dict(self.resources),
            "available": dict(self.available),
            "metadata": dict(self.metadata),
        }

    @staticmethod
    def from_dict(d: typing.Dict) -> "SchedulerNode":
        hostname = ht.Hostname(d["hostname"])
        resources = d.get("resources", {})
        available = d.get("available", {})
        metadata = d.get("metadata", {})
        job_ids = d.get("job_ids", [])
        ret = SchedulerNode(hostname, resources)

        for job_id in job_ids:
            ret.assign(job_id)

        ret.available.update(available)
        ret.metadata.update(metadata)
        return ret

    def __lt__(self, node: typing.Any) -> int:
        return node.hostname_or_uuid < self.hostname_or_uuid

    def __str__(self) -> str:
        return "Scheduler{}".format(Node.__str__(self))

    def __repr__(self) -> str:
        return "Scheduler{}".format(Node.__repr__(self))
