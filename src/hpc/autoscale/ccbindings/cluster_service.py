import json
import typing
from typing import Dict, List, Optional

import cyclecloud.api.clusters
import requests
from cyclecloud.model import (
    ClusterNodearrayStatus,
    ClusterStatus,
    NodearrayBucketStatus,
    NodearrayBucketStatusDefinition,
    NodearrayBucketStatusVirtualMachine,
    NodeCreationResult,
    NodeCreationResultSet,
    NodeList,
    NodeManagementResult,
    PlacementGroupStatus,
)

from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.hpctypes import (
    ClusterName,
    Hostname,
    IpAddress,
    NodeArrayName,
    NodeId,
    NodeName,
    OperationId,
    RequestId,
)
from hpc.autoscale.node import node

from . import cluster_service_client as csc


class ClusterServiceBinding(ClusterBindingInterface):
    def __init__(self, config: Dict) -> None:
        self.config = config

    def cluster_name(self) -> ClusterName:
        return self.config["cluster_name"]

    def create_nodes(self, nodes: List[node.Node]) -> NodeCreationResult:
        create_count = 0
        for n in nodes:
            msg = csc.CreateOrUpdateNodeRequestMessage(
                cluster_id=self.config["cluster_name"],
                properties=csc.NodePropertiesV2Message(
                    name=n.name, pool_id=n.nodearray
                ),
            )
            res = _make_request(self.config, "CreateOrUpdateNode", msg)
            if res.created:
                create_count += 1

        nsets = [NodeCreationResultSet(added=create_count, message="")]
        return NodeCreationResult(operation_id="123", sets=nsets)

    def deallocate_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    def get_cluster_status(self, nodes: bool = False) -> ClusterStatus:
        msg = csc.GetClusterRequestMessage(self.config["cluster_name"])
        # print("Here is the msg", msg, type(msg))
        response: csc.ClusterMessage = _make_request(self.config, "GetCluster", msg)
        all_nodes = []
        nodearray_statuses = []
        # print(type(response))
        # print(type(response.properties))
        # print(type(response.properties.pools))
        for pool in response.properties.pools:
            # print(type(pool))

            # all_nodes.extend(pool.nodes)
            buckets = []
            virtual_machine = pool.properties.vm_size
            """
                    cores_per_socket: integer, The number of cores per socket, Required
        gpu_count: integer, The number of GPUs this machine type has, Required
        infiniband: boolean, If this virtual machine supports InfiniBand connectivity, Required
        memory: float, The RAM in this virtual machine, in GB, Required
        pcpu_count: integer, The number of physical CPUs this machine type has, Required
        vcpu_count: integer, The number of virtual CPUs this machine type has, Required
            """
            vm = NodearrayBucketStatusVirtualMachine(
                cores_per_socket=max(
                    1, virtual_machine.pcpu_count // virtual_machine.vcpu_count
                ),
                vcpu_count=virtual_machine.vcpu_count,
                pcpu_count=virtual_machine.pcpu_count,
                memory=virtual_machine.memory_gb,
                infiniband=virtual_machine.infiniband,
                gpu_count=virtual_machine.gpu_count,
            )
            definition = NodearrayBucketStatusDefinition(
                machine_type=virtual_machine.name
            )
            pool_limits = pool.properties.limits
            buckets.append(
                NodearrayBucketStatus(
                    active_nodes=pool.properties.member_ids,
                    active_core_count=pool_limits.active_core_count,
                    active_count=pool_limits.active_count,
                    available_core_count=pool_limits.available_core_count,
                    available_count=pool_limits.available_count,
                    bucket_id=pool.properties.name,
                    consumed_core_count=pool_limits.consumed_core_count,
                    definition=definition,
                    family_consumed_core_count=pool_limits.family_consumed_core_count,
                    family_quota_core_count=pool_limits.family_quota_core_count,
                    family_quota_count=pool_limits.family_quota_count,
                    invalid_reason="",
                    valid=True,
                    max_core_count=pool_limits.max_core_count,
                    max_count=pool_limits.max_count,
                    max_placement_group_size=1000,  # TODO
                    placement_groups=[],
                    quota_core_count=pool_limits.quota_core_count,
                    quota_count=pool_limits.quota_count,
                    regional_consumed_core_count=pool_limits.regional_consumed_core_count,
                    regional_quota_core_count=pool_limits.regional_quota_core_count,
                    regional_quota_count=pool_limits.regional_quota_count,
                    virtual_machine=vm,
                )
            )
            for n in pool.nodes:
                all_nodes.append(
                    {
                        "Name": n.display_name,
                        "Template": n.pool_id,
                        "NodeId": n.id,
                        "MachineType": virtual_machine.name,
                        "TargetState": "Started",  # TODO
                        "Status": n.status,
                        "State": n.status,
                        "Hostname": n.hostname,
                        "PrivateIp": n.ipv4,
                        "InstanceId": n.instance_id,
                        "KeepAlive": n.keep_alive,
                        "Configuration": pool.properties.configuration,
                    }
                )
            """
            buckets: [NodearrayBucketStatus], Each bucket of allocation for this nodearray. The "core count" settings are always a multiple of the core count for this bucket.
, Required
            max_core_count: integer, The maximum number of cores that may be in this nodearray, Required
            max_count: integer, The maximum number of nodes that may be in this nodearray, Required
            name: string, The nodearray this is describing, Required
            nodearray: object, A node record, Required
            """
            nodearray_statuses.append(
                ClusterNodearrayStatus(
                    name=pool.pool_id,
                    max_core_count=pool.properties.limits.max_core_count,
                    max_count=pool.properties.limits.max_count,
                    buckets=buckets,
                    nodearray={
                        "ClusterName": self.config["cluster_name"],
                        "Region": pool.properties.location,
                        "SubnetId": "tbd",
                        "Configuration": pool.properties.configuration,
                    },
                )
            )
        """

        max_core_count: integer, The maximum number of cores that may be added to this cluster, Required
        max_count: integer, The maximum number of nodes that may be added to this cluster, Required
        nodearrays: [ClusterNodearrayStatus], , Required
        nodes: [object], An optional list of nodes in this cluster, only included if nodes=true is in the query, Optional
        state: string, The current state of the cluster, if it has been started at least once, Optional
        target_state: string, The desired state of the cluster (eg Started or Terminated), Optional"""
        nodes = []

        ret = ClusterStatus(
            max_core_count=10 ** 10,
            max_count=10 ** 10,
            nodearrays=nodearray_statuses,
            state="Started",
            target_state="Started",
            nodes=all_nodes,
        )

        return ret

    def get_nodes(
        self,
        operation_id: Optional[OperationId] = None,
        request_id: Optional[RequestId] = None,
    ) -> NodeList:
        cs = self.get_cluster_status(nodes=True)
        return NodeList(nodes=cs.nodes)

    def remove_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        assert nodes
        for n in nodes:
            msg = csc.DeleteNodeRequestMessage(
                cluster_id=self.config["cluster_name"],
                node_id=n.name,
            )

            res = _make_request(self.config, "DeleteNode", msg)

        return NodeManagementResult(nodes=[], operation_id="123")

    def scale(
        self,
        nodearray: NodeArrayName,
        total_core_count: Optional[int] = None,
        total_node_count: Optional[int] = None,
    ) -> None:
        pass

    def shutdown_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    def start_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    def terminate_nodes(
        self,
        nodes: Optional[List[node.Node]] = None,
        names: Optional[List[NodeName]] = None,
        node_ids: Optional[List[NodeId]] = None,
        hostnames: Optional[List[Hostname]] = None,
        ip_addresses: Optional[List[IpAddress]] = None,
        custom_filter: str = None,
    ) -> NodeManagementResult:
        pass

    def delete_nodes(self, nodes: List[node.Node]) -> NodeManagementResult:
        pass

    def retry_failed_nodes(self) -> NodeManagementResult:
        pass


def _get_session(config: typing.Dict) -> requests.sessions.Session:
    try:
        retries = 3
        while retries > 0:
            try:
                # if not config["verify_certificates"]:
                #     urllib3.disable_warnings(InsecureRequestWarning)

                s = requests.session()
                s.auth = (config["username"], config["password"])
                # TODO apparently this does nothing...
                # s.timeout = config["cycleserver"]["timeout"]
                s.verify = config[
                    "verify_certificates"
                ]  # Should we auto-accept unrecognized certs?
                s.headers = {
                    "X-Cycle-Client-Version": "%s-cli:%s" % ("hpc-autoscale", "0.0.0")
                }

                return s
            except requests.exceptions.SSLError:
                retries = retries - 1
                if retries < 1:
                    raise
    except ImportError:
        raise

    raise AssertionError(
        "Could not connect to CycleCloud. Please see the log for more details."
    )


def _make_request(config, func_name, msg):
    session = _get_session(config)
    _request_context = cyclecloud.api.clusters._RequestContext()

    _request_context.path = f"/clusterservice/{func_name}"

    _body = msg.to_json()

    _responses = []
    _responses.append((200, "object", lambda v: v))

    _response = session.request(
        "POST",
        url=f"{config['url']}/clusterservice/{func_name}",
        json=_body,
    )
    print(_response.text)
    data = json.loads(_response.text)

    if "_type" in data:
        clz = getattr(csc, data["_type"])
        msg = clz.from_json(data)
        return msg
    raise RuntimeError(json.dumps(data, indent=2))


"""
node_id: DelayedNodeId,
        name: ht.NodeName,
        nodearray: ht.NodeArrayName,
        bucket_id: ht.BucketId,
        hostname: Optional[ht.Hostname],
        private_ip: Optional[ht.IpAddress],
        instance_id: Optional[ht.InstanceId],
        vm_size: ht.VMSize,
        location: ht.Location,
        spot: bool,
        vcpu_count: int,
        memory: ht.Memory,
        infiniband: bool,
        state: ht.NodeStatus,
        target_state: ht.NodeStatus,
        power_state: ht.NodeStatus,
        exists: bool,
        placement_group: Optional[ht.PlacementGroup],
        managed: bool,
        resources: ht.ResourceDict,
        software_configuration: ImmutableOrderedDict,
        keep_alive: bool,
        gpu_count: Optional[int] = None,
"""


def main():
    with open("/Users/ryhamel/autoscale.json") as fr:
        config = json.load(fr)
    b = ClusterServiceBinding(config)
    from hpc.autoscale.node.node import Node
    from hpc.autoscale.node.delayednodeid import DelayedNodeId
    from hpc.autoscale.hpctypes import Memory
    import random

    index = random.randint(1, 100)
    node = Node(
        node_id=DelayedNodeId("s1-htc-%s" % index),
        name="s1-htc-%s" % index,
        nodearray="htc",
        bucket_id="123",
        hostname=None,
        private_ip=None,
        instance_id=None,
        vm_size="Standard_F2",
        location="westus2",
        spot=False,
        vcpu_count=2,
        infiniband=False,
        memory=Memory.value_of("4g"),
        state="Off",
        target_state="Off",
        power_state="Off",
        exists=False,
        placement_group=None,
        managed=True,
        resources={},
        software_configuration={},
        keep_alive=False,
    )
    node2 = Node(
        node_id=DelayedNodeId("s1-htc-44"),
        name="s1-htc-44",
        nodearray="htc",
        bucket_id="123",
        hostname=None,
        private_ip=None,
        instance_id=None,
        vm_size="Standard_F2",
        location="westus2",
        spot=False,
        vcpu_count=2,
        infiniband=False,
        memory=Memory.value_of("4g"),
        state="Off",
        target_state="Off",
        power_state="Off",
        exists=False,
        placement_group=None,
        managed=True,
        resources={},
        software_configuration={},
        keep_alive=False,
    )
    b.create_nodes(nodes=[node])
    cs = b.get_cluster_status()
    import sys

    json.dump(cs.to_dict(), sys.stdout, indent=2)

    b.remove_nodes([node])

    cs = b.get_cluster_status()

    json.dump(cs.to_dict(), sys.stdout, indent=2)


if __name__ == "__main__":

    main()
