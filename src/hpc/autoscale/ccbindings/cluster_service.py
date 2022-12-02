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
)

from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale.hpctypes import (
    ClusterName,
    Hostname,
    IpAddress,
    Memory,
    NodeArrayName,
    NodeId,
    NodeName,
    OperationId,
    RequestId,
)
from hpc.autoscale.node import node

from swagger_client.api.cluster_api import ClusterApi

# import ApiClient
from swagger_client.api_client import ApiClient
from swagger_client.configuration import Configuration

# import models into sdk package
from swagger_client.models.admin_access import AdminAccess
from swagger_client.models.cloud_error import CloudError
from swagger_client.models.cloud_error_body import CloudErrorBody
from swagger_client.models.cluster import Cluster
from swagger_client.models.cluster_properties import ClusterProperties
from swagger_client.models.image_reference import ImageReference
from swagger_client.models.ip_rule import IpRule
from swagger_client.models.network_profile import NetworkProfile
from swagger_client.models.network_rule_action import NetworkRuleAction
from swagger_client.models.node import Node as CSNode
from swagger_client.models.node_network_access_configuration import (
    NodeNetworkAccessConfiguration,
)
from swagger_client.models.node_properties import NodeProperties
from swagger_client.models.provisioning_state import ProvisioningState
from swagger_client.models.public_network_access import PublicNetworkAccess
from swagger_client.models.scale_set_type import ScaleSetType
from swagger_client.models.virtual_machine_profile import VirtualMachineProfile


from hpc.autoscale.node import vm_sizes


class ClusterServiceBinding(ClusterBindingInterface):
    def __init__(self, config: Dict) -> None:
        self.config = config

    def _api(self) -> ClusterApi:
        csconf = Configuration()
        csconf.host = self.config["url"]
        csconf.verify_ssl = False
        return ClusterApi(api_client=ApiClient(configuration=csconf))

    @property
    def cluster_name(self) -> ClusterName:
        return self.config["cluster_name"]

    def create_cluster(self) -> None:
        api = self._api()
        nw_prof = NetworkProfile(
            node_management_access=NodeNetworkAccessConfiguration(default_action="Allow", ip_rules=[]),
            public_network_access="Disabled",
        )
        cprops = ClusterProperties(network_profile=nw_prof, tags={"Owner": "ryhamel"})
        api.clusters_create_or_update(
            cprops,
            self.config["subscription_id"],
            self.config["resource_group"],
            self.config["cluster_name"],
        )

    def create_nodes(self, nodes: List[node.Node]) -> NodeCreationResult:
        self.create_cluster()
        create_count = 0
        api = self._api()
        for n in nodes:

            vm_prof = VirtualMachineProfile(
                scaleset_type=self.config["demo"]["scaleset_type"],
                admin_access=AdminAccess(**self.config["demo"]["admin_access"]),
                cloud_init=self.config["demo"]["cloud_init"],
                image_reference=ImageReference(
                    **self.config["demo"]["image_reference"]
                ),
                region=self.config["demo"]["region"],
                subnet_id=self.config["demo"]["subnet_id"],
                tags=self.config["demo"]["tags"],
                vm_size=self.config["demo"]["vm_size"],
            )
            node_props = NodeProperties(vm_prof)

            csnode: CSNode = api.nodes_create_or_update(
                body=node_props,
                subscription_id=self.config["subscription_id"],
                resource_group=self.config["resource_group"],
                cluster_name=self.cluster_name,
                name=n.name,
            )
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
        vm_size = self.config["demo"]["vm_size"]
        region = self.config["demo"]["region"]
        aux_info = vm_sizes.get_aux_vm_size_info(region, vm_size)

        vm = NodearrayBucketStatusVirtualMachine(
            cores_per_socket=max(1, aux_info.pcpu_count // aux_info.vcpu_count),
            vcpu_count=aux_info.vcpu_count,
            pcpu_count=aux_info.pcpu_count,
            memory=aux_info.memory.value,
            infiniband=aux_info.infiniband,
            gpu_count=aux_info.gpu_count,
        )
        definition = NodearrayBucketStatusDefinition(machine_type=vm_size)

        cores = 10000
        vms = cores // aux_info.vcpu_count
        buckets = [
            NodearrayBucketStatus(
                active_nodes=[],
                active_core_count=0,
                active_count=0,
                available_core_count=cores,
                available_count=vms,
                bucket_id="b-001",
                consumed_core_count=0,
                definition=definition,
                family_consumed_core_count=0,
                family_quota_core_count=cores,
                family_quota_count=vms,
                invalid_reason="",
                valid=True,
                max_core_count=cores,
                max_count=vms,
                max_placement_group_size=1000,  # TODO
                placement_groups=[],
                quota_core_count=cores,
                quota_count=vms,
                regional_consumed_core_count=0,
                regional_quota_core_count=cores,
                regional_quota_count=vms,
                virtual_machine=vm,
            )
        ]
        # for n in pool.nodes:
        #     all_nodes.append(
        #             {
        #                 "Name": n.display_name,
        #                 "Template": n.pool_id,
        #                 "NodeId": n.id,
        #                 "MachineType": virtual_machine.name,
        #                 "TargetState": "Started",  # TODO
        #                 "Status": n.status,
        #                 "State": n.status,
        #                 "Hostname": n.hostname,
        #                 "PrivateIp": n.ipv4,
        #                 "InstanceId": n.instance_id,
        #                 "KeepAlive": n.keep_alive,
        #                 "Configuration": pool.properties.configuration,
        #             }
        #     )

        nodearray_statuses = [
            ClusterNodearrayStatus(
                name="htc",
                max_core_count=10000,
                max_count=10000 // aux_info.vcpu_count,
                buckets=buckets,
                nodearray={
                    "ClusterName": self.config["cluster_name"],
                    "Region": self.config["demo"]["region"],
                    "SubnetId": self.config["demo"]["subnet_id"],
                    "Configuration": {"slurm": {"autoscale": True, "hpc": False}},
                },
            )
        ]

        ret = ClusterStatus(
            max_core_count=10**10,
            max_count=10**10,
            nodearrays=nodearray_statuses,
            state="Started",
            target_state="Started",
            nodes=[],
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
        api = self._api()
        for n in nodes:
            api.nodes_delete(
                subscription_id=self.config["subscription_id"],
                resource_group=self.config["resource_group"],
                cluster_name=self.cluster_name,
                name=n.name,
            )

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
        api = self._api()
        for n in nodes:
            api.nodes_delete(
                subscription_id=self.config["subscription_id"],
                resource_group=self.config["resource_group"],
                cluster_name=self.cluster_name,
                name=n.name,
            )

        return NodeManagementResult(nodes=[], operation_id="123")

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
