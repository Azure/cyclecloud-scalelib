from hpc.autoscale.ccbindings.cluster_service import ClusterServiceBinding

from typing import Optional

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node.delayednodeid import DelayedNodeId
from hpc.autoscale.node.node import Node


class EasyNode(Node):
    def __init__(
        self,
        name: ht.NodeName,
        node_id: Optional[DelayedNodeId] = None,
        nodearray: ht.NodeArrayName = ht.NodeArrayName("execute"),
        bucket_id: Optional[ht.BucketId] = None,
        hostname: Optional[ht.Hostname] = None,
        private_ip: Optional[ht.IpAddress] = None,
        instance_id: Optional[ht.InstanceId] = None,
        vm_size: ht.VMSize = ht.VMSize("Standard_F4"),
        location: ht.Location = ht.Location("westus"),
        spot: bool = False,
        vcpu_count: int = 4,
        memory: ht.Memory = ht.Memory.value_of("8g"),
        infiniband: bool = False,
        state: ht.NodeStatus = ht.NodeStatus("Off"),
        target_state: ht.NodeStatus = ht.NodeStatus("Off"),
        power_state: ht.NodeStatus = ht.NodeStatus("off"),
        exists: bool = False,
        placement_group: Optional[ht.PlacementGroup] = None,
        managed: bool = True,
        resources: ht.ResourceDict = ht.ResourceDict({}),
        software_configuration: dict = {},
        keep_alive: bool = False,
        gpu_count: Optional[int] = None,
    ) -> None:

        Node.__init__(
            self,
            name=name,
            nodearray=nodearray,
            hostname=hostname,
            node_id=node_id or DelayedNodeId(name),
            bucket_id=bucket_id or "b1",
            private_ip=private_ip,
            instance_id=instance_id,
            vm_size=vm_size,
            location=location,
            spot=spot,
            vcpu_count=vcpu_count,
            memory=memory,
            infiniband=infiniband,
            state=state,
            target_state=target_state,
            power_state=power_state,
            exists=exists,
            placement_group=placement_group,
            managed=managed,
            resources=resources,
            software_configuration=software_configuration,
            keep_alive=keep_alive,
            gpu_count=gpu_count,
        )


def test_cluster_service() -> None:
    config = {
        "subscription_id": "s-123",
        "resource_group": "r-123",
        "url": "https://localhost",
        "cluster_name": "c100",

        "demo": {
            "vm_size": "Standard_F2",
            "region": "westus2",
            "subnet_id": "s-100",
            "scaleset_type": "Flex",
            "admin_access": {
                "user_name": "usah",
                "public_key": "pk100"
            },
            "cloud_init": "#!/bin/bash\necho hello world",
            "image_reference": {
                "publisher": "msft",
                "offer": "alma",
                "sku": "8_5",
                "version": "latest"
            },
            "tags": {
                "Owner": "ryhamel"
            }
        }
    }
    csb = ClusterServiceBinding(config)
    csb.get_cluster_status(True)
    # csb.create_nodes([EasyNode("node-1")])
    csb.remove_nodes(nodes=[EasyNode("node-1")])