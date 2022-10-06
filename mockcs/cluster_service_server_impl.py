"""
This is appended to the generated classes.
"""

from cloud.rest.cluster import status as cluster_status_plugin
from cloud.rest.nodes import create as create_nodes_plugin
from cloud.rest.nodes import remove as remove_nodes_plugin
from application import records, datastore, logger, expressions
import json

from com.cyclecomputing.appsupport.action import ActionExecution


class ServiceImpl(Service):
    def GetCluster(self, msg):
        """
        @msg: GetClusterByNameRequestMessage
        @returns: ClusterMessage
        """
        # cluster_ad = clusters.get_cluster(msg.name)
        return self._GetCluster(msg.id)

    def _GetCluster(self, cluster_name):
        """
        @cluster_name: str
        @returns: ClusterMessage
        """
        # TODO I guess we could include deleting.
        cluster_status = ClusterStatus.CLUSTER_STATUS_ACTIVE
        # TODO - actually check I guess?
        network_profile = NetworkProfileMessage(
            public_network_access=PublicNetworkAccess.PUBLIC_NETWORK_ACCESS_DISABLED,
            node_management_access=NetworkAccessConfigurationMessage(
                default_action=NetworkRuleAction.NETWORK_RULE_ACTION_ALLOW,
                ip_rules=IpRuleMessage(
                    action=NetworkRuleAction.NETWORK_RULE_ACTION_ALLOW, value="ignore"
                ),
            ),
        )

        availability = cluster_status_plugin.getController().getClusterAvailability(
            cluster_name, True
        )
        pools = []
        for nodearray_avail in availability.nodearrays:
            pm = self._getPoolMessage(nodearray_avail, availability.nodes)
            pools.append(pm)

        cluster_properties_message = ClusterPropertiesMessage(
            subscription_id="",
            resource_group_name="",
            name=cluster_name,
            tags={},
            cluster_creation_source="cyclecloud",
            cluster_visibility=ClusterVisibility.CLUSTER_VISIBILITY_VISIBLE,
            network_profile=network_profile,
            pools=pools,
        )

        return ClusterMessage(cluster_name, cluster_status, cluster_properties_message,)

    def _getPoolMessage(self, nodearray_avail, all_nodes):
        # NodeArrayAvailablity
        config_ad = nodearray_avail.nodearray.getAsAd("Configuration")
        slurm_config = (
            nodearray_avail.nodearray.getAsAd("Configuration").getAsAd("slurm").toMap()
        )

        if not nodearray_avail.buckets:
            raise RuntimeError(
                "No buckets found for nodearray %s" % nodearray_avail.nodearray
            )
        bucket = nodearray_avail.buckets[0]
        nodes = [
            n
            for n in all_nodes
            if n.getAsString("Template") == nodearray_avail.name
            and n.getAsString("MachineType") == bucket.definition.machineType
        ]

        vm = VMSize(
            name=bucket.definition.machineType,
            vcpu_count=bucket.virtualMachine.vcpuCount,
            pcpu_count=bucket.virtualMachine.pcpuCount,
            gpu_count=bucket.virtualMachine.gpuCount,
            memory_gb=bucket.virtualMachine.memory,
            infiniband=bucket.virtualMachine.infiniband,
        )

        nodes = [
            NodeStatusMessage(
                id=n.getAsString("NodeId"),
                cluster_id=n.getAsString("ClusterName"),
                display_name=n.getAsString("Name"),
                pool_id=n.getAsString("Template"),
                status=n.getAsString("Status"),
                instance_id=n.getAsString("InstanceId"),
                hostname=n.getAsString("Hostname"),
                ipv4=n.getAsString("PrivateIp"),
                keep_alive=n.getAsDefaultBoolean("KeepAlive", False),
                vm_size=vm,
            )
            for n in nodes
        ]

        pool_limits = PoolLimits(
            active_core_count=bucket.activeCoreCount,
            active_count=bucket.activeCount,
            available_core_count=bucket.availableCoreCount,
            available_count=bucket.availableCount,
            consumed_core_count=bucket.consumedCoreCount,
            family_consumed_core_count=bucket.familyConsumedCoreCount,
            family_quota_core_count=bucket.familyQuotaCoreCount,
            family_quota_count=bucket.familyQuotaCount,
            max_count=bucket.maxCount,
            max_core_count=bucket.maxCoreCount,
            quota_core_count=bucket.quotaCoreCount,
            quota_count=bucket.quotaCount,
            regional_consumed_core_count=bucket.regionalConsumedCoreCount,
            regional_quota_core_count=bucket.regionalQuotaCoreCount,
            regional_quota_count=bucket.regionalQuotaCount,
        )

        slurm_config_dict = {}
        for key in slurm_config.keySet():
            slurm_config_dict[key] = str(slurm_config.get(key))

        pool_props = PoolPropertiesMessage(
            name=nodearray_avail.name,
            max_count=nodearray_avail.maxCount,
            vm_size=vm,
            spot=False,  # TODO,
            autoscale=True,  # TODO
            limits=pool_limits,
            member_ids=list(bucket.activeNodes),
            location=nodearray_avail.nodearray.getAsString("Region"),
            configuration={
                "slurm": slurm_config_dict,
                "autoscale": {
                    "enabled": config_ad.getAsAd("autoscale").getAsDefaultBoolean(
                        "enabled", True
                    )
                },
            },
        )

        return PoolMessage(
            pool_id=nodearray_avail.name,
            cluster_id=nodearray_avail.clusterName,
            properties=pool_props,
            nodes=nodes,
        )

    def CreateOrUpdateNode(self, msg):
        """
        @msg: CreateOrUpdateNodeRequestMessage
        @returns: CreateOrUpdateNodeResponseMessage
        """
        try:
            cluster_name = msg.cluster_id
            pool_name = msg.properties.pool_id
            keep_alive = False  # msg.keep_alive
            action_execution = ActionExecution()

            params = action_execution.getParameters()
            params.setString("ClusterName", cluster_name)
            params.setString("Template", pool_name)
            params.setString("NodeArray", pool_name)
            params.setString("NameFormat", msg.properties.name)
            params.setInteger("NameOffset", 1)
            params.setInteger("Count", 1)

            params.setBoolean("KeepAlive", keep_alive)
            result = create_nodes_plugin.execute(action_execution)
            node = datastore.get("Cloud.Node", cluster_name, msg.properties.name)

            # return "ok yay %s %s" % (result.sets[0].added, result.sets[0].message)
            return CreateOrUpdateNodeResponseMessage(
                NodeV2Message(
                    id=node.getAsString("NodeId") if node else "",
                    cluster_id=cluster_name,
                    properties=NodePropertiesV2Message(
                        name=msg.properties.name, pool_id=pool_name
                    ),
                ),
                created=bool(node),
            )
        except:
            import traceback

            msg = traceback.format_exc()
            logger.error(msg)
            raise

    def DeleteNode(self, msg):
        """
        @msg: DeleteNodeRequestMessage
        @returns: DeleteNodeResponseMessage
        """
        cluster_name = msg.cluster_id

        action_execution = ActionExecution()
        action_execution.setString("Action", "Shutdown:Cloud.Node")
        action_execution.setExpression(
            "Filter",
            expressions.andAll(
                expressions.attributeEqualsValue("ClusterName", cluster_name),
                expressions.attributeIn("Name", [msg.node_id]),
            ),
        )
        params = action_execution.getParameters()
        params.setString("ClusterName", cluster_name)

        result = remove_nodes_plugin.execute(action_execution)

        return DeleteNodeResponseMessage(deleted=True)  # TODO

    def GetNode(self, msg):
        """
        @msg: GetNodeRequestMessage
        @returns: NodeMessage
        """
        raise NotImplementedError("GetNode")


def get(req, res):
    service = ServiceImpl()
    fname = req.attribute("function")
    data = json.loads(req.body())
    msg = globals()[data["_type"]].from_json(data)
    if not hasattr(service, fname):
        res.setStatus(404)
        res.write("Unknown function %s" % fname)
        return
    func = getattr(service, fname)
    ret = func(msg)

    as_json = ret.to_json()
    out = json.dumps(as_json)
    res.write(out)


def post(req, res):
    get(req, res)
