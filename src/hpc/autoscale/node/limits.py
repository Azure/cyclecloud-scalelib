from typing import Dict, Tuple, Union

from cyclecloud.model.ClusterStatusModule import ClusterStatus  # noqa: F401

from hpc.autoscale import hpctypes as ht
from hpc.autoscale.node import vm_sizes
from hpc.autoscale.util import partition


class _SharedLimit:
    def __init__(self, name: str, consumed_core_count: int, max_core_count: int):
        self._name = name
        assert (
            consumed_core_count is not None
        ), "consumed_core_count is None for limit {}".format(name)
        assert max_core_count is not None, "max_core_count is None for limit {}".format(
            name
        )
        self._consumed_core_count = max(0, consumed_core_count)
        self._max_core_count = max(0, max_core_count)
        assert (
            self._consumed_core_count <= self._max_core_count
        ), "consumed_core_count({}) > max_core_count({}) for {} limit".format(
            self._consumed_core_count, self._max_core_count, name
        )

    def _max_count(self, core_count: int) -> int:
        return self._max_core_count // core_count

    def _active_count(self, core_count: int) -> int:
        return self._max_count(core_count) - self._available_count(core_count)

    def _available_count(self, core_count: int) -> int:
        return (self._max_core_count - self._consumed_core_count) // core_count

    def _decrement(self, nodes: int, cores_per_node: int) -> None:
        new_core_count = self._consumed_core_count + (nodes * cores_per_node)
        if new_core_count > self._max_core_count:
            raise RuntimeError(
                "OutOfCapacity: Asked for {} * {} cores, which would be over the {} limit ({}/{})".format(
                    nodes,
                    cores_per_node,
                    self._name,
                    self._consumed_core_count,
                    self._max_core_count,
                )
            )
        self._consumed_core_count = new_core_count


class _SpotLimit:
    """
        Enabling spot/interruptible 0's out the family limit response
        as the regional limit is supposed to be used in its place,
        however that responsibility is on the caller and not the
        REST api. For this library we handle that for them.
    """

    def __init__(self, regional_limits: _SharedLimit) -> None:
        self._name = "Spot"
        self._regional_limits = regional_limits

    def _max_count(self, core_count: int) -> int:
        return self._regional_limits._max_count(core_count)

    def _active_count(self, core_count: int) -> int:
        return self._regional_limits._active_count(core_count)

    def _available_count(self, core_count: int) -> int:
        return self._regional_limits._available_count(core_count)

    @property
    def _consumed_core_count(self) -> int:
        return self._regional_limits._consumed_core_count

    @property
    def _max_core_count(self) -> int:
        return self._regional_limits._max_core_count

    def _decrement(self, nodes: int, cores_per_node: int) -> None:
        pass


class BucketLimits:
    def __init__(
        self,
        vcpu_count: int,
        regional_limits: _SharedLimit,
        cluster_limits: _SharedLimit,
        nodearray_limits: _SharedLimit,
        family_limits: Union[_SharedLimit, _SpotLimit],
        active_core_count: int,
        active_count: int,
        available_core_count: int,
        available_count: int,
        max_core_count: int,
        max_count: int,
    ) -> None:
        assert vcpu_count is not None
        self.__vcpu_count = vcpu_count
        assert isinstance(
            regional_limits, _SharedLimit
        ) and regional_limits._name.startswith("Region")
        self._regional_limits = regional_limits
        assert isinstance(
            cluster_limits, _SharedLimit
        ) and cluster_limits._name.startswith("Cluster")
        self._cluster_limits = cluster_limits
        assert isinstance(
            nodearray_limits, _SharedLimit
        ) and nodearray_limits._name.startswith("NodeArray")
        self._nodearray_limits = nodearray_limits
        assert isinstance(family_limits, (_SharedLimit, _SpotLimit)) and (
            family_limits._name.startswith("VM Family")
            or family_limits._name.startswith("Spot")
        )
        self._family_limits = family_limits
        assert active_core_count is not None
        self.__active_core_count = active_core_count
        assert active_count is not None
        self.__active_count = active_count
        assert available_core_count is not None
        self.__available_core_count = available_core_count
        assert available_count is not None
        self.__available_count = available_count
        assert max_core_count is not None
        self.__max_core_count = max_core_count
        assert max_count is not None
        self.__max_count = max_count

    def decrement(self, vcpu_count: int, count: int = 1) -> None:
        self.__active_core_count += count * vcpu_count
        self.__active_count += count
        self.__available_core_count -= vcpu_count * count
        self.__available_count -= count

        self._regional_limits._decrement(count, vcpu_count)
        self._cluster_limits._decrement(count, vcpu_count)
        self._nodearray_limits._decrement(count, vcpu_count)
        self._family_limits._decrement(count, vcpu_count)

    def increment(self, vcpu_count: int, count: int = 1) -> None:
        return self.decrement(vcpu_count, -count)

    @property
    def active_core_count(self) -> int:
        return self.__active_core_count

    @property
    def active_count(self) -> int:
        return self.__active_count

    @property
    def available_core_count(self) -> int:
        return self.__available_core_count

    @property
    def available_count(self) -> int:
        return self.__available_count

    @property
    def max_core_count(self) -> int:
        return self.__max_core_count

    @property
    def max_count(self) -> int:
        return self.__max_count

    @property
    def regional_consumed_core_count(self) -> int:
        return self._regional_limits._consumed_core_count

    @property
    def regional_quota_core_count(self) -> int:
        return self._regional_limits._max_core_count

    @property
    def regional_quota_count(self) -> int:
        return self._regional_limits._max_count(self.__vcpu_count)

    @property
    def regional_available_count(self) -> int:
        return self._regional_limits._available_count(self.__vcpu_count)

    @property
    def cluster_consumed_core_count(self) -> int:
        return self._cluster_limits._consumed_core_count

    @property
    def cluster_max_core_count(self) -> int:
        return self._cluster_limits._max_core_count

    @property
    def cluster_max_count(self) -> int:
        return self._cluster_limits._max_count(self.__vcpu_count)

    @property
    def cluster_available_count(self) -> int:
        return self._cluster_limits._available_count(self.__vcpu_count)

    @property
    def nodearray_consumed_core_count(self) -> int:
        return self._nodearray_limits._consumed_core_count

    @property
    def nodearray_max_core_count(self) -> int:
        return self._nodearray_limits._max_core_count

    @property
    def nodearray_max_count(self) -> int:
        return self._nodearray_limits._max_count(self.__vcpu_count)

    @property
    def nodearray_available_count(self) -> int:
        return self._nodearray_limits._available_count(self.__vcpu_count)

    @property
    def family_consumed_core_count(self) -> int:
        return self._family_limits._consumed_core_count

    @property
    def family_max_core_count(self) -> int:
        return self._family_limits._max_core_count

    @property
    def family_max_count(self) -> int:
        return self._family_limits._max_count(self.__vcpu_count)

    @property
    def family_available_count(self) -> int:
        return self._family_limits._available_count(self.__vcpu_count)

    def __str__(self) -> str:
        return "BucketLimit(vcpu_count={}, region={}/{}, family={}/{}, cluster={}/{}, nodearray={}/{})".format(
            self.__vcpu_count,
            self.regional_available_count,
            self.regional_quota_count,
            self.family_available_count,
            self.family_max_count,
            self.cluster_available_count,
            self.cluster_max_count,
            self.nodearray_available_count,
            self.nodearray_max_count,
        )

    def __repr__(self) -> str:
        return str(self)


def null_bucket_limits(num_nodes: int, vcpu_count: int) -> BucketLimits:
    cores = num_nodes * vcpu_count
    return BucketLimits(
        vcpu_count,
        _SharedLimit("Region(?)", cores, cores),
        _SharedLimit("Cluster(?)", cores, cores),
        _SharedLimit("NodeArray(?)", cores, cores),
        _SharedLimit("VM Family(?)", cores, cores),
        active_core_count=cores,
        active_count=num_nodes,
        available_core_count=0,
        available_count=0,
        max_core_count=num_nodes * vcpu_count,
        max_count=num_nodes,
    )


def create_bucket_limits(
    cluster_name: ht.ClusterName, cluster_status: ClusterStatus,
) -> Dict[Tuple[ht.BucketId, ht.PlacementGroup], BucketLimits]:

    by_nodearray = partition(cluster_status.nodes, lambda n: n["Template"])
    for nodearray in cluster_status.nodearrays:
        if nodearray.name not in by_nodearray:
            by_nodearray[nodearray.name] = []

    cluster_limit = _SharedLimit(
        "Cluster({})".format(cluster_name),
        0,  # noqa: E128
        cluster_status.max_core_count,
    )
    nodearray_limits: Dict[str, _SharedLimit] = {}
    regional_limits: Dict[str, _SharedLimit] = {}
    family_limits: Dict[str, _SharedLimit] = {}
    bucket_limits: Dict[Tuple[ht.BucketId, ht.PlacementGroup], BucketLimits] = {}

    for nodearray in cluster_status.nodearrays:
        region = nodearray.nodearray["Region"]

        nodearray_limits[nodearray.name] = _SharedLimit(
            "NodeArray({})".format(nodearray.name),
            0,
            nodearray.max_core_count,  # noqa: E128
        )

        for bucket in nodearray.buckets:
            aux_vm_info = vm_sizes.get_aux_vm_size_info(
                region, bucket.definition.machine_type
            )
            assert isinstance(aux_vm_info, vm_sizes.AuxVMSizeInfo)
            vm_family = aux_vm_info.vm_family

            if region not in regional_limits:
                regional_limits[region] = _SharedLimit(
                    "Region({})".format(region),
                    bucket.regional_consumed_core_count,
                    bucket.regional_quota_core_count,
                )

            if vm_family not in family_limits:
                family_limits[vm_family] = _SharedLimit(
                    "VM Family({})".format(vm_family),
                    bucket.family_consumed_core_count,
                    bucket.family_quota_core_count,
                )
            assert (
                bucket.bucket_id not in bucket_limits
            ), "duplicate UUID for bucket_id: {}".format(bucket.bucket_id)

            vcpu_count = bucket.virtual_machine.vcpu_count

            # the nodearray could actually have PlacementGroupId set
            placement_groups = set(
                [nodearray.nodearray.get("PlacementGroupId")]
                + (bucket.placement_groups or [])
            )
            for pg in placement_groups:
                key = (bucket.bucket_id, pg)
                active_nodes = [
                    n
                    for n in cluster_status.nodes
                    if n["MachineType"] == bucket.definition.machine_type
                    and n["Template"] == nodearray.name
                    and n.get("PlacementGroupId") == pg
                ]

                active_cores = len(active_nodes) * vcpu_count
                cluster_limit._consumed_core_count += active_cores
                nodearray_limits[nodearray.name]._consumed_core_count += active_cores

                if pg:
                    max_count = min(bucket.max_placement_group_size, bucket.max_count)
                    max_core_count = max_count * vcpu_count
                    active_count = len(active_nodes)
                    active_core_count = active_count * vcpu_count

                    available_count = max_count - active_count
                    available_core_count = max_core_count - active_core_count
                else:
                    max_count = bucket.max_count
                    max_core_count = bucket.max_core_count
                    active_count = bucket.active_count
                    active_core_count = bucket.active_core_count
                    available_count = bucket.available_count
                    available_core_count = bucket.available_core_count

                family_limit: Union[_SpotLimit, _SharedLimit] = family_limits[vm_family]
                if nodearray.nodearray.get("Interruptible"):
                    # enabling spot/interruptible 0's out the family limit response
                    # as the regional limit is supposed to be used in its place,
                    # however that responsibility is on the caller and not the
                    # REST api. For this library we handle that for them.
                    family_limit = _SpotLimit(regional_limits[region])

                bucket_limits[key] = BucketLimits(
                    vcpu_count,
                    regional_limits[region],
                    cluster_limit,
                    nodearray_limits[nodearray.name],
                    family_limit,
                    active_core_count=active_core_count,
                    active_count=active_count,
                    available_core_count=available_core_count,
                    available_count=available_count,
                    max_core_count=max_core_count,
                    max_count=max_count,
                )

    return bucket_limits
