from typing import Optional, Union


class _SharedLimit:
    def __init__(
        self,
        name: str,
        consumed_core_count: int,
        max_core_count: int,
        consumed_count: Optional[int] = None,
        max_count: Optional[int] = None,
    ):
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
        if max_count is None:
            assert consumed_count is None
        else:
            assert consumed_count is not None

        self.__max_count = max_count
        self.__consumed_count = consumed_count

    def _max_count(self, core_count: int) -> int:
        ret = int(self._max_core_count / core_count)
        if self.__max_count is not None:
            ret = min(ret, self.__max_count)
        return ret

    def _active_count(self, core_count: int) -> int:
        return self._max_count(core_count) - self._available_count(core_count)

    def _available_count(self, core_count: int) -> int:
        # do not round down, round up
        ret = (self._max_core_count - self._consumed_core_count) // core_count
        if self.__max_count is not None and self.__consumed_count is not None:
            return min(ret, self.__max_count - self.__consumed_count)
        return ret

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
        new_node_count = self._consumed_core_count / cores_per_node + nodes

        if new_node_count > self._max_count(cores_per_node):
            raise RuntimeError(
                "OutOfCapacity: Asked for {} * {} nodes, which would be over the {} limit ({}/{})".format(
                    nodes,
                    cores_per_node,
                    self._name,
                    self._consumed_core_count // cores_per_node,
                    self._max_count(cores_per_node),
                )
            )

        self._consumed_core_count = new_core_count
        if self.__max_count is not None and self.__consumed_count is not None:
            assert self.__consumed_count + nodes <= self.__max_count
            self.__consumed_count += nodes

    def __str__(self) -> str:
        return "{}({}/{} cores)".format(
            self._name, self._consumed_core_count, self._max_core_count
        )

    def __repr__(self) -> str:
        return str(self)


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

    def __str__(self) -> str:
        return "{}({}/{} cores)".format(
            self._name, self._consumed_core_count, self._max_core_count
        )

    def __repr__(self) -> str:
        return str(self)


class BucketLimits:
    def __init__(
        self,
        vcpu_count: int,
        regional_limits: _SharedLimit,
        cluster_limits: _SharedLimit,
        nodearray_limits: _SharedLimit,
        family_limits: Union[_SharedLimit, _SpotLimit],
        placement_group_limits: Optional[_SharedLimit],
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

        if placement_group_limits is not None:
            assert isinstance(
                placement_group_limits, _SharedLimit
            ) and placement_group_limits._name.startswith("PlacementGroup")
        self._placement_group_limits = placement_group_limits

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
        if self._placement_group_limits:
            self._placement_group_limits._decrement(count, vcpu_count)

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
        # nodearray limit is floored by a max_count OR a max_core_count
        return self.nodearray_max_count * self.__vcpu_count

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

    @property
    def placement_group_max_count(self) -> int:
        if not self._placement_group_limits:
            return -1
        return self._placement_group_limits._max_count(self.__vcpu_count)

    @property
    def placement_group_available_count(self) -> int:
        if not self._placement_group_limits:
            return -1
        return self._placement_group_limits._available_count(self.__vcpu_count)

    def __str__(self) -> str:

        ret = "BucketLimit(vcpu_count={} cores, region={}/{} cores, family={}/{} cores, cluster={}/{} cores, nodearray={}/{} nodes".format(
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

        if not self._placement_group_limits:
            return ret + ")"

        return ret + ", placement_group={}/{} nodes)".format(
            self.placement_group_available_count, self.placement_group_max_count
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
        None,
        active_core_count=cores,
        active_count=num_nodes,
        available_core_count=0,
        available_count=0,
        max_core_count=num_nodes * vcpu_count,
        max_count=num_nodes,
    )
