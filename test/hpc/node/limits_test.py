from typing import Dict

from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.node.bucket import NodeBucket
from hpc.autoscale.node.limits import _SharedLimit
from hpc.autoscale.node.nodemanager import new_node_manager
from hpc.autoscale.util import partition, partition_single


def test_family_and_spots() -> None:
    bindings = MockClusterBinding("clusty")
    bindings.add_nodearray("htc", {}, spot=False, max_count=10, max_core_count=400)
    bindings.add_nodearray("hpc", {}, spot=False, max_placement_group_size=7)
    bindings.add_bucket(
        "htc",
        "Standard_F4s",
        max_count=20,
        available_count=10,
        family_consumed_core_count=40,
        family_quota_core_count=80,
        family_quota_count=20,
        regional_consumed_core_count=45,
        regional_quota_core_count=100,
        regional_quota_count=25,
    )

    bindings.add_bucket(
        "htc",
        "Standard_D4s_v3",
        max_count=20,
        available_count=10,
        family_consumed_core_count=40,
        family_quota_core_count=80,
        family_quota_count=20,
        regional_consumed_core_count=45,
        regional_quota_core_count=100,
        regional_quota_count=25,
    )

    bindings.add_bucket(
        "hpc",
        "Standard_D4s_v3",
        max_count=20,
        available_count=10,
        family_consumed_core_count=40,
        family_quota_core_count=80,
        family_quota_count=20,
        regional_consumed_core_count=45,
        regional_quota_core_count=100,
        regional_quota_count=25,
    )

    bindings.add_bucket(
        "hpc",
        "Standard_D4s_v3",
        max_count=20,
        available_count=10,
        family_consumed_core_count=40,
        family_quota_core_count=80,
        family_quota_count=20,
        regional_consumed_core_count=45,
        regional_quota_core_count=100,
        regional_quota_count=25,
        placement_groups=["123"],
    )

    bindings.add_nodearray("htcspot", {}, spot=True)
    bindings.add_bucket(
        "htcspot",
        "Standard_F4s",
        max_count=20,
        available_count=10,
        family_consumed_core_count=0,
        family_quota_core_count=0,
        family_quota_count=0,
        regional_consumed_core_count=45,
        regional_quota_core_count=100,
        regional_quota_count=25,
    )

    node_mgr = new_node_manager({"_mock_bindings": bindings})
    by_key: Dict[str, NodeBucket] = partition(
        node_mgr.get_buckets(), lambda b: (b.nodearray, b.vm_size)
    )
    htc = by_key[("htc", "Standard_F4s")][0]
    htc2 = by_key[("htc", "Standard_D4s_v3")][0]
    htcspot = by_key[("htcspot", "Standard_F4s")][0]
    spot = by_key[("htcspot", "Standard_F4s")][0]
    hpcs = by_key[("hpc", "Standard_D4s_v3")]
    hpc_pg = [x for x in hpcs if x.placement_group][0]

    # ondemand instances use actual family quota
    assert htc.limits.family_max_count == 20
    assert htc2.limits.family_max_count == 20
    assert htc.limits.family_available_count == 10
    assert htc2.limits.family_available_count == 10

    # spot instances replace family with regional
    assert htcspot.limits.family_max_count == 25
    assert htcspot.limits.family_available_count == 13

    assert node_mgr.allocate(
        {"node.nodearray": "htc", "node.vm_size": "Standard_F4s"}, node_count=1
    )
    # ondemand instances use actual family quota
    assert htc.limits.family_max_count == 20
    assert htc2.limits.family_max_count == 20
    assert htc.limits.family_available_count == 9
    assert htc2.limits.family_available_count == 10
    assert htc.limits.nodearray_available_count == 9
    assert htc2.limits.nodearray_available_count == 9
    assert htc.available_count == 9
    # nodearray limit affects htc2 since max_count=10
    assert htc2.available_count == 9

    # now the regional is affected by our allocation
    assert htcspot.limits.family_max_count == 25
    assert htcspot.limits.family_available_count == 13 - 1

    assert hpc_pg.available_count == 7


def test_shared_limit() -> None:

    limit = _SharedLimit("test", 10, 20)
    assert limit._max_core_count == 20
    assert limit._consumed_core_count == 10
    assert limit._max_count(2) == 10
    assert limit._active_count(2) == 5
    assert limit._available_count(2) == 5

    assert limit._max_count(3) == 6
    assert limit._active_count(3) == 3
    assert limit._available_count(3) == 3

    limit = _SharedLimit("test", 45, 100)
    assert limit._available_count(4) == 13


def test_limits_hypothesis() -> None:
    # TODO
    pass
