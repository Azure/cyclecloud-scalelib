from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.node.limits import _SharedLimit
from hpc.autoscale.node.nodemanager import new_node_manager


def test_family_and_spots():
    bindings = MockClusterBinding("clusty")
    bindings.add_nodearray("htc", {}, spot=False)
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
    htc, htcspot = node_mgr.get_buckets()
    if htc.nodearray != "htc":
        htc, htcspot = htcspot, htc

    # ondemand instances use actual family quota
    assert htc.limits.family_max_count == 20
    assert htc.limits.family_available_count == 10

    # spot instances replace family with regional
    assert htcspot.limits.family_max_count == 25
    assert htcspot.limits.family_available_count == 13


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
