from typing import Dict, Optional

import pytest

from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.node.bucket import NodeBucket
from hpc.autoscale.node.limits import _SharedLimit
from hpc.autoscale.node.nodemanager import new_node_manager
from hpc.autoscale.util import partition


@pytest.mark.parametrize(
    "account,other_account,other_region",
    [
        ("account-a", "account-b", "westus2"),
        ("account-a", "account-a", "eastus"),
        ("account-a", "account-b", "eastus"),
        ("account-a", "account-a", "westus2"),
        (None, None, "westus2"),
    ],
)
@pytest.mark.parametrize(
    "spot,limiting_quota",
    [(False, "family"), (False, "regional"), (True, "regional")],
)
def test_quota_scope(
    account: Optional[str],
    other_account: Optional[str],
    other_region: str,
    spot: bool,
    limiting_quota: str,
) -> None:
    bindings = MockClusterBinding("clusty")
    same_scope = account == other_account and other_region == "westus2"
    other_consumed = 4 if same_scope else 8
    for name, account_name, region, consumed in [
        ("htc", account, "westus2", 4),
        ("htc2", other_account, other_region, other_consumed),
    ]:
        nodearray = bindings.add_nodearray(name, {}, location=region, spot=spot)
        if account_name is not None:
            nodearray.nodearray["AccountName"] = account_name
        # Credential names must not prevent sharing within the same account.
        nodearray.nodearray["Credentials"] = name
        bindings.add_bucket(
            name,
            "Standard_F4s",
            max_count=10,
            available_count=10 - consumed // 4,
            family_consumed_core_count=0 if spot else consumed,
            family_quota_core_count=(
                0 if spot else (40 if limiting_quota == "family" else 400)
            ),
            regional_consumed_core_count=consumed,
            regional_quota_core_count=40 if limiting_quota == "regional" else 400,
        )

    node_mgr = new_node_manager({"_mock_bindings": bindings})
    buckets = {b.nodearray: b for b in node_mgr.get_buckets()}
    htc, htc2 = buckets["htc"], buckets["htc2"]
    other_available = 10 - other_consumed // 4
    assert htc.available_count == 9
    assert htc2.available_count == other_available
    assert htc2.limits.regional_consumed_core_count == other_consumed
    assert htc2.limits.family_consumed_core_count == other_consumed

    result = node_mgr.allocate(
        {"node.nodearray": "htc", "exclusive": True},
        node_count=9,
        all_or_nothing=True,
    )
    assert result
    assert len(result.nodes) == 9
    assert htc.available_count == 0
    assert htc2.available_count == (0 if same_scope else other_available)
    assert htc2.limits.regional_consumed_core_count == (
        40 if same_scope else other_consumed
    )
    assert htc2.limits.family_consumed_core_count == (
        40 if same_scope else other_consumed
    )

    result = node_mgr.allocate(
        {"node.nodearray": "htc2", "exclusive": True},
        node_count=other_available,
        all_or_nothing=True,
    )
    if same_scope:
        assert not result
    else:
        assert result
        assert len(result.nodes) == other_available
        assert htc2.available_count == 0


@pytest.mark.parametrize(
    "spot,other_spot,other_account,other_region",
    [
        (False, False, "account-b", "eastus"),
        (False, False, "account-b", "westus2"),
        (False, False, "account-a", "eastus"),
        (False, True, "account-a", "westus2"),
        (True, False, "account-a", "westus2"),
        (True, True, "account-b", "westus2"),
    ],
)
def test_quota_scope_full_allocation(
    spot: bool, other_spot: bool, other_account: str, other_region: str
) -> None:
    bindings = MockClusterBinding("clusty")
    for name, is_spot, account, region in [
        ("htc", spot, "account-a", "westus2"),
        ("htc2", other_spot, other_account, other_region),
    ]:
        nodearray = bindings.add_nodearray(name, {}, spot=is_spot, location=region)
        nodearray.nodearray["AccountName"] = account
        bindings.add_bucket(
            name,
            "Standard_F4s",
            max_count=50,
            available_count=50,
            family_consumed_core_count=0,
            family_quota_core_count=0 if is_spot else 200,
            regional_consumed_core_count=0,
            regional_quota_core_count=200,
        )

    node_mgr = new_node_manager({"_mock_bindings": bindings})
    buckets = {b.nodearray: b for b in node_mgr.get_buckets()}
    for name in ("htc", "htc2"):
        assert buckets[name].available_count == 50
        result = node_mgr.allocate(
            {"node.nodearray": name, "exclusive": True},
            node_count=50,
            all_or_nothing=True,
        )
        assert result
        assert len(result.nodes) == 50
        assert buckets[name].available_count == 0


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

    # spot instances replace family with regional
    assert htcspot.limits.family_max_count == 25
    # note: unaffected because it is a _spot_ not VM family limit.
    assert htcspot.limits.family_available_count == 13

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
