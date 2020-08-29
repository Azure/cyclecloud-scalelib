from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.node.nodemanager import new_node_manager


def test_basic() -> None:
    binding = MockClusterBinding()
    binding.add_nodearray("hpc", {"ncpus": "node.vcpu_count"})
    binding.add_bucket("hpc", "Standard_F4", max_count=100, available_count=100)
    node_mgr = new_node_manager({"_mock_bindings": binding})
    bucket = node_mgr.get_buckets()[0]

    assert 100 == bucket.available_count
    bucket.decrement(5)
    assert 95 == bucket.available_count
    bucket.rollback()
    assert 100 == bucket.available_count
    bucket.decrement(5)
    assert 95 == bucket.available_count
    bucket.commit()
    assert 95 == bucket.available_count
    bucket.decrement(5)
    assert 90 == bucket.available_count
    bucket.rollback()
    assert 95 == bucket.available_count
