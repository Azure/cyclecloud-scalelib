from hpc.autoscale.ccbindings.mock import MockClusterBinding
from hpc.autoscale.job.computenode import SchedulerNode
from hpc.autoscale.job.job import Job
from hpc.autoscale.node.nodemanager import new_node_manager


def setup_module() -> None:
    SchedulerNode.ignore_hostnames = True


def test_placement_group() -> None:
    node = SchedulerNode("", {})
    node.exists = False

    node.placement_group = ""
    assert node.placement_group is None

    node.placement_group = "a"
    assert node.placement_group == "a"

    node.placement_group = "0"
    assert node.placement_group == "0"
    try:
        node.placement_group = "."
    except Exception:
        pass

    assert node.placement_group == "0"
    node.set_placement_group_escaped(".")
    assert node.placement_group == "_"

    node.exists = True
    try:
        node.placement_group = "123"
    except Exception:
        assert node.placement_group == "_"


def test_custom_node_attrs_and_node_config() -> None:
    b = MockClusterBinding()
    b.add_nodearray("htc", {}, software_configuration={"myscheduler": {"A": 1}})
    b.add_bucket("htc", "Standard_F2", 10, 10)
    b.add_node("htc-1", "htc")
    node_mgr = new_node_manager({"_mock_bindings": b})
    (existing_node,) = node_mgr.get_nodes()

    try:
        existing_node.node_attribute_overrides["willfail"] = 123
        assert False
    except TypeError:
        pass

    result = node_mgr.allocate({"exclusive": True}, node_count=2)
    assert result
    (node,) = [n for n in result.nodes if not n.exists]

    assert node.software_configuration.get("test_thing") is None
    node.node_attribute_overrides["Configuration"] = {"test_thing": "is set"}
    assert node.software_configuration.get("test_thing") == "is set"
    try:
        node.software_configuration["willfail"] = 123
        assert not node.software_configuration.get("willfail")
    except TypeError:
        pass

    # we won't handle dict merges here.
    assert node.software_configuration.get("myscheduler") == {"A": 1}

    node.node_attribute_overrides["Configuration"] = {"myscheduler": {"B": 2}}
    assert node.software_configuration.get("myscheduler") == {"B": 2}

    # if you want to add to the existing software_configuration, use
    # the node.software_configuration
    node.node_attribute_overrides["Configuration"][
        "myscsheduler"
    ] = node.software_configuration.get("myscheduler", {})
    node.node_attribute_overrides["Configuration"]["myscheduler"]["B"] = 2

    node.node_attribute_overrides["Configuration"] = {"myscheduler": {"A": 1, "B": 2}}


def test_clone() -> None:
    orig = SchedulerNode("lnx0", {"ncpus": 4})
    orig.metadata["exists_in_both"] = True
    new = orig.clone()
    assert new.available["ncpus"] == 4
    assert new.resources["ncpus"] == 4
    new.available["ncpus"] -= 1
    assert new.available["ncpus"] == 3
    assert orig.available["ncpus"] == 4

    job = Job("1", {"ncpus": 2})
    new.decrement(job._constraints, assignment_id=job.name)
    assert new.available["ncpus"] == 1
    assert orig.available["ncpus"] == 4
    assert new.assignments == set(["1"])
    assert orig.assignments == set()

    orig.metadata["exists_in_orig"] = True
    new.metadata["exists_in_new"] = True

    assert orig.metadata["exists_in_both"] is True
    assert "exists_in_new" not in orig.metadata
    assert orig.metadata["exists_in_orig"] is True

    assert new.metadata["exists_in_both"] is True
    assert new.metadata["exists_in_new"] is True
    assert "exists_in_orig" not in new.metadata
