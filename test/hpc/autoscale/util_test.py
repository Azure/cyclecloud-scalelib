import json
import os
import shutil
import tempfile
from typing import Dict

from hpc.autoscale.job.schedulernode import SchedulerNode, TempNode
from hpc.autoscale.util import (
    AliasDict,
    CircularIncludeError,
    ConfigurationException,
    NullSingletonLock,
    SingletonFileLock,
    is_valid_hostname,
    load_config,
    new_singleton_lock,
    parse_idle_timeout,
    parse_boot_timeout,
    partition,
    partition_single,
)


def setup_module() -> None:
    SchedulerNode.ignore_hostnames = True


def test_partition_single() -> None:
    objs = [{"id": 1}, {"id": 2}, {"id": 3}]
    by_id = partition_single(objs, lambda x: x["id"])
    assert set([1, 2, 3]) == set(by_id.keys())

    for k, v in by_id.items():
        assert by_id[k] == {"id": k}

    try:
        partition_single(objs, lambda x: None)
        assert False
    except RuntimeError as e:
        expected = "Could not partition list into single values - key=None values=[{'id': 1}, {'id': 2}, {'id': 3}]"
        assert str(e) == expected


def test_partition() -> None:
    objs = [{"id": 1, "name": "A"}, {"id": 2}, {"id": 3}]
    by_id = partition(objs, lambda x: x["id"])
    assert set([1, 2, 3]) == set(by_id.keys())
    assert by_id[1] == [objs[0]]
    assert by_id[2] == [objs[1]]
    assert by_id[3] == [objs[2]]

    by_name = partition(objs, lambda x: x.get("name"))
    assert set(["A", None]) == set(by_name.keys())
    assert by_name["A"] == [objs[0]]
    assert by_name[None] == [objs[1], objs[2]]


def test_new_singleton_lock() -> None:
    assert isinstance(new_singleton_lock({"lock_file": None}), NullSingletonLock)
    lock_file = tempfile.mktemp()
    singleton_lock = new_singleton_lock({"lock_file": lock_file})
    assert isinstance(singleton_lock, SingletonFileLock)
    assert os.path.exists(lock_file)
    expected = str(os.getpid())
    with open(lock_file) as fr:
        assert expected == fr.read()
    singleton_lock.unlock()
    os.remove(lock_file)


def test_alias_dict() -> None:
    d = AliasDict()
    assert not d
    d["c"] = 100
    assert "a" not in d
    d.add_alias("a", "c")
    assert d._key("a") == "c"
    assert "a" not in d.keys()
    assert "a" in d
    assert d["a"] == 100
    assert d["c"] == 100

    d["a"] = 90
    assert d["a"] == 90
    assert d["c"] == 90

    d["c"] = 80
    assert d["a"] == 80
    assert d["c"] == 80


def test_include_json() -> None:
    # Issue: #5
    # Feature Request: support multiple autoscale.json files
    tempdir = tempfile.mkdtemp()
    try:
        a = os.path.join(tempdir, "a.json")
        b = os.path.join(tempdir, "b.json")
        c = os.path.join(tempdir, "c.json")

        try:
            load_config(a)
            assert False
        except ConfigurationException as e:
            assert a in str(e)

        with open(a, "w") as fw:
            json.dump({"include": ["b.json"]}, fw)

        assert os.path.exists(a)
        assert not os.path.exists(b)
        assert not os.path.exists(c)

        try:
            load_config(a)
            assert False
        except ConfigurationException as e:
            assert b in str(e)

        with open(b, "w") as fw:
            json.dump({"include": [c]}, fw)

        assert os.path.exists(a)
        assert os.path.exists(b)
        assert not os.path.exists(c)

        try:
            load_config(a)
            assert False
        except ConfigurationException as e:
            assert c in str(e)

        with open(a, "w") as fw:
            json.dump({"include": [b, c], "a": 1}, fw)
        with open(b, "w") as fw:
            json.dump({"b": 2}, fw)
        with open(c, "w") as fw:
            json.dump({"c": 3}, fw)

        expected = load_config(a)
        expected.pop("include")
        assert {"a": 1, "b": 2, "c": 3} == expected

        with open(a, "w") as fw:
            json.dump({"include": [b, c], "x": 1}, fw)
        with open(b, "w") as fw:
            json.dump({"x": 2}, fw)
        with open(c, "w") as fw:
            json.dump({"x": 3}, fw)

        expected = load_config(a)
        expected.pop("include")
        assert {"x": 3} == expected

        with open(a, "w") as fw:
            json.dump({"include": [b]}, fw)
        with open(b, "w") as fw:
            json.dump({"include": [a]}, fw)

        try:
            load_config(a)
            assert False
        except CircularIncludeError as e:
            expected = "Circular include found: {} ---> {} ---> {}".format(a, b, a)
            assert expected == str(e)

        def _rec_merge_test(ad: Dict, bd: Dict, expected: Dict) -> None:

            with open(a, "w") as fw:
                ad["include"] = [b]
                json.dump(ad, fw)

            with open(b, "w") as fw:
                json.dump(bd, fw)

            actual = load_config(a)
            actual.pop("include")
            assert actual == expected

        _rec_merge_test(
            {"d0": {"k0": 0}}, {"d0": {"k1": 1}}, {"d0": {"k0": 0, "k1": 1}}
        )

        _rec_merge_test(
            {"d0": {"d1": {"k0": 0}}},
            {"d0": {"d1": {"k1": 1}}},
            {"d0": {"d1": {"k0": 0, "k1": 1}}},
        )

        _rec_merge_test(
            {"d0": {"d1": [0]}}, {"d0": {"d1": [1]}}, {"d0": {"d1": [0, 1]}},
        )

        _rec_merge_test(
            {"d0": {"d1": [0]}}, {"d0": {"d1": {"a": 0}}}, {"d0": {"d1": {"a": 0}}},
        )

        _rec_merge_test(
            {"d0": {"d1": [{"k0": 0}]}},
            {"d0": {"d1": [{"k1": 1}]}},
            {"d0": {"d1": [{"k0": 0}, {"k1": 1}]}},
        )

        # check slurm converged
        # allow multipl configs (abspath them!)
        with open(a, "w") as fw:
            json.dump({"a": 1}, fw)
        with open(b, "w") as fw:
            json.dump({"b": 2}, fw)
        assert {"a": 1, "b": 2} == load_config(a, b)

    finally:
        shutil.rmtree(tempdir, ignore_errors=True)


def test_is_valid_hostname() -> None:
    def _test(hostname: str, standalone_dns: bool, *valid_hostnames: str) -> bool:
        node = TempNode(
            hostname,
            {},
            software_configuration={
                "cyclecloud": {"hosts": {"standalone_dns": {"enabled": standalone_dns}}}
            },
        )
        valid_hostnames_list = list(valid_hostnames) if valid_hostnames else None
        return is_valid_hostname({"valid_hostnames": valid_hostnames_list}, node)

    def _valid(hostname: str, standalone_dns: bool, *valid_hostnames: str) -> None:
        assert _test(hostname, standalone_dns, *valid_hostnames)

    def _invalid(hostname: str, standalone_dns: bool, *valid_hostnames: str) -> None:
        assert not _test(hostname, standalone_dns, *valid_hostnames)

    _valid("ip-0A010010", True)
    _valid("ip-0A010010", False)
    _invalid("ip-0A0100100", True)
    _valid("ip-0A0100100", False)
    _valid("ip-0A0100100", True, "ip-0A0100100")
    _invalid("ip-0A0100100", True, "ip-0A0100109")
    _valid("ip-0A0100100", True, "ip-0A0100109", "ip-0A0100100")
    _valid("ip-0A0100100", True, "^ip-[0-9A-Za-z]{9}$")


def test_timeouts() -> None:
    node = TempNode("localhost", nodearray="hpc")
    assert 500 == parse_idle_timeout({"idle_timeout": {"default": 500}}, node)
    assert 200 == parse_idle_timeout({"idle_timeout": {"default": 500, "hpc": 200}}, node)
    assert 300 == parse_idle_timeout({"idle_timeout": {}}, node)
    assert 600 == parse_idle_timeout({"idle_timeout": 600}, node)

    assert 500 == parse_boot_timeout({"boot_timeout": {"default": 500}}, node)
    assert 200 == parse_boot_timeout({"boot_timeout": {"default": 500, "hpc": 200}}, node)
    assert 1800 == parse_boot_timeout({"boot_timeout": {}}, node)
    assert 600 == parse_boot_timeout({"boot_timeout": 600}, node)