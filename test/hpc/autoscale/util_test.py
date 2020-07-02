import os
import tempfile

from hpc.autoscale.util import (
    NullSingletonLock,
    SingletonFileLock,
    new_singleton_lock,
    partition,
    partition_single,
)


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
