from hpc.autoscale.util import partition, partition_single


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
        assert (
            str(e)
            == "Could not partition list into single values - key=None values=[{'id': 1}, {'id': 2}, {'id': 3}]"
        )


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
