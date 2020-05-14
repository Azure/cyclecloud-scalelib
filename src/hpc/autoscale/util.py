import uuid as uuidlib
from typing import Any, Callable, Dict, List, TypeVar

from hpc.autoscale.codeanalysis import hpcwrapclass


@hpcwrapclass
class IncrementingUUID:
    """
    A pseudo uuid utility to make debugging tests easier.
    """

    def __init__(self) -> None:
        self.current: Dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        if prefix and not prefix.endswith("-"):
            prefix = prefix + "-"

        if prefix not in self.current:
            self.current[prefix] = 0

        ret = str(self.current[prefix])
        self.current[prefix] += 1
        return prefix + ret


def _uuid_func_impl(ignore: str) -> uuidlib.UUID:
    return uuidlib.uuid4()


_uuid_func = _uuid_func_impl


def set_uuid_func(uuid_func: Any) -> None:
    global _uuid_func
    _uuid_func = uuid_func


def uuid(prefix: str = "") -> str:
    return str(_uuid_func(prefix))


T = TypeVar("T")
K = TypeVar("K")


def partition(node_list: List[T], func: Callable[[T], K]) -> Dict[K, List[T]]:
    by_key: Dict[K, List[T]] = {}
    for node in node_list:
        key = func(node)
        if key not in by_key:
            by_key[key] = []
        by_key[key].append(node)
    return by_key


def partition_single(node_list: List[T], func: Callable[[T], K]) -> Dict[K, T]:
    result = partition(node_list, func)
    ret: Dict[K, T] = {}
    for key, value in result.items():
        if len(value) != 1:
            raise RuntimeError(
                "Could not partition list into single values - key={} values={}".format(
                    key, value,
                )
            )
        ret[key] = value[0]
    return ret
