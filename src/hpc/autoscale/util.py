import json
import os
import uuid as uuidlib
from abc import ABC, abstractmethod
from functools import reduce
from typing import Any, Callable, Dict, List, TextIO, TypeVar, Union

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


def partition_single(
    node_list: List[T], func: Callable[[T], K], strict: bool = True
) -> Dict[K, T]:
    result = partition(node_list, func)
    ret: Dict[K, T] = {}
    for key, value in result.items():
        if len(value) > 1:

            if strict or not reduce(lambda x, y: x == y, value):  # type: ignore
                raise RuntimeError(
                    "Could not partition list into single values - key={} values={}".format(
                        key, value,
                    )
                )
        ret[key] = value[0]
    return ret


class MultipleInstancesError(RuntimeError):
    pass


# Handle advisory locking on windows and posix
try:
    import fcntl

    def _lock(lockfp: TextIO) -> None:
        fcntl.lockf(lockfp, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(lockfp: TextIO) -> None:
        pass


except ModuleNotFoundError:
    import msvcrt

    def _lock(lockfp: TextIO) -> None:
        file_size = os.path.getsize(os.path.realpath(lockfp.name))
        msvcrt.locking(lockfp.fileno(), msvcrt.LK_RLCK, file_size)

    def _unlock(lockfp: TextIO) -> None:
        file_size = os.path.getsize(os.path.realpath(lockfp.name))
        msvcrt.locking(lockfp.fileno(), msvcrt.LK_UNLCK, file_size)


class SingletonLock(ABC):
    @abstractmethod
    def unlock(self) -> None:
        ...


class SingletonFileLock(SingletonLock):
    def __init__(self, path: str) -> None:
        super().__init__()
        self.lockpath = path
        try:
            self.lockfp = open(self.lockpath, "w")
            _lock(self.lockfp)

            self.lockfp.write(str(os.getpid()))
            self.lockfp.flush()
        except IOError:
            with open(self.lockpath) as fr:
                pid = fr.read()
            raise MultipleInstancesError(
                "Could not acquire lock ({}) - more than one instance is running (pid {}).".format(
                    self.lockpath, pid,
                )
            )

    def unlock(self) -> None:
        _unlock(self.lockfp)
        self.lockfp.close()


class NullSingletonLock(SingletonLock):
    def unlock(self) -> None:
        pass


def new_singleton_lock(config: Dict) -> SingletonLock:
    """
    define {"lock_path": null}
    explicitly in the autoscale config file to disable file locking.
    """
    if os.name == "nt":
        cc_home = os.getenv("CYCLECLOUD_HOME", "c:\\cycle\\jetpack")
    else:
        cc_home = os.getenv("CYCLECLOUD_HOME", "/opt/cycle/jetpack")

    if os.path.exists(cc_home):
        default_path = os.path.join(cc_home, "system", "bootstrap", "scalelib.lock")
    else:
        default_path = os.path.join(os.getcwd(), "scalelib.lock")

    lock_path = config.get("lock_file", default_path)

    if lock_path:
        return SingletonFileLock(lock_path)

    return NullSingletonLock()


class AliasDict(dict):
    """
    Dictionary that allows you to set/get/contain an alias of the actual key.
    The alias key will never show up in keys() or items().
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.__aliases: Dict = {}

    def add_alias(self, alias: str, canonical: str) -> None:
        self.__aliases[alias] = canonical

    def _key(self, key: object) -> object:
        if key in self.__aliases:
            return self.__aliases[key]
        return key

    def __contains__(self, key: object) -> bool:
        return super().__contains__(self._key(key))

    def __getitem__(self, key: object) -> object:
        return super().__getitem__(self._key(key))

    def __setitem__(self, key: object, value: object) -> None:
        return super().__setitem__(self._key(key), value)


class ConfigurationException(RuntimeError):
    pass


def json_load(config: Union[str, Dict]) -> Dict:
    if hasattr(config, "keys"):
        return config  # type: ignore

    try:
        assert isinstance(config, str)
        with open(config) as fr:
            return json.load(fr)
    except Exception as e:
        msg = "Could not parse config {}: {}".format(config, str(e))
        raise ConfigurationException(msg)
