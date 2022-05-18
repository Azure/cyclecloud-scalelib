import json
import os
import re
import sys
import typing
import uuid as uuidlib
import warnings
from abc import ABC, abstractmethod
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, TextIO, TypeVar, Union

from hpc.autoscale.codeanalysis import hpcwrapclass

if typing.TYPE_CHECKING:
    from hpc.autoscale.node.node import Node
    from hpc.autoscale.node.bucket import NodeBucket  # noqa:F401


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
            raise MultipleInstancesError(
                "Could not acquire lock ({}) - more than one instance is running.".format(
                    self.lockpath,
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
    warnings.warn("Please use driver.new_singleton_lock")
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


class CircularIncludeError(ConfigurationException):
    pass


def json_dump(obj: object, writer: TextIO = sys.stdout) -> None:
    def default_json(x: object) -> Any:
        if hasattr(x, "to_json"):
            return getattr(x, "to_json")()

        if hasattr(x, "to_dict"):
            return getattr(x, "to_dict")()
        assert False, type(x)
        return str(x)

    json.dump(obj, writer, indent=2, default=default_json)


def json_load(config: Union[str, Dict]) -> Dict:
    warnings.warn("Will be removed in version 0.2")
    return load_config(config)


def load_config(*configs: Union[str, Dict]) -> Dict:
    ret: Dict = {}
    for config in configs:
        child = _load_config(config, [])
        ret = _dict_merge(ret, child)
    return ret


def _load_config(config: Union[str, Dict], path_stack: List[str]) -> Dict:
    if hasattr(config, "keys"):
        return config  # type: ignore

    assert isinstance(config, str)

    config_abs_path = os.path.abspath(config)
    if not path_stack:
        path_stack.append(config_abs_path)

    try:
        assert isinstance(config, str)
        with open(config) as fr:
            base_config = json.load(fr)

        for to_import in base_config.get("include", []):
            child_abs_import: str
            if os.path.isabs(to_import):
                child_abs_import = to_import
            else:
                directory = os.path.dirname(os.path.abspath(config))
                child_abs_import = os.path.join(directory, to_import)

            if child_abs_import in path_stack:
                # append child to complete the circle
                path_stack.append(child_abs_import)
                raise CircularIncludeError(
                    "Circular include found: {}".format(" ---> ".join(path_stack))
                )

            path_stack.append(child_abs_import)
            child_config = _load_config(child_abs_import, path_stack)
            assert child_abs_import == path_stack.pop(-1)
            base_config = _dict_merge(base_config, child_config)
        return base_config

    except ConfigurationException:
        raise

    except Exception as e:
        msg = "Could not parse config {}: {}".format(config, str(e))
        raise ConfigurationException(msg)


def _dict_merge(d1: Dict, d2: Dict) -> Dict:
    ret = {}

    for k, v1 in d1.items():
        if k not in d2:
            ret[k] = v1
            continue

        v2 = d2[k]
        if type(v1) != type(v2):
            ret[k] = v2
            continue

        if isinstance(v1, list):
            ret[k] = []
            for v0 in v1 + v2:
                if v0 not in ret[k]:
                    ret[k].append(v0)

        elif isinstance(v1, dict):
            ret[k] = _dict_merge(v1, v2)
        else:
            ret[k] = v2

    for k, v2 in d2.items():
        if k not in ret:
            ret[k] = v2

    return ret


def is_valid_hostname(config: Dict, node: "Node") -> bool:
    # delayed import, as logging will import this module
    from hpc.autoscale import hpclogging as logging

    if not node.hostname:
        return False

    valid_hostnames: Optional[List[str]] = config.get("valid_hostnames")

    if not valid_hostnames:
        if is_standalone_dns(node):
            valid_hostnames = ["^ip-[0-9A-Za-z]{8}$"]
        else:
            return True

    for valid_hostname in valid_hostnames:
        if re.match(valid_hostname, node.hostname):
            return True

    logging.warning(
        "Rejecting invalid hostname '%s': Did not match any of the following patterns: %s",
        node.hostname,
        valid_hostnames,
    )
    return False


def is_standalone_dns(node_or_bucket: Union["Node", "NodeBucket"]) -> bool:
    return (
        node_or_bucket.software_configuration.get("cyclecloud", {})
        .get("hosts", {})
        .get("standalone_dns", {})
        .get("enabled", True)
    )


def parse_idle_timeout(config: Dict, node: Optional["Node"] = None) -> int:
    return parse_timeout("idle_timeout", 300, config, node)


def parse_boot_timeout(config: Dict, node: Optional["Node"] = None) -> int:
    return parse_timeout("boot_timeout", 1800, config, node)


def parse_timeout(
    timeout_key: str, default_value: int, config: Dict, node: Optional["Node"] = None
) -> int:
    value: Union[int, str, Dict] = config.get(timeout_key, default_value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    section = node.nodearray if node else "default"
    key = section
    if key not in value:
        key = "default"
    if key not in value:
        return default_value
    ret = value.get(key)
    if not ret:
        return default_value
    if isinstance(ret, str):
        return int(ret)
    return ret
