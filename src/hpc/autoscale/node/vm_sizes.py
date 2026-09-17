import json
import os
from typing import Any, Callable, Dict, List, Optional

import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht

RESOURCE_FILE = os.path.join(os.path.dirname(__file__), "vm_sizes.json")
VM_SIZES: Dict[str, Dict] = {}


def _inititialize_impl() -> None:
    global VM_SIZES
    if VM_SIZES:
        return

    try:
        with open(RESOURCE_FILE) as fr:
            VM_SIZES = json.load(fr)
    except Exception:
        logging.exception(
            (
                "Could not load resource file {}. Auxiliary vm size information "
                + "(vm_family, gpu_count, capabilities etc) will be unavailable."
            ).format(RESOURCE_FILE)
        )


def _initialize(f: Callable) -> Callable:
    def call_initialize(*args: Any, **kwargs: Any) -> Any:
        _inititialize_impl()
        return f(*args, **kwargs)

    return call_initialize


class AuxVMSizeInfo:
    def __init__(self, record: Dict[str, Any]):
        self.__record = record
        self.__capabilities = record.get("capabilities", {})
        self.__memory = ht.Memory(self.__capabilities.get("MemoryGB", -1), "g")

    @property
    def memory(self) -> ht.Memory:
        return self.__memory

    @property
    def infiniband(self) -> bool:
        return self.__capabilities.get("RdmaEnabled", False)

    @property
    def vm_family(self) -> str:
        return self.__record.get("family", "unknown")

    @property
    def vcpu_count(self) -> int:
        ret = self.__capabilities.get("vCPUsAvailable")
        if ret is None:
            return self.__capabilities.get("vCPUs", -1)
        return ret

    @property
    def pcpu_count(self) -> int:
        per_core = self.__capabilities.get("vCPUsPerCore", -1)
        if per_core > 0:
            return self.vcpu_count // per_core
        return self.vcpu_count

    @property
    def gpu_count(self) -> int:
        return self.__capabilities.get("GPUs", 0)

    @property
    def cores_per_socket(self) -> int:
        return self.__capabilities.get("vCPUsPerCore", -1)

    def get(self, key: str) -> Optional[Any]:
        return self.__record.get(key)

    @property
    def capabilities(self) -> Dict[str, Any]:
        from hpc.autoscale.clilib import ShellDict

        return ShellDict(self.__capabilities)


__AUX_CACHE = {}


@_initialize
def get_aux_vm_size_info(location: str, vm_size: str) -> AuxVMSizeInfo:
    if location not in VM_SIZES:
        proxied_location = VM_SIZES.get("proxied-locations", {}).get(location)
        logging.debug("Using proxied-location %s instead of %s to get auxiliary VM Size information for %s",
                    proxied_location,
                    location,
                    vm_size)
        if proxied_location:
            location = proxied_location

    key = (location, vm_size)

    if key not in __AUX_CACHE:
        by_name = VM_SIZES.get(location)

        if not by_name:
            return AuxVMSizeInfo({"family": "unknown"})

        vm_aux_info = by_name.get(vm_size)
        if not vm_aux_info:
            return AuxVMSizeInfo({"family": "unknown"})

        __AUX_CACHE[key] = AuxVMSizeInfo(vm_aux_info)

    return __AUX_CACHE[key]


@_initialize
def all_possible_vm_sizes() -> List[str]:
    ret = set()
    for by_name in VM_SIZES.values():
        for vm_size in by_name.keys():
            ret.add(vm_size)
    return sorted(list(ret))


@_initialize
def all_possible_vm_families() -> List[str]:
    ret = set()
    for by_name in VM_SIZES.values():
        for aux_info in by_name.values():
            ret.add(aux_info["family"])
    return sorted(list(ret))


@_initialize
def all_possible_locations() -> List[str]:
    return sorted(list(VM_SIZES.keys()))


@_initialize
def main() -> None:
    record = VM_SIZES["southcentralus"]["Standard_M208ms_v2"]
    for vm_size_name, record in VM_SIZES["southcentralus"].items():
        aux = AuxVMSizeInfo(record)
        if aux.vcpu_count < 1:
            print(vm_size_name)


if __name__ == "__main__":
    main()
