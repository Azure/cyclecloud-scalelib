import json
import os
from typing import Any, Dict, Optional

import hpc.autoscale.hpclogging as logging
from hpc.autoscale import hpctypes as ht

RESOURCE_FILE = os.path.join(os.path.dirname(__file__), "vm_sizes.json")
VM_SIZES = {}

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
        return self.__capabilities


def get_aux_vm_size_info(location: str, vm_size: str) -> AuxVMSizeInfo:
    by_name = VM_SIZES.get(location)

    if not by_name:
        return AuxVMSizeInfo({"family": "unknown"})

    vm_aux_info = by_name.get(vm_size)
    if not vm_aux_info:
        return AuxVMSizeInfo({"family": "unknown"})

    return AuxVMSizeInfo(vm_aux_info)


def main() -> None:
    record = VM_SIZES["southcentralus"]["Standard_M208ms_v2"]
    for vm_size_name, record in VM_SIZES["southcentralus"].items():
        aux = AuxVMSizeInfo(record)
        if aux.vcpu_count < 1:
            print(vm_size_name)


if __name__ == "__main__":
    main()
