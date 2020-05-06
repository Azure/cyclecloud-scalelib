import json
import os
import sys
from shutil import which
from subprocess import check_output
from typing import Dict, Optional

from hpc.autoscale.node.vm_sizes import AuxVMSizeInfo
from hpc.autoscale.util import partition, partition_single


def create_vm_sizes(cache_path: Optional[str] = None) -> None:

    if cache_path and os.path.exists(cache_path):
        raw = open(cache_path).read()
    else:

        if which("labrat"):
            raw = (
                check_output(
                    ["labrat", "exec", """/labrat/scripts/az.sh vm list-skus --all"""]
                )
                .decode()
                .replace("\x1b[0m", "")  # formatting chars show up for some reason
            )

        elif which("az"):
            raw = check_output(["az", "vm", "list-skus", "--all"]).decode()
        else:
            print("You need either labrat or az cli installed.")

        if cache_path:
            with open(cache_path, "w") as fw:
                fw.write(raw)

    print("Parsing list-skus...")
    try:
        skus = json.loads(raw)
    except Exception as e:

        toks = str(e).split()
        line_no = int(toks[toks.index("line") + 1])
        print("{}: '{}'".format(e, raw.splitlines()[line_no - 1]))
        return

    print("done")

    skus = [
        s
        for s in skus
        if s.get("family") and s.get("resourceType") == "virtualMachines"
    ]

    min_skus = []
    for sku in skus:
        min_sku = {}
        for key in ["name", "family", "size", "tier"]:
            min_sku[key] = sku[key]

        assert min_sku["family"], sku
        min_sku["location"] = sku["locationInfo"][0]["location"]

        cap_list = sku["capabilities"]
        cap_dict = {}
        for entry in cap_list:
            value = entry["value"]
            if value.isdigit():
                value = int(value)
            elif value in ["True", "False"]:
                value = value == "True"
            elif "," in value:
                value = value.split(",")
            else:
                try:
                    value = float(value)
                except ValueError:
                    pass
            cap_dict[entry["name"]] = value
        min_sku["capabilities"] = cap_dict
        min_skus.append(min_sku)

    by_location = partition(min_skus, lambda s: s["location"])
    if os.path.exists("src/hpc/autoscale/node/vm_sizes.json"):
        print("reload")
        vm_sizes = json.load(open("src/hpc/autoscale/node/vm_sizes.json"))
    else:
        vm_sizes = {}
    locs = list(by_location.keys())
    a = sorted(
        by_location.items(), key=lambda x: locs.index(x[0]) if x[0] in locs else -1
    )
    for loc, loc_skus in a:
        vm_sizes[loc] = partition_single(loc_skus, lambda s: s["name"])

    if which("labrat"):
        cs_mts = json.loads(
            check_output(
                [
                    "labrat",
                    "exec",
                    "cycle_server execute --format json 'select * from Azure.MachineType'",
                ]
            ).decode()
        )
    elif which("cycle_server"):
        cs_mts = json.loads(
            check_output(
                [
                    "cycle_server",
                    "execute",
                    "--format",
                    "json",
                    "select * from Azure.MachineType",
                ]
            ).decode()
        )
    else:
        print(
            "Warning: no labrat or cycle_server found! Skipping validation",
            file=sys.stderr,
        )
        cs_mts = []

    for row in cs_mts:
        try:
            aux_info = AuxVMSizeInfo(vm_sizes[row["Location"]][row["Name"]])
            if aux_info.vcpu_count != row["CoreCount"]:

                print(
                    row,
                    aux_info.vcpu_count,
                    json.dumps(getattr(aux_info, "_AuxVMSizeInfo__record"), indent=2),
                )
                if row["Location"] not in vm_sizes:
                    vm_sizes[row["Location"]] = {}

                rec = {
                    "name": row.pop("Name"),
                    "family": row.pop("Family"),
                    "size": row.pop("SKU"),
                    "tier": row.pop("Tier"),
                    "location": row.pop("Location"),
                    "linux_price": row.get("Linux", {}).get("Regular", 0.0),
                    "windows_price": row.get("Linux", {}).get("Regular", 0.0),
                    "capabilities": row,
                }
                vm_sizes[row["Location"]][row["Name"]] = rec
                sys.exit(1)
            continue
        except KeyError:
            pass

        if row["Location"] not in vm_sizes:
            vm_sizes[row["Location"]] = {}

        vm_sizes[row["Location"]][row["Name"]]
        vm_sizes[row["Location"]]

    final_vm_sizes: Dict = {}
    for loc in sorted(vm_sizes):
        final_vm_sizes[loc] = loc_dict = {}
        for vm_size in sorted(vm_sizes[loc]):
            loc_dict[vm_size] = vm_sizes[loc][vm_size]

    with open("new_vm_sizes.json", "w") as fw:
        json.dump(final_vm_sizes, fw, indent=2)

    print(
        "Copy ./new_vm_sizes.json to ./src/hpc/autoscale/node/vm_sizes.json to complete the creation."
    )


if __name__ == "__main__":
    cache_path = None
    if len(sys.argv) == 2:
        cache_path = sys.argv[1]
    create_vm_sizes(cache_path)
