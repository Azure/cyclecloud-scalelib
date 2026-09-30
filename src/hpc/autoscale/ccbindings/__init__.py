from hpc.autoscale import hpctypes
from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale import util as hpcutil


def new_cluster_bindings(
    config: dict,
) -> ClusterBindingInterface:
    if config.get("_mock_bindings"):
        ret = config["_mock_bindings"]

        if isinstance(ret, dict):
            assert ret.get("name") in ["reproduce", None], "unsupported _mock_bindings"

            from hpc.autoscale.ccbindings import reproduce

            return reproduce.ReproduceFromResponse(ret)
        return ret
    
    from hpc.autoscale.ccbindings import legacy
    from cyclecloud.client import Client

    cluster_name = hpctypes.ClusterName(config["cluster_name"])
    config["verify_certificates"] = config.get("verify_certificates") or False
    client = Client(config)
    cluster = client.clusters.get(cluster_name)
    read_only: bool = config.get("read_only", False)
    if read_only is None:
        read_only = False

    return legacy.ClusterBinding(
        config, cluster._client.session, cluster._client, read_only=read_only
    )
    
