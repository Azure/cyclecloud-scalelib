from hpc.autoscale import hpctypes
from hpc.autoscale.ccbindings.interface import ClusterBindingInterface
from hpc.autoscale import util as hpcutil


def new_cluster_bindings(
    config: dict,
) -> ClusterBindingInterface:
    if config.get("_mock_bindings"):
        return config["_mock_bindings"]
    if hpcutil.LEGACY:
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
    else:
        from hpc.autoscale.ccbindings import cluster_service

        # from cyclecloud.client import Client

        # cluster_name = hpctypes.ClusterName(config["cluster_name"])
        # config["verify_certificates"] = config.get("verify_certificates") or False
        # client = Client(config)
        # cluster = client.clusters.get(cluster_name)
        # read_only: bool = config.get("read_only", False)
        # if read_only is None:
        #     read_only = False

        return cluster_service.ClusterServiceBinding(config)
