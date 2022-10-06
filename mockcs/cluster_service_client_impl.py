import json
import requests
import sys
import urllib3
import cyclecloud.api.clusters


def _get_session(config: typing.Dict) -> requests.sessions.Session:
    try:
        retries = 3
        while retries > 0:
            try:
                # if not config["verify_certificates"]:
                #     urllib3.disable_warnings(InsecureRequestWarning)

                s = requests.session()
                s.auth = (config["username"], config["password"])
                # TODO apparently this does nothing...
                # s.timeout = config["cycleserver"]["timeout"]
                s.verify = config.get("verify_certificates", True)
                s.headers = {
                    "X-Cycle-Client-Version": "%s-cli:%s" % ("hpc-autoscale", "0.0.0")
                }

                return s
            except requests.exceptions.SSLError:
                retries = retries - 1
                if retries < 1:
                    raise
    except ImportError:
        raise

    raise AssertionError(
        "Could not connect to CycleCloud. Please see the log for more details."
    )


def _make_request(config, func_name, msg):
    session = _get_session(config)
    _request_context = cyclecloud.api.clusters._RequestContext()

    _request_context.path = f"/clusterservice/{func_name}"

    _query: typing.Dict = {}
    _headers: typing.Dict = {}

    _body = msg.to_json()
    print(f"BODY is of type {type(_body)} {_body}")

    _responses = []
    _responses.append((200, "object", lambda v: v))

    _response = session.request(
        "POST",
        url=f"{config['url']}/clusterservice/{func_name}",
        # query=_query,
        json=_body,
        # body=_body,
        # expected_responses=_responses,
    )
    # print(_response)
    # print(_response.text)
    # print(dir(_response))
    return _response


def _load_config():
    return json.load(open("/Users/ryhamel/autoscale.json"))


def get_cluster_status():
    config = _load_config()

    msg = GetClusterRequestMessage(config["cluster_name"])
    _response = _make_request(config, "GetCluster", msg)
    json.dump(json.loads(_response.text), sys.stdout, indent=2)


def add_node(pool_name):
    count = int(count)
    config = _load_config()
    msg = AddNodesRequestMessage(
        config["cluster_name"], pool_name, count, False, None, "westus2"
    )
    _make_request(config, "AddNode", msg)


def remove_node(node_id):
    config = _load_config()
    msg = RemoveNodeRequestMessage(
        config["cluster_name"], node_id
    )
    _make_request(config, "RemoveNode", msg)

if __name__ == "__main__":
    f = globals()[sys.argv[1]]  
    resp = f(*sys.argv[2:])
    json.dump(resp, sys.stdout, indent=2)

