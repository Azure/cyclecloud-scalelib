import datetime
import logging
import shutil
import sqlite3
import typing
from abc import ABC, abstractmethod

from hpc.autoscale.hpctypes import Hostname
from hpc.autoscale.node.node import Node

# TODO RDH reset
SQLITE_VERSION = "0.0.2"


NodeHistoryResult = typing.List[typing.Tuple[Hostname, float]]


class NodeHistory(ABC):
    @abstractmethod
    def update(self, nodes: typing.Iterable[Node]) -> None:
        pass

    @abstractmethod
    def find_unmatched(self, for_at_least: float = 300) -> NodeHistoryResult:
        pass


class NullNodeHistory(NodeHistory):
    def __init__(self) -> None:
        self.nodes: typing.List[Node] = []

    def update(self, nodes: typing.Iterable[Node]) -> None:
        self.nodes = list(nodes)

    def find_unmatched(self, for_at_least: float = 300) -> NodeHistoryResult:
        return [(n.hostname_required, 0) for n in self.nodes]


def initialize_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE metadata (version)")
    except sqlite3.OperationalError as e:
        if "table metadata already exists" not in e.args:
            raise

    cursor = conn.execute("SELECT version from metadata")
    version_result = list(cursor)
    if version_result:
        version = version_result[0][0]
    else:
        conn.execute(
            "INSERT INTO metadata (version) VALUES ('{}')".format(SQLITE_VERSION)
        )
        version = SQLITE_VERSION

    if version != SQLITE_VERSION:
        conn.close()
        new_path = "{}.{}".format(path, version)
        print("Invalid version - moving to {}".format(new_path))
        shutil.move(path, new_path)
        return initialize_db(path)

    try:
        conn.execute(
            "CREATE TABLE nodes (hostname TEXT PRIMARY KEY, last_match_time REAL)"
        )
    except sqlite3.OperationalError as e:
        if "table nodes already exists" not in e.args:
            raise

    return conn


class SQLiteNodeHistory(NodeHistory):
    def __init__(self, path: str = "nodehistory.db") -> None:
        self.conn = initialize_db(path)

    def update(self, nodes: typing.Iterable[Node]) -> None:
        now = datetime.datetime.utcnow().timestamp()
        node_match_times = dict(
            list(self.conn.execute("SELECT hostname, last_match_time from nodes"))
        )
        to_delete = set(node_match_times.keys()) - set([h.hostname for h in nodes])

        for node in nodes:
            if node.hostname not in node_match_times:
                # first time we see it, just put an entry
                node_match_times[node.hostname] = now
            if node.required:
                node_match_times[node.hostname] = now

        if node_match_times:
            values_expr = ",".join(
                [
                    "('{}', {})".format(hostname, match_time)
                    for hostname, match_time in node_match_times.items()
                ]
            )
            stmt = "INSERT OR REPLACE INTO nodes (hostname, last_match_time) VALUES {}".format(
                values_expr
            )
            logging.log(logging.DEBUG // 2, stmt)
            self.conn.execute(stmt)

        if to_delete:
            to_delete_expr = " OR ".join(
                ['hostname="{}"'.format(hostname) for hostname in to_delete]
            )
            stmt = "DELETE FROM nodes where {}".format(to_delete_expr)

            self.conn.execute(stmt)

        self.conn.commit()

    def find_unmatched(self, for_at_least: float = 300) -> NodeHistoryResult:
        now = datetime.datetime.utcnow().timestamp()
        omega = now - for_at_least
        return list(
            self.conn.execute(
                "SELECT hostname, last_match_time from nodes where last_match_time < {}".format(
                    omega
                )
            )
        )
