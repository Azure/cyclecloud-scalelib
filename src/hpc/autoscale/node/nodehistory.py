import datetime
import os
import shutil
import sqlite3
import typing
from abc import ABC, abstractmethod

import hpc.autoscale.hpclogging as logging
from hpc.autoscale.hpctypes import Hostname, NodeId
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import partition_single

# TODO RDH reset
SQLITE_VERSION = "0.0.4"


NodeHistoryResult = typing.List[typing.Tuple[NodeId, Hostname, float]]


class NodeHistory(ABC):
    @abstractmethod
    def update(self, nodes: typing.Iterable[Node]) -> None:
        pass

    @abstractmethod
    def find_unmatched(self, for_at_least: float = 300) -> NodeHistoryResult:
        pass

    @abstractmethod
    def find_booting(self, for_at_least: float = 1800) -> NodeHistoryResult:
        pass

    def decorate(self, nodes: typing.List[Node]) -> None:
        pass


class NullNodeHistory(NodeHistory):
    def __init__(self) -> None:
        self.nodes: typing.List[Node] = []

    def update(self, nodes: typing.Iterable[Node]) -> None:
        self.nodes = list(nodes)

    def find_unmatched(self, for_at_least: float = 300) -> NodeHistoryResult:
        return [
            (n.delayed_node_id.node_id, n.hostname_required, 0)
            for n in self.nodes
            if n.delayed_node_id.node_id
        ]

    def find_booting(self, for_at_least: float = 1800) -> NodeHistoryResult:
        return [
            (n.delayed_node_id.node_id, n.hostname_required, 0)
            for n in self.nodes
            if n.delayed_node_id.node_id
        ]


def initialize_db(path: str, read_only: bool) -> sqlite3.Connection:
    try:
        if read_only:
            path = os.path.abspath(path)
            file_uri = "file://{}?mode=ro".format(path)
            conn = sqlite3.connect(file_uri, uri=True)
        else:
            file_uri = path
            conn = sqlite3.connect(path)
    except sqlite3.OperationalError as e:
        logging.exception("Error while opening %s - %s", file_uri, e)
        raise

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
        return initialize_db(path, read_only)

    try:
        conn.execute(
            """CREATE TABLE nodes (node_id TEXT PRIMARY KEY, hostname TEXT,
                                   last_match_time REAL, create_time REAL,
                                   delete_time REAL)"""
        )
    except sqlite3.OperationalError as e:
        if "table nodes already exists" not in e.args:
            raise

    return conn


class SQLiteNodeHistory(NodeHistory):
    def __init__(self, path: str = "nodehistory.db", read_only: bool = False) -> None:
        self.path = path
        self.conn = initialize_db(path, read_only)
        self.read_only = read_only
        self.last_match_timeout = self.create_timeout = 0.0

    def now(self) -> float:
        return datetime.datetime.utcnow().timestamp()

    def update(self, nodes: typing.Iterable[Node]) -> None:
        if self.read_only:
            return

        now = self.now()

        rows = list(
            self._execute(
                """SELECT node_id, hostname, last_match_time,
                          create_time from nodes where delete_time IS NULL"""
            )
        )

        rows_by_id = partition_single(rows, lambda r: r[0])

        nodes_by_id: typing.Dict[typing.Optional[NodeId], Node] = partition_single(
            list(nodes), lambda n: n.delayed_node_id.node_id,
        )

        to_delete = set(rows_by_id.keys()) - set(nodes_by_id.keys())

        for node in nodes:
            node_id = node.delayed_node_id.node_id

            if node_id not in rows_by_id:
                # first time we see it, just put an entry
                rows_by_id[node_id] = tuple([node_id, node.hostname, now, now])

            if node.required:
                rec = list(rows_by_id[node_id])
                rec[-2] = now
                rows_by_id[node_id] = tuple(rec)

        if rows_by_id:
            exprs = []
            for row in rows_by_id.values():
                node_id, hostname, match_time, create_time = row
                expr = "('{}', '{}', {}, {}, NULL)".format(
                    node_id, hostname, match_time, create_time
                )
                exprs.append(expr)

            values_expr = ",".join(exprs)

            stmt = "INSERT OR REPLACE INTO nodes (node_id, hostname, last_match_time, create_time, delete_time) VALUES {}".format(
                values_expr
            )
            self._execute(stmt)

        if to_delete:
            to_delete_expr = " OR ".join(
                ['node_id="{}"'.format(node_id) for node_id in to_delete]
            )
            now = datetime.datetime.utcnow().timestamp()
            self._execute(
                "UPDATE nodes set delete_time={} where {}".format(now, to_delete_expr)
            )

        self.retire_records(commit=True)

    def retire_records(
        self, timeout: int = (7 * 24 * 60 * 60), commit: bool = True
    ) -> None:
        if self.read_only:
            return

        retire_omega = self.now() - timeout
        cursor = self._execute(
            """DELETE from nodes where delete_time is not null AND delete_time < {} AND delete_time > 0""".format(
                retire_omega
            )
        )
        deleted = list(cursor)
        logging.info(
            "Deleted %s nodes - %s", len(deleted), [(d[0], d[1]) for d in deleted]
        )
        if commit:
            self.conn.commit()

    def find_unmatched(self, for_at_least: float = 300) -> NodeHistoryResult:
        now = datetime.datetime.utcnow().timestamp()
        omega = now - for_at_least
        return list(
            self._execute(
                "SELECT node_id, hostname, last_match_time from nodes where last_match_time < {}".format(
                    omega
                )
            )
        )

    def find_booting(self, for_at_least: float = 1800) -> NodeHistoryResult:
        now = datetime.datetime.utcnow().timestamp()
        omega = now - for_at_least
        return list(
            self._execute(
                "SELECT node_id, hostname, create_time from nodes where create_time < {}".format(
                    omega
                )
            )
        )

    def decorate(self, nodes: typing.List[Node]) -> None:
        if not nodes:
            nodes = []

        nodes = [n for n in nodes if n.exists]
        equalities = [
            " (node_id == '{}') ".format(n.delayed_node_id.node_id) for n in nodes
        ]

        if not equalities:
            return

        stmt = "select node_id, last_match_time, create_time, delete_time from nodes where {}".format(
            "{}".format(" OR ".join(equalities))
        )

        rows = self._execute(stmt)
        rows_by_id = partition_single(list(rows), lambda r: r[0])

        now = self.now()

        for node in nodes:
            node_id = node.delayed_node_id.node_id

            # should be impossible because we already filtered by exists
            if not node_id:
                logging.warning(
                    "Null node_id for node %s. Leaving create/last_match/delete times as null.",
                    node,
                )
                continue

            if node_id in rows_by_id:

                node_id, last_match_time, create_time, delete_time = rows_by_id[node_id]
                node.create_time_unix = create_time
                node.last_match_time_unix = last_match_time
                node.delete_time_unix = delete_time

                if self.create_timeout:
                    create_elapsed = max(0, now - create_time)
                    create_remaining = max(0, self.create_timeout - create_elapsed)
                    node.create_time_remaining = create_remaining

                if self.last_match_timeout:
                    match_elapsed = max(0, now - last_match_time)
                    match_remaining = max(0, self.last_match_timeout - match_elapsed)
                    node.idle_time_remaining = match_remaining

    def _execute(self, stmt: str) -> sqlite3.Cursor:
        logging.debug(stmt)
        return self.conn.execute(stmt)

    def __repr__(self) -> str:
        return "SQLiteNodeHistory({}, read_only={})".format(self.path, self.read_only)
