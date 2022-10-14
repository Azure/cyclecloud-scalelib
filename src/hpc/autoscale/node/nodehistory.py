import datetime
import os
import shutil
import sqlite3
import typing
from abc import ABC, abstractmethod

import hpc.autoscale.hpclogging as logging
from hpc.autoscale.hpctypes import Hostname, NodeId
from hpc.autoscale.node.node import Node
from hpc.autoscale.util import parse_boot_timeout, parse_idle_timeout, partition_single

# TODO RDH reset
SQLITE_VERSION = "0.0.6"

# the keyword true/false is relatively new for sqlite, so use int values for
# backwards compatibility.
SQL_TRUE = 1
SQL_FALSE = 0


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

    @abstractmethod
    def find_ignored(self) -> NodeHistoryResult:
        pass

    @abstractmethod
    def mark_ignored(self, nodes: typing.List[Node]) -> None:
        pass

    @abstractmethod
    def unmark_ignored(self, nodes: typing.List[Node]) -> None:
        pass

    def decorate(self, nodes: typing.List[Node], config: typing.Dict = {}) -> None:
        pass

    def now(self) -> float:
        return datetime.datetime.utcnow().timestamp()


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

    def find_ignored(self) -> NodeHistoryResult:
        return [
            (n.delayed_node_id.node_id, n.hostname_required, 0)
            for n in self.nodes
            if n.metadata.get("__ignore__")
        ]

    def mark_ignored(self, nodes: typing.List[Node]) -> None:
        for n in self.nodes:
            n.metadata["__ignore__"] = True

    def unmark_ignored(self, nodes: typing.List[Node]) -> None:
        for n in self.nodes:
            n.metadata["__ignore__"] = False


def upgrade_database(current_version: str, conn: sqlite3.Connection) -> None:
    assert False, "Please contact CycleCloud support"


def initialize_db(path: str, read_only: bool, uri: bool = False) -> sqlite3.Connection:
    file_uri = path
    try:
        if read_only:
            path = os.path.abspath(path)
            # just use an in memory db if this is the first time this is run
            if not os.path.exists(path):
                file_uri = "file:memory"
            else:
                file_uri = "file://{}?mode=ro".format(path)
            conn = sqlite3.connect(file_uri, uri=True)
            # uninitialized file conns will fail here, so just
            # use memory instead
            try:
                conn = sqlite3.connect(file_uri, uri=True)
            except sqlite3.OperationalError:
                conn = sqlite3.connect("file:memory", uri=True)

        else:
            conn = sqlite3.connect(path, uri=uri)
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
        if SQLITE_VERSION > "0.0.6":
            upgrade_database(version, conn)

        conn.close()
        new_path = "{}.{}".format(path, version)
        print("Invalid version - moving to {}".format(new_path))
        shutil.move(path, new_path)
        return initialize_db(path, read_only)

    try:
        conn.execute(
            """CREATE TABLE nodes (node_id TEXT PRIMARY KEY, hostname TEXT,
                                   instance_id TEXT,
                                   create_time REAL, last_match_time REAL,
                                   ready_time REAL, delete_time REAL,
                                   ignore BOOL)"""
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

    def update(self, nodes: typing.Iterable[Node]) -> None:
        self._update(nodes)

    def _update(self, nodes: typing.Iterable[Node]) -> None:
        if self.read_only:
            return

        now = self.now()

        rows = list(
            self._execute(
                """SELECT node_id, instance_id, hostname, create_time, last_match_time, ready_time, ignore
                         from nodes where delete_time IS NULL"""
            )
        )

        rows_by_id = partition_single(rows, lambda r: r[0])
        nodes_with_ids = [n for n in nodes if n.delayed_node_id.node_id]

        nodes_by_id: typing.Dict[typing.Optional[NodeId], Node] = partition_single(
            nodes_with_ids, lambda n: n.delayed_node_id.node_id,
        )

        to_delete = set(rows_by_id.keys()) - set(nodes_by_id.keys())

        for node in nodes:
            node_id = node.delayed_node_id.node_id

            if node_id not in rows_by_id:
                # first time we see it, just put an entry
                rows_by_id[node_id] = tuple(
                    [node_id, node.instance_id, node.hostname, now, now, 0, False]
                )

            if node.required or node.state != "Ready":
                rec = list(rows_by_id[node_id])
                rec[-3] = now
                rows_by_id[node_id] = tuple(rec)

            # if a node is running a job according to the scheduler, assume it
            # is 'ready' for boot timeout purposes.
            if node.state == "Ready" or node.metadata.get("_running_job_"):
                (
                    node_id,
                    instance_id,
                    hostname,
                    create_time,
                    match_time,
                    ready_time,
                    ignore,
                ) = rows_by_id[node_id]
                if ready_time < 1:
                    rows_by_id[node_id] = tuple(
                        [
                            node_id,
                            instance_id,
                            hostname,
                            create_time,
                            match_time,
                            now,
                            ignore,
                        ]
                    )

        if rows_by_id:
            exprs = []
            for row in rows_by_id.values():
                (
                    node_id,
                    instance_id,
                    hostname,
                    create_time,
                    match_time,
                    ready_time,
                    ignore,
                ) = row
                ignore_int = SQL_TRUE if ignore else SQL_FALSE
                expr = f"('{node_id}', '{instance_id}', '{hostname}', {create_time}, {match_time}, {ready_time}, NULL, {ignore_int})".lower()

                exprs.append(expr)
            block_size = int(os.getenv("SCALELIB_SQLITE_INSERT_BLOCK", "25"))
            for i in range(0, len(exprs), block_size):
                sub_exprs = exprs[i: i + block_size]
                values_expr = ",".join(sub_exprs)
                stmt = "INSERT OR REPLACE INTO nodes (node_id, instance_id, hostname, create_time, last_match_time, ready_time, delete_time, ignore) VALUES {}".format(
                    values_expr
                )
                self._execute(stmt)

        if to_delete:
            to_delete_expr = " OR ".join(
                ['node_id="{}"'.format(node_id) for node_id in to_delete]
            )
            now = self.now()
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
        now = self.now()
        omega = now - for_at_least
        return list(
            self._execute(
                "SELECT node_id, hostname, last_match_time from nodes where last_match_time < {}".format(
                    omega
                )
            )
        )

    def find_booting(self, for_at_least: float = 1800) -> NodeHistoryResult:
        now = self.now()
        omega = now - for_at_least

        return list(
            self._execute(
                f"SELECT node_id, hostname, create_time as ctime from nodes where ctime < {omega} AND ready_time < create_time"
            )
        )

    def decorate(self, nodes: typing.List[Node], config: typing.Dict = {}) -> None:
        for i in range(0, len(nodes), 100):
            nodes_sublist = nodes[i: i + 100]
            self._decorate(nodes_sublist)

    def _decorate(self, nodes: typing.List[Node], config: typing.Dict = {}) -> None:
        if not nodes:
            nodes = []

        nodes = [n for n in nodes if n.exists]
        equalities = [
            " (node_id == '{}') ".format(n.delayed_node_id.node_id) for n in nodes
        ]

        if not equalities:
            return

        stmt = "select node_id, create_time, last_match_time, ready_time, delete_time from nodes where {}".format(
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
                    "Null node_id for %s. Leaving create/last_match/delete times as null.",
                    node,
                )
                continue

            if node_id in rows_by_id:

                (
                    node_id,
                    create_time,
                    last_match_time,
                    ready_time,
                    delete_time,
                ) = rows_by_id[node_id]

                node.create_time_unix = create_time
                node.last_match_time_unix = last_match_time
                node.delete_time_unix = delete_time
                boot_timeout = parse_boot_timeout(config, node)
                idle_timeout = parse_idle_timeout(config, node)
                if boot_timeout:
                    if ready_time < 1:
                        create_elapsed = max(0, now - create_time)
                        create_remaining = max(0, boot_timeout - create_elapsed)
                        node.create_time_remaining = create_remaining

                if idle_timeout:
                    if node.keep_alive or node.state != "Ready":
                        node.idle_time_remaining = -1
                    else:
                        match_elapsed = max(0, now - last_match_time)
                        match_remaining = max(0, idle_timeout - match_elapsed)
                        node.idle_time_remaining = match_remaining

    def find_ignored(self) -> NodeHistoryResult:
        return list(
            self._execute(
                f"SELECT node_id, hostname, create_time as ctime from nodes where ignore"
            )
        )

    def mark_ignored(self, nodes: typing.List[Node]) -> None:
        for n in nodes:
            if n.delayed_node_id:
                self._execute(
                    f"UPDATE nodes SET ignore={SQL_TRUE} WHERE node_id='{n.delayed_node_id.node_id}'"
                )
        self.conn.commit()

    def unmark_ignored(self, nodes: typing.List[Node]) -> None:
        for n in nodes:
            if n.delayed_node_id:
                self._execute(
                    f"UPDATE nodes SET ignore={SQL_FALSE} WHERE node_id='{n.delayed_node_id.node_id}'"
                )
        self.conn.commit()

    def _execute(self, stmt: str) -> sqlite3.Cursor:
        logging.debug(stmt)
        return self.conn.execute(stmt)

    def __repr__(self) -> str:
        return "SQLiteNodeHistory({}, read_only={})".format(self.path, self.read_only)
