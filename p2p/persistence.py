from abc import ABC, abstractmethod
import datetime
import functools
from pathlib import Path
import sqlite3
from typing import Any, Callable, TypeVar, cast

from trinity._utils.logging import HasExtendedDebugLogger

from p2p.kademlia import Node


# a top-level function so it can be easily mocked
def current_time() -> datetime.datetime:
    return datetime.datetime.utcnow()


def time_to_str(time: datetime.datetime) -> str:
    return time.isoformat(timespec='seconds')


def str_to_time(as_str: str) -> datetime.datetime:
    # use datetime.datetime.fromisoformat once support for 3.6 is dropped
    return datetime.datetime.strptime(as_str, "%Y-%m-%dT%H:%M:%S")


class BasePeerInfoPersistence(ABC, HasExtendedDebugLogger):
    @abstractmethod
    def record_failure(self, remote: Node, timeout: int, reason: str) -> None:
        pass

    @abstractmethod
    def should_connect_to(self, remote: Node) -> bool:
        pass


class NoopPeerInfoPersistence(BasePeerInfoPersistence):
    def record_failure(self, remote: Node, timeout: int, reason: str) -> None:
        pass

    def should_connect_to(self, remote: Node) -> bool:
        return True


class ClosedException(Exception):
    # methods of SQLitePeerInfoPersistence cannot be called after it's been closed
    pass


T = TypeVar('T', bound=Callable[..., Any])


def must_be_open(func: T) -> T:
    @functools.wraps(func)
    def wrapper(self: 'SQLitePeerInfoPersistence', *args: Any, **kwargs: Any) -> Any:
        if self.closed:
            raise ClosedException()
        return func(self, *args, **kwargs)
    return cast(T, wrapper)


class SQLitePeerInfoPersistence(BasePeerInfoPersistence):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.closed = False

        # python 3.6 does not support sqlite3.connect(Path)
        self.db = sqlite3.connect(str(self.path))
        self.db.row_factory = sqlite3.Row
        self.setup_schema()

    def __str__(self) -> str:
        return f'<SQLitePeerInfo({self.path})>'

    @must_be_open
    def record_failure(self, remote: Node, timeout: int, reason: str) -> None:
        enode = remote.uri()
        row = self._fetch_node(remote)
        now = current_time()
        if row:
            new_error_count = row['error_count'] + 1
            usable_time = now + datetime.timedelta(seconds=timeout * new_error_count)
            self.logger.debug(
                '%s will not be retried until %s because %s', remote, usable_time, reason
            )
            self._update_node(enode, usable_time, reason, new_error_count)
            return

        usable_time = now + datetime.timedelta(seconds=timeout)
        self.logger.debug(
            '%s will not be retried until %s because %s', remote, usable_time, reason
        )
        self._insert_node(enode, usable_time, reason, error_count=1)

    @must_be_open
    def should_connect_to(self, remote: Node) -> bool:
        row = self._fetch_node(remote)

        if not row:
            return True

        until = str_to_time(row['until'])
        if current_time() < until:
            self.logger.debug(
                'skipping %s, it failed because "%s" and is not usable until %s',
                remote, row['reason'], row['until']
            )
            return False

        return True

    def _fetch_node(self, remote: Node) -> sqlite3.Row:
        enode = remote.uri()
        cursor = self.db.execute('SELECT * from bad_nodes WHERE enode = ?', (enode,))
        return cursor.fetchone()

    def _insert_node(self,
                     enode: str,
                     until: datetime.datetime,
                     reason: str,
                     error_count: int) -> None:
        with self.db:
            self.db.execute(
                '''
                INSERT INTO bad_nodes (enode, until, reason, error_count)
                VALUES (?, ?, ?, ?)
                ''',
                (enode, time_to_str(until), reason, error_count),
            )

    def _update_node(self,
                     enode: str,
                     until: datetime.datetime,
                     reason: str,
                     error_count: int) -> None:
        with self.db:
            self.db.execute(
                '''
                UPDATE bad_nodes
                SET until = ?, reason = ?, error_count = ?
                WHERE enode = ?
                ''',
                (time_to_str(until), reason, error_count, enode),
            )

    def close(self) -> None:
        self.db.close()
        self.db = None
        self.closed = True

    @must_be_open
    def setup_schema(self) -> None:
        try:
            if self._schema_already_created():
                return
        except Exception:
            self.close()
            raise

        with self.db:
            self.db.execute('create table bad_nodes (enode, until, reason, error_count)')
            self.db.execute('create table schema_version (version)')
            self.db.execute('insert into schema_version VALUES (1)')

    def _schema_already_created(self) -> bool:
        "Inspects the database to see if the expected tables already exist"

        count = self.db.execute("""
            SELECT count() FROM sqlite_master
            WHERE type='table' AND name='schema_version'
        """).fetchone()['count()']
        if count == 0:
            return False

        # a schema_version table already exists, get the version
        cur = self.db.execute("SELECT version FROM schema_version")
        rows = cur.fetchall()
        if len(rows) != 1:
            self.logger.error(
                "malformed nodedb. try deleting %s. (got rows: %s)",
                self.path, rows,
            )
            raise Exception(
                "malformed nodedb: Expected one row in schema_version and got %s",
                len(rows),
            )
        version = rows[0]['version']
        if version != 1:
            # in the future this block might kick off a schema migration
            self.logger.error("malformed. try deleting %s", self.path)
            raise Exception("cannot read nodedb: version %s is unsupported", version)

        # schema_version exists and is 1, this database has already been initialized!
        return True


class MemoryPeerInfoPersistence(SQLitePeerInfoPersistence):
    def __init__(self) -> None:
        super().__init__(Path(":memory:"))

    def __str__(self) -> str:
        return '<MemoryPeerInfo()>'
