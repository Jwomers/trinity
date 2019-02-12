from pathlib import Path
import sqlite3

from eth.tools.logging import ExtendedDebugLogger


class BasePeerInfoPersistance:
    def __init__(self, logger: ExtendedDebugLogger) -> None:
        if logger:
            logger = logger.getChild('PeerInfo')
        self.logger = logger


class NoopPeerInfoPersistance(BasePeerInfoPersistance):
    def __init__(self) -> None:
        super().__init__(None)


class SQLPeerInfoPersistance(BasePeerInfoPersistance):
    def __init__(self, path: Path, logger: ExtendedDebugLogger) -> None:
        super().__init__(logger)
        self.path = path
        self.closed = False

        # python 3.6 does not support sqlite3.connect(Path)
        self.db = sqlite3.connect(str(self.path))
        self.db.row_factory = sqlite3.Row
        self.setup_schema()

    def close(self) -> None:
        self.closed = True
        self.db.close()
        self.db = None

    def setup_schema(self) -> None:
        try:
            if self._schema_is_already_created():
                return
        except Exception:
            self.db.close()
            self.closed = True
            raise

        with self.db:
            self.db.execute('create table bad_nodes (enode, until, reason, error_count, kwargs)')
            self.db.execute('create table good_nodes (enode)')
            self.db.execute('create table events (enode, event, kwargs)')
            self.db.execute('create table schema_version (version)')
            self.db.execute('insert into schema_version VALUES (1)')

    def _schema_is_already_created(self) -> bool:
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


class MemoryPeerInfoPersistance(SQLPeerInfoPersistance):
    def __init__(self, logger: ExtendedDebugLogger) -> None:
        super().__init__(Path(":memory:"), logger)
