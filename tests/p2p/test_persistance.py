import logging
from pathlib import Path
import sqlite3
import pytest
import tempfile

from p2p.persistance import SQLPeerInfoPersistance


@pytest.fixture
def temp_path():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)


def test_reads_schema(temp_path):
    dbpath = temp_path / "nodedb"
    logger = logging.getLogger('PeerInfo')

    # this will setup the tables
    peer_info = SQLPeerInfoPersistance(dbpath, logger)
    peer_info.close()

    # this runs a quick check that the tables were setup
    peer_info = SQLPeerInfoPersistance(dbpath, logger)
    peer_info.close()


def test_fails_when_schema_version_is_not_1(temp_path):
    dbpath = temp_path / "nodedb"
    logger = logging.getLogger('PeerInfo')

    db = sqlite3.connect(str(dbpath))
    db.execute('CREATE TABLE schema_version (version)')
    db.close()

    # there's no version information!
    with pytest.raises(Exception):
        SQLPeerInfoPersistance(dbpath, logger)

    db = sqlite3.connect(str(dbpath))
    with db:
        db.execute('INSERT INTO schema_version VALUES (2)')
    db.close()

    # version 2 is not supported!
    with pytest.raises(Exception):
        SQLPeerInfoPersistance(dbpath, logger)
