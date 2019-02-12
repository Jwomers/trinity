import datetime
import logging
from pathlib import Path
import sqlite3
import pytest
import tempfile

from p2p.ecies import generate_privkey
from p2p import kademlia
from p2p import persistance


# do it the long way to enable monkeypatching p2p.persistance.current_time
SQLPeerInfoPersistance = persistance.SQLPeerInfoPersistance
MemoryPeerInfoPersistance = persistance.MemoryPeerInfoPersistance


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


def random_node():
    address = kademlia.Address('127.0.0.1', 30303)
    return kademlia.Node(generate_privkey(), address)


def test_records_failures():
    # where can you get a random pubkey from?
    logger = logging.getLogger('PeerInfo')
    peer_info = MemoryPeerInfoPersistance(logger)

    node = random_node()
    assert peer_info.can_connect_to(node) is True

    peer_info.record_failure(node, 10, 'no-reason')

    assert peer_info.can_connect_to(node) is False

    # And just to make sure, check that it's been saved to the db
    db = peer_info.db
    rows = db.execute('''
        SELECT * FROM bad_nodes
    ''').fetchall()
    assert len(rows) == 1
    assert rows[0]['enode'] == node.uri()


def test_memory_does_not_persist():
    logger = logging.getLogger('PeerInfo')
    node = random_node()

    peer_info = MemoryPeerInfoPersistance(logger)
    assert peer_info.can_connect_to(node) is True
    peer_info.record_failure(node, 10, 'no-reason')
    assert peer_info.can_connect_to(node) is False
    peer_info.close()

    # open a second instance
    peer_info = MemoryPeerInfoPersistance(logger)
    # the second instance has no memory of the failure
    assert peer_info.can_connect_to(node) is True


def test_sql_does_persist(temp_path):
    dbpath = temp_path / "nodedb"
    logger = logging.getLogger('PeerInfo')
    node = random_node()

    peer_info = SQLPeerInfoPersistance(dbpath, logger)
    assert peer_info.can_connect_to(node) is True
    peer_info.record_failure(node, 10, 'no-reason')
    assert peer_info.can_connect_to(node) is False
    peer_info.close()

    # open a second instance
    peer_info = SQLPeerInfoPersistance(dbpath, logger)
    # the second instance remembers the failure
    assert peer_info.can_connect_to(node) is False
    peer_info.close()


def test_timeout_works(monkeypatch):
    logger = logging.getLogger('PeerInfo')
    node = random_node()

    current_time = datetime.datetime.utcnow()

    def get_time():
        return current_time

    monkeypatch.setattr(persistance, 'current_time', get_time)

    peer_info = MemoryPeerInfoPersistance(logger)
    assert peer_info.can_connect_to(node) is True

    peer_info.record_failure(node, 10, 'no-reason')
    assert peer_info.can_connect_to(node) is False

    current_time += datetime.timedelta(seconds=1)
    assert peer_info.can_connect_to(node) is False

    current_time += datetime.timedelta(seconds=10)
    assert peer_info.can_connect_to(node) is True


def test_fails_when_closed():
    logger = logging.getLogger('PeerInfo')
    peer_info = MemoryPeerInfoPersistance(logger)
    peer_info.close()

    node = random_node()
    with pytest.raises(persistance.ClosedException):
        peer_info.record_failure(node, 10, 'no-reason')
