import datetime
import logging
from pathlib import Path
import sqlite3
import pytest
import tempfile

from p2p.ecies import generate_privkey
from p2p import kademlia
from p2p import persistence


# do it the long way to enable monkeypatching p2p.persistence.current_time
SQLitePeerInfoPersistence = persistence.SQLitePeerInfoPersistence
MemoryPeerInfoPersistence = persistence.MemoryPeerInfoPersistence


@pytest.fixture
def temp_path():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)


def test_has_str(temp_path):
    dbpath = temp_path / "nodedb"
    peer_info = SQLitePeerInfoPersistence(dbpath)
    assert str(peer_info) == f'<SQLitePeerInfo({str(dbpath)})>'


def test_reads_schema(temp_path):
    dbpath = temp_path / "nodedb"

    # this will setup the tables
    peer_info = SQLitePeerInfoPersistence(dbpath)
    peer_info.close()

    # this runs a quick check that the tables were setup
    peer_info = SQLitePeerInfoPersistence(dbpath)
    peer_info.close()


def test_fails_when_schema_version_is_not_1(temp_path):
    dbpath = temp_path / "nodedb"

    db = sqlite3.connect(str(dbpath))
    db.execute('CREATE TABLE schema_version (version)')
    db.close()

    # there's no version information!
    with pytest.raises(Exception):
        SQLitePeerInfoPersistence(dbpath)

    db = sqlite3.connect(str(dbpath))
    with db:
        db.execute('INSERT INTO schema_version VALUES (2)')
    db.close()

    # version 2 is not supported!
    with pytest.raises(Exception):
        SQLitePeerInfoPersistence(dbpath)


def random_node():
    address = kademlia.Address('127.0.0.1', 30303)
    return kademlia.Node(generate_privkey(), address)


def test_records_failures():
    # where can you get a random pubkey from?
    peer_info = MemoryPeerInfoPersistence()

    node = random_node()
    assert peer_info.should_connect_to(node) is True

    peer_info.record_failure(node, 10, 'no-reason')

    assert peer_info.should_connect_to(node) is False

    # And just to make sure, check that it's been saved to the db
    db = peer_info.db
    rows = db.execute('''
        SELECT * FROM bad_nodes
    ''').fetchall()
    assert len(rows) == 1
    assert rows[0]['enode'] == node.uri()


def test_memory_does_not_persist():
    node = random_node()

    peer_info = MemoryPeerInfoPersistence()
    assert peer_info.should_connect_to(node) is True
    peer_info.record_failure(node, 10, 'no-reason')
    assert peer_info.should_connect_to(node) is False
    peer_info.close()

    # open a second instance
    peer_info = MemoryPeerInfoPersistence()
    # the second instance has no memory of the failure
    assert peer_info.should_connect_to(node) is True


def test_sql_does_persist(temp_path):
    dbpath = temp_path / "nodedb"
    node = random_node()

    peer_info = SQLitePeerInfoPersistence(dbpath)
    assert peer_info.should_connect_to(node) is True
    peer_info.record_failure(node, 10, 'no-reason')
    assert peer_info.should_connect_to(node) is False
    peer_info.close()

    # open a second instance
    peer_info = SQLitePeerInfoPersistence(dbpath)
    # the second instance remembers the failure
    assert peer_info.should_connect_to(node) is False
    peer_info.close()


def test_timeout_works(monkeypatch):
    node = random_node()

    current_time = datetime.datetime.utcnow()

    class patched_datetime(datetime.datetime):
        @classmethod
        def utcnow(cls):
            return current_time

    monkeypatch.setattr(datetime, 'datetime', patched_datetime)

    peer_info = MemoryPeerInfoPersistence()
    assert peer_info.should_connect_to(node) is True

    peer_info.record_failure(node, 10, 'no-reason')
    assert peer_info.should_connect_to(node) is False

    current_time += datetime.timedelta(seconds=1)
    assert peer_info.should_connect_to(node) is False

    current_time += datetime.timedelta(seconds=10)
    assert peer_info.should_connect_to(node) is True


def test_fails_when_closed():
    peer_info = MemoryPeerInfoPersistence()
    peer_info.close()

    node = random_node()
    with pytest.raises(persistence.ClosedException):
        peer_info.record_failure(node, 10, 'no-reason')
