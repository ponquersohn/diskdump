import hashlib
import json
import os

from diskdump.blockstore import BlockStore


def test_init_creates_config(tmp_base):
    bs = BlockStore(tmp_base)
    bs.init()
    config_path = os.path.join(bs.base, 'config.json')
    assert os.path.exists(config_path)
    with open(config_path) as f:
        config = json.load(f)
    assert config['block_size'] == 131072
    assert config['hash_algo'] == 'sha256'
    assert config['version'] == 1


def test_init_idempotent(tmp_base):
    bs = BlockStore(tmp_base)
    bs.init(block_size=131072)
    bs.init(block_size=999)
    config = bs.load_config()
    assert config['block_size'] == 131072


def test_init_custom_block_size(tmp_base):
    bs = BlockStore(tmp_base)
    bs.init(block_size=65536)
    config = bs.load_config()
    assert config['block_size'] == 65536


def test_store_and_read(blockstore, sample_block, sample_block_hash):
    assert blockstore.store_block(sample_block_hash, sample_block) is True
    data = blockstore.read_block(sample_block_hash)
    assert data == sample_block


def test_store_returns_false_on_duplicate(blockstore, sample_block, sample_block_hash):
    assert blockstore.store_block(sample_block_hash, sample_block) is True
    assert blockstore.store_block(sample_block_hash, sample_block) is False


def test_has_block(blockstore, sample_block, sample_block_hash):
    assert blockstore.has_block(sample_block_hash) is False
    blockstore.store_block(sample_block_hash, sample_block)
    assert blockstore.has_block(sample_block_hash) is True


def test_remove_block(blockstore, sample_block, sample_block_hash):
    blockstore.store_block(sample_block_hash, sample_block)
    assert blockstore.remove_block(sample_block_hash) is True
    assert blockstore.has_block(sample_block_hash) is False
    assert blockstore.remove_block(sample_block_hash) is False


def test_block_path_two_level_nesting(blockstore, sample_block_hash):
    path = blockstore._block_path(sample_block_hash)
    parts = path.split(os.sep)
    assert parts[-3] == sample_block_hash[:2]
    assert parts[-2] == sample_block_hash[2:4]
    assert parts[-1] == sample_block_hash + '.lz4'


def test_stats(blockstore):
    for i in range(5):
        data = os.urandom(131072)
        h = hashlib.sha256(data).hexdigest()
        blockstore.store_block(h, data)

    stats = blockstore.stats()
    assert stats['total_blocks'] == 5
    assert stats['total_compressed_bytes'] > 0


def test_all_hashes(blockstore):
    stored = set()
    for i in range(3):
        data = os.urandom(131072)
        h = hashlib.sha256(data).hexdigest()
        blockstore.store_block(h, data)
        stored.add(h)

    assert blockstore.all_hashes() == stored


def test_stats_empty(blockstore):
    stats = blockstore.stats()
    assert stats['total_blocks'] == 0
    assert stats['total_compressed_bytes'] == 0


def test_store_data_integrity(blockstore):
    for _ in range(10):
        data = os.urandom(131072)
        h = hashlib.sha256(data).hexdigest()
        blockstore.store_block(h, data)
        retrieved = blockstore.read_block(h)
        assert hashlib.sha256(retrieved).hexdigest() == h


def test_read_nonexistent_raises(blockstore):
    try:
        blockstore.read_block('00' * 32)
        assert False, 'Should have raised'
    except FileNotFoundError:
        pass


def test_zero_block(blockstore):
    data = b'\x00' * 131072
    h = hashlib.sha256(data).hexdigest()
    blockstore.store_block(h, data)
    assert blockstore.read_block(h) == data


def test_reindex(blockstore):
    hashes = set()
    for _ in range(5):
        data = os.urandom(131072)
        h = hashlib.sha256(data).hexdigest()
        blockstore.store_block(h, data)
        hashes.add(h)

    assert blockstore.stats()['total_blocks'] == 5

    blockstore._conn().execute('DELETE FROM blocks')
    blockstore._conn().commit()
    assert blockstore.stats()['total_blocks'] == 0

    count = blockstore.reindex()
    assert count == 5
    assert blockstore.all_hashes() == hashes
    assert blockstore.stats()['total_blocks'] == 5
