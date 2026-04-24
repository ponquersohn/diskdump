import hashlib
import os

import pytest


@pytest.fixture
def tmp_base(tmp_path):
    return str(tmp_path)


@pytest.fixture
def blockstore(tmp_base):
    from diskdump.blockstore import BlockStore
    bs = BlockStore(tmp_base)
    bs.init()
    return bs


@pytest.fixture
def sample_block():
    return os.urandom(131072)


@pytest.fixture
def sample_block_hash(sample_block):
    return hashlib.sha256(sample_block).hexdigest()


@pytest.fixture
def test_image(tmp_path):
    path = str(tmp_path / 'test.img')
    data = os.urandom(8 * 131072)
    with open(path, 'wb') as f:
        f.write(data)
    return path, data


@pytest.fixture
def test_image_with_dupes(tmp_path):
    path = str(tmp_path / 'dupes.img')
    block_a = os.urandom(131072)
    block_b = os.urandom(131072)
    block_c = os.urandom(131072)
    data = block_a + block_b + block_a + block_c + block_b + block_a
    with open(path, 'wb') as f:
        f.write(data)
    return path, data


@pytest.fixture
def zero_image(tmp_path):
    path = str(tmp_path / 'zeros.img')
    data = b'\x00' * (4 * 131072)
    with open(path, 'wb') as f:
        f.write(data)
    return path, data
