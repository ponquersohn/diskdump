import hashlib
import io
import os

from diskdump.blockstore import BlockStore
from diskdump.manifest import Manifest
from diskdump.restore import restore


def _import_image(blockstore, data, block_size=131072):
    hashes = []
    for i in range(0, len(data), block_size):
        block = data[i:i + block_size]
        h = hashlib.sha256(block).hexdigest()
        blockstore.store_block(h, block)
        hashes.append(h)
    return hashes


def test_restore_to_file(tmp_base, test_image):
    img_path, original_data = test_image
    blockstore = BlockStore(tmp_base)
    blockstore.init()
    hashes = _import_image(blockstore, original_data)

    m = Manifest()
    m.source = 'test:test'
    m.block_size = 131072
    m.total_blocks = len(hashes)
    m.total_size = len(original_data)
    m.hashes = hashes
    manifest_path = Manifest.output_path(tmp_base, 'test', '/dev/test')
    m.write(manifest_path)

    output_path = os.path.join(tmp_base, 'restored.img')
    restore(manifest_path, output_path, blockstore)

    with open(output_path, 'rb') as f:
        restored = f.read()
    assert restored == original_data


def test_restore_to_stream(tmp_base, test_image):
    img_path, original_data = test_image
    blockstore = BlockStore(tmp_base)
    blockstore.init()
    hashes = _import_image(blockstore, original_data)

    m = Manifest()
    m.source = 'test:test'
    m.block_size = 131072
    m.total_blocks = len(hashes)
    m.total_size = len(original_data)
    m.hashes = hashes
    manifest_path = Manifest.output_path(tmp_base, 'test', '/dev/test')
    m.write(manifest_path)

    buf = io.BytesIO()
    restore(manifest_path, buf, blockstore)
    assert buf.getvalue() == original_data


def test_restore_with_deduped_blocks(tmp_base, test_image_with_dupes):
    img_path, original_data = test_image_with_dupes
    blockstore = BlockStore(tmp_base)
    blockstore.init()
    hashes = _import_image(blockstore, original_data)

    m = Manifest()
    m.source = 'test:test'
    m.block_size = 131072
    m.total_blocks = len(hashes)
    m.total_size = len(original_data)
    m.hashes = hashes
    manifest_path = Manifest.output_path(tmp_base, 'test', '/dev/test')
    m.write(manifest_path)

    output_path = os.path.join(tmp_base, 'restored.img')
    restore(manifest_path, output_path, blockstore)

    with open(output_path, 'rb') as f:
        restored = f.read()
    assert restored == original_data
    assert len(set(hashes)) < len(hashes)


def test_restore_truncates_last_block(tmp_base):
    blockstore = BlockStore(tmp_base)
    blockstore.init()

    total_size = 131072 + 50000
    data = os.urandom(total_size)
    block1 = data[:131072]
    block2 = data[131072:]
    block2_padded = block2 + b'\x00' * (131072 - len(block2))

    h1 = hashlib.sha256(block1).hexdigest()
    h2 = hashlib.sha256(block2_padded).hexdigest()
    blockstore.store_block(h1, block1)
    blockstore.store_block(h2, block2_padded)

    m = Manifest()
    m.source = 'test:test'
    m.block_size = 131072
    m.total_blocks = 2
    m.total_size = total_size
    m.hashes = [h1, h2]
    manifest_path = Manifest.output_path(tmp_base, 'test', '/dev/test')
    m.write(manifest_path)

    buf = io.BytesIO()
    bytes_written = restore(manifest_path, buf, blockstore)
    assert bytes_written == total_size
    assert buf.getvalue() == data


def test_restore_zero_image(tmp_base, zero_image):
    img_path, original_data = zero_image
    blockstore = BlockStore(tmp_base)
    blockstore.init()
    hashes = _import_image(blockstore, original_data)

    assert len(set(hashes)) == 1

    m = Manifest()
    m.source = 'test:test'
    m.block_size = 131072
    m.total_blocks = len(hashes)
    m.total_size = len(original_data)
    m.hashes = hashes
    manifest_path = Manifest.output_path(tmp_base, 'test', '/dev/test')
    m.write(manifest_path)

    output_path = os.path.join(tmp_base, 'restored.img')
    restore(manifest_path, output_path, blockstore)

    with open(output_path, 'rb') as f:
        restored = f.read()
    assert restored == original_data
