import sys

from .blockstore import BlockStore
from .manifest import Manifest


def restore(manifest_path: str, output, blockstore: BlockStore):
    manifest = Manifest.read(manifest_path)
    total = len(manifest.hashes)

    if isinstance(output, str):
        f = open(output, 'wb')
        should_close = True
    else:
        f = output
        should_close = False

    try:
        bytes_written = 0
        for i, hash_hex in enumerate(manifest.hashes):
            block = blockstore.read_block(hash_hex)
            if manifest.total_size and i == total - 1:
                expected_remaining = manifest.total_size - bytes_written
                if expected_remaining < len(block):
                    block = block[:expected_remaining]
            f.write(block)
            bytes_written += len(block)
            if (i + 1) % 10000 == 0 or i == total - 1:
                pct = 100 * (i + 1) // total
                print(f'\rRestoring: {i + 1}/{total} blocks ({pct}%)', end='', file=sys.stderr)
        print(file=sys.stderr)
    finally:
        if should_close:
            f.close()

    return bytes_written
