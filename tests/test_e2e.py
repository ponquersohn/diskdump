"""End-to-end tests: import -> restore -> verify -> gc cycle, plus client-server protocol."""
import hashlib
import os
import subprocess
import struct
import sys
import zlib

import pytest

from diskdump.blockstore import BlockStore
from diskdump.manifest import Manifest
from diskdump.protocol import (
    MSG_BATCH_HASHES, MSG_BATCH_NEEDED, MSG_BLOCK, MSG_DONE,
    MSG_INIT, MSG_RESULT, recv_msg, send_msg,
)
from diskdump.restore import restore

BLOCK_SIZE = 131072
PYTHON = sys.executable
CLIENT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           'src', 'diskdump', 'client.py')


def _import_image(base_dir, data, hostname='testhost', device='/dev/sda'):
    blockstore = BlockStore(base_dir)
    blockstore.init()
    hashes = []
    stored = 0
    for i in range(0, len(data), BLOCK_SIZE):
        block = data[i:i + BLOCK_SIZE]
        h = hashlib.sha256(block).hexdigest()
        hashes.append(h)
        if blockstore.store_block(h, block):
            stored += 1
    m = Manifest()
    m.source = f'{hostname}:{device}'
    m.date = '2026-04-24T00:00:00Z'
    m.block_size = BLOCK_SIZE
    m.total_blocks = len(hashes)
    m.total_size = len(data)
    m.hashes = hashes
    manifest_path = Manifest.output_path(base_dir, hostname, device)
    m.write(manifest_path)
    return manifest_path, stored, len(hashes) - stored


class TestImportRestoreCycle:
    def test_import_and_restore_match(self, tmp_base, test_image):
        img_path, original_data = test_image
        manifest_path, stored, reused = _import_image(tmp_base, original_data)
        assert stored == 8
        assert reused == 0

        blockstore = BlockStore(tmp_base)
        output_path = os.path.join(tmp_base, 'restored.img')
        restore(manifest_path, output_path, blockstore)

        with open(output_path, 'rb') as f:
            restored = f.read()
        assert hashlib.sha256(restored).hexdigest() == hashlib.sha256(original_data).hexdigest()

    def test_dedup_within_image(self, tmp_base, test_image_with_dupes):
        img_path, original_data = test_image_with_dupes
        manifest_path, stored, reused = _import_image(tmp_base, original_data)
        assert stored == 3
        assert reused == 3

        blockstore = BlockStore(tmp_base)
        output_path = os.path.join(tmp_base, 'restored.img')
        restore(manifest_path, output_path, blockstore)

        with open(output_path, 'rb') as f:
            restored = f.read()
        assert restored == original_data

    def test_dedup_across_images(self, tmp_base, test_image):
        img_path, original_data = test_image
        _, stored1, _ = _import_image(tmp_base, original_data, hostname='host1')

        _, stored2, reused2 = _import_image(tmp_base, original_data, hostname='host2')
        assert stored1 == 8
        assert stored2 == 0
        assert reused2 == 8

    def test_incremental_dump(self, tmp_base):
        data1 = os.urandom(8 * BLOCK_SIZE)
        _, stored1, _ = _import_image(tmp_base, data1)
        assert stored1 == 8

        data2 = data1[:4 * BLOCK_SIZE] + os.urandom(4 * BLOCK_SIZE)
        _, stored2, reused2 = _import_image(tmp_base, data2, hostname='host2')
        assert stored2 == 4
        assert reused2 == 4

    def test_zero_image_high_dedup(self, tmp_base, zero_image):
        img_path, original_data = zero_image
        _, stored, reused = _import_image(tmp_base, original_data)
        assert stored == 1
        assert reused == 3

        blockstore = BlockStore(tmp_base)
        stats = blockstore.stats()
        assert stats['total_blocks'] == 1


class TestGarbageCollection:
    def test_gc_removes_orphaned_blocks(self, tmp_base):
        blockstore = BlockStore(tmp_base)
        blockstore.init()

        data = os.urandom(BLOCK_SIZE)
        h = hashlib.sha256(data).hexdigest()
        blockstore.store_block(h, data)

        orphan_data = os.urandom(BLOCK_SIZE)
        orphan_h = hashlib.sha256(orphan_data).hexdigest()
        blockstore.store_block(orphan_h, orphan_data)

        m = Manifest()
        m.source = 'test:test'
        m.block_size = BLOCK_SIZE
        m.total_blocks = 1
        m.total_size = BLOCK_SIZE
        m.hashes = [h]
        m.write(Manifest.output_path(tmp_base, 'test', '/dev/test'))

        referenced = {h}
        orphaned = blockstore.all_hashes() - referenced
        for oh in orphaned:
            blockstore.remove_block(oh)

        assert blockstore.has_block(h) is True
        assert blockstore.has_block(orphan_h) is False

    def test_gc_no_orphans(self, tmp_base):
        blockstore = BlockStore(tmp_base)
        blockstore.init()

        data = os.urandom(BLOCK_SIZE)
        h = hashlib.sha256(data).hexdigest()
        blockstore.store_block(h, data)

        m = Manifest()
        m.source = 'test:test'
        m.block_size = BLOCK_SIZE
        m.total_blocks = 1
        m.total_size = BLOCK_SIZE
        m.hashes = [h]
        m.write(Manifest.output_path(tmp_base, 'test', '/dev/test'))

        orphaned = blockstore.all_hashes() - set(m.hashes)
        assert len(orphaned) == 0


class TestVerify:
    def test_verify_all_blocks_valid(self, tmp_base):
        blockstore = BlockStore(tmp_base)
        blockstore.init()

        for _ in range(5):
            data = os.urandom(BLOCK_SIZE)
            h = hashlib.sha256(data).hexdigest()
            blockstore.store_block(h, data)

        errors = 0
        for h in blockstore.all_hashes():
            data = blockstore.read_block(h)
            if hashlib.sha256(data).hexdigest() != h:
                errors += 1
        assert errors == 0


class TestClientServerProtocol:
    def _run_server_side(self, proc, blockstore):
        """Drive the server side of the protocol against a client subprocess."""
        import json

        msg_type, payload = recv_msg(proc.stdout)
        assert msg_type == MSG_INIT
        meta = json.loads(payload)

        all_hashes = []
        stored = 0

        while True:
            msg_type, payload = recv_msg(proc.stdout)
            if msg_type == MSG_DONE:
                break
            assert msg_type == MSG_BATCH_HASHES

            batch_start, batch_count = struct.unpack('!II', payload[:8])
            batch_hashes = []
            for i in range(batch_count):
                h = payload[8 + i * 32: 8 + (i + 1) * 32].hex()
                batch_hashes.append(h)

            needed = [off for off, h in enumerate(batch_hashes)
                      if not blockstore.has_block(h)]

            needed_payload = struct.pack('!I', len(needed))
            if needed:
                needed_payload += struct.pack(f'!{len(needed)}I', *needed)
            send_msg(proc.stdin, MSG_BATCH_NEEDED, needed_payload)

            for _ in range(len(needed)):
                msg_type, bpayload = recv_msg(proc.stdout)
                assert msg_type == MSG_BLOCK
                idx, comp_len = struct.unpack('!II', bpayload[:8])
                raw = zlib.decompress(bpayload[8:8 + comp_len])
                off = idx - batch_start
                blockstore.store_block(batch_hashes[off], raw)
                stored += 1

            all_hashes.extend(batch_hashes)

        result = {'status': 'ok', 'blocks_stored': stored,
                  'blocks_reused': len(all_hashes) - stored, 'manifest': 'test'}
        send_msg(proc.stdin, MSG_RESULT, json.dumps(result).encode())
        proc.stdin.close()
        proc.wait(timeout=5)

        return meta, all_hashes, stored

    def test_client_server_local(self, tmp_base, test_image):
        img_path, original_data = test_image
        blockstore = BlockStore(tmp_base)
        blockstore.init()

        proc = subprocess.Popen(
            [PYTHON, CLIENT_PATH, img_path, str(BLOCK_SIZE), '4'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        try:
            meta, all_hashes, stored = self._run_server_side(proc, blockstore)
            assert meta['total_size'] == len(original_data)
            assert proc.returncode == 0

            reconstructed = b''
            for h in all_hashes:
                reconstructed += blockstore.read_block(h)
            assert reconstructed == original_data
        finally:
            proc.kill()
            proc.wait()

    def test_client_server_with_existing_blocks(self, tmp_base, test_image):
        img_path, original_data = test_image
        blockstore = BlockStore(tmp_base)
        blockstore.init()

        first_block = original_data[:BLOCK_SIZE]
        first_hash = hashlib.sha256(first_block).hexdigest()
        blockstore.store_block(first_hash, first_block)

        proc = subprocess.Popen(
            [PYTHON, CLIENT_PATH, img_path, str(BLOCK_SIZE), '8'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        try:
            _, all_hashes, stored = self._run_server_side(proc, blockstore)
            assert stored == 7
            assert proc.returncode == 0
        finally:
            proc.kill()
            proc.wait()
