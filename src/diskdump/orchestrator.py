import asyncio
import json
import os
import struct
import sys
import zlib
from datetime import datetime, timezone

from .blockstore import BlockStore
from .manifest import Manifest
from .protocol import (
    MSG_BATCH_HASHES, MSG_BATCH_NEEDED, MSG_BLOCK, MSG_DONE, MSG_ERROR,
    MSG_INIT, MSG_RESULT, async_recv_msg, async_send_msg,
)

DEFAULT_BATCH_BLOCKS = 80  # 80 * 128KB = 10MB


class Orchestrator:
    def __init__(self, base_dir: str, block_size: int = 131072,
                 ssh_user: str = None, sudo_hosts: set = None,
                 batch_blocks: int = DEFAULT_BATCH_BLOCKS):
        self.base_dir = base_dir
        self.block_size = block_size
        self.ssh_user = ssh_user
        self.sudo_hosts = sudo_hosts or set()
        self.batch_blocks = batch_blocks
        self.blockstore = BlockStore(base_dir)
        self.blockstore.init(block_size)
        self._client_path = os.path.join(os.path.dirname(__file__), 'client.py')

    async def dump_targets(self, targets: list, names: dict = None):
        names = names or {}
        results = {}
        async with asyncio.TaskGroup() as tg:
            deployed = set()
            for hostname, devices in targets:
                if hostname not in deployed:
                    await self._deploy_client(hostname)
                    deployed.add(hostname)
                for device in devices:
                    key = f'{hostname}:{device}'
                    manifest_name = names.get(key)
                    task = tg.create_task(self._dump_one(hostname, device, manifest_name))
                    results[key] = task
        return {k: v.result() for k, v in results.items()}

    def _ssh_target(self, hostname: str) -> str:
        if self.ssh_user:
            return f'{self.ssh_user}@{hostname}'
        return hostname

    async def _deploy_client(self, hostname: str):
        target = self._ssh_target(hostname)
        self._log(hostname, 'deploying client...')
        proc = await asyncio.create_subprocess_exec(
            'scp', '-q', self._client_path, f'{target}:/tmp/diskdump_client.py',
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f'Failed to deploy client to {hostname}: {stderr.decode()}')
        self._log(hostname, 'client deployed')

    async def _dump_one(self, hostname: str, device: str, manifest_name: str = None) -> dict:
        target = self._ssh_target(hostname)
        label = f'{hostname}:{device}'
        self._log(label, 'starting dump...')

        sudo_prefix = 'sudo ' if hostname in self.sudo_hosts else ''
        proc = await asyncio.create_subprocess_exec(
            'ssh', target,
            f'{sudo_prefix}python3 /tmp/diskdump_client.py {device} {self.block_size} {self.batch_blocks}',
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stderr_task = asyncio.create_task(self._forward_stderr(label, proc.stderr))

        try:
            msg_type, payload = await async_recv_msg(proc.stdout)
            if msg_type != MSG_INIT:
                raise RuntimeError(f'Expected INIT, got {msg_type:#x}')
            meta = json.loads(payload)
            total_blocks = meta['total_blocks']
            self._log(label, f'size: {meta["total_size"]:,} bytes, {total_blocks} blocks, '
                       f'batch: {meta["batch_blocks"]}')

            all_hashes = []
            stored = 0
            reused = 0

            while True:
                msg_type, payload = await async_recv_msg(proc.stdout)

                if msg_type == MSG_DONE:
                    break

                if msg_type == MSG_ERROR:
                    raise RuntimeError(f'Client error: {payload.decode()}')

                if msg_type != MSG_BATCH_HASHES:
                    raise RuntimeError(f'Expected BATCH_HASHES or DONE, got {msg_type:#x}')

                batch_start, batch_count = struct.unpack('!II', payload[:8])
                batch_hashes = []
                for i in range(batch_count):
                    h = payload[8 + i * 32: 8 + (i + 1) * 32].hex()
                    batch_hashes.append(h)

                needed_offsets = []
                for off, h in enumerate(batch_hashes):
                    if not self.blockstore.has_block(h):
                        needed_offsets.append(off)

                needed_payload = struct.pack('!I', len(needed_offsets))
                if needed_offsets:
                    needed_payload += struct.pack(f'!{len(needed_offsets)}I', *needed_offsets)
                await async_send_msg(proc.stdin, MSG_BATCH_NEEDED, needed_payload)

                for _ in range(len(needed_offsets)):
                    msg_type, bpayload = await async_recv_msg(proc.stdout)
                    if msg_type == MSG_ERROR:
                        raise RuntimeError(f'Client error: {bpayload.decode()}')
                    if msg_type != MSG_BLOCK:
                        raise RuntimeError(f'Expected BLOCK, got {msg_type:#x}')
                    idx, comp_len = struct.unpack('!II', bpayload[:8])
                    compressed = bpayload[8:8 + comp_len]
                    raw = zlib.decompress(compressed)
                    off = idx - batch_start
                    self.blockstore.store_block(batch_hashes[off], raw)
                    stored += 1

                reused += batch_count - len(needed_offsets)
                all_hashes.extend(batch_hashes)

            manifest = Manifest()
            manifest.source = f'{hostname}:{device}'
            manifest.date = datetime.now(timezone.utc).isoformat()
            manifest.block_size = self.block_size
            manifest.total_blocks = len(all_hashes)
            manifest.total_size = meta['total_size']
            manifest.hashes = all_hashes

            if manifest_name:
                name = manifest_name
                manifest_dir = os.path.join(self.blockstore.manifests_dir, name)
                version = Manifest._next_version(manifest_dir)
                manifest_path = os.path.join(manifest_dir, f'{version}.manifest')
            else:
                manifest_path = Manifest.output_path(self.base_dir, hostname, device)
            manifest.write(manifest_path)

            rel_manifest = os.path.relpath(manifest_path, self.base_dir)
            result = {
                'status': 'ok',
                'manifest': rel_manifest,
                'blocks_stored': stored,
                'blocks_reused': reused,
            }
            await async_send_msg(proc.stdin, MSG_RESULT, json.dumps(result).encode())

            self._log(label, f'done — stored={stored}, reused={reused}, manifest={rel_manifest}')
            return result

        except Exception as e:
            self._log(label, f'ERROR: {e}')
            try:
                await async_send_msg(proc.stdin, MSG_ERROR, str(e).encode())
            except Exception:
                pass
            raise
        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass
            await stderr_task
            await proc.wait()

    async def _forward_stderr(self, label: str, stream):
        while True:
            line = await stream.readline()
            if not line:
                break
            self._log(label, line.decode().rstrip())

    def _log(self, label: str, msg: str):
        print(f'[{label}] {msg}', file=sys.stderr, flush=True)
