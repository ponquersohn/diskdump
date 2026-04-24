#!/usr/bin/env python3
"""diskdump client — runs on remote machine, communicates over stdin/stdout.
Single-pass streaming: reads device in batches, hashes, asks server which
blocks are needed, sends them from memory. No second read pass."""
import fcntl
import hashlib
import json
import os
import struct
import sys
import zlib

MSG_INIT         = 0x01
MSG_BATCH_HASHES = 0x02
MSG_BLOCK        = 0x03
MSG_DONE         = 0x04
MSG_BATCH_NEEDED = 0x10
MSG_RESULT       = 0x11
MSG_ERROR        = 0xFF

stdin_bin = sys.stdin.buffer
stdout_bin = sys.stdout.buffer


def send_msg(msg_type, payload=b''):
    header = struct.pack('!IB', len(payload) + 1, msg_type)
    stdout_bin.write(header)
    if payload:
        stdout_bin.write(payload)
    stdout_bin.flush()


def recv_msg():
    header = _read_exact(stdin_bin, 5)
    length, msg_type = struct.unpack('!IB', header)
    payload = _read_exact(stdin_bin, length - 1) if length > 1 else b''
    return msg_type, payload


def _read_exact(stream, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError(f'Expected {n} bytes, got {len(buf)}')
        buf.extend(chunk)
    return bytes(buf)


def get_device_size(path):
    with open(path, 'rb') as f:
        try:
            buf = b' ' * 8
            BLKGETSIZE64 = 0x80081272
            buf = fcntl.ioctl(f.fileno(), BLKGETSIZE64, buf)
            return struct.unpack('Q', buf)[0]
        except (OSError, IOError):
            f.seek(0, 2)
            return f.tell()


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def main():
    if len(sys.argv) < 4:
        print('Usage: diskdump_client.py <device> <block_size> <batch_blocks>', file=sys.stderr)
        sys.exit(1)

    device = sys.argv[1]
    block_size = int(sys.argv[2])
    batch_blocks = int(sys.argv[3])

    total_size = get_device_size(device)
    total_blocks = (total_size + block_size - 1) // block_size

    log(f'Device: {device}, size: {total_size}, blocks: {total_blocks}, '
        f'block_size: {block_size}, batch: {batch_blocks}')

    init_data = json.dumps({
        'version': 1,
        'hostname': os.uname().nodename,
        'device': device,
        'block_size': block_size,
        'batch_blocks': batch_blocks,
        'total_blocks': total_blocks,
        'total_size': total_size,
    }).encode()
    send_msg(MSG_INIT, init_data)

    total_sent = 0
    total_skipped = 0

    with open(device, 'rb') as f:
        block_idx = 0
        while block_idx < total_blocks:
            batch_end = min(block_idx + batch_blocks, total_blocks)
            batch_size = batch_end - block_idx

            blocks = []
            hashes_raw = []
            for _ in range(batch_size):
                data = f.read(block_size)
                if not data:
                    break
                blocks.append(data)
                hashes_raw.append(hashlib.sha256(data).digest())

            actual_batch = len(blocks)
            if actual_batch == 0:
                break

            payload = struct.pack('!II', block_idx, actual_batch) + b''.join(hashes_raw)
            send_msg(MSG_BATCH_HASHES, payload)

            msg_type, resp = recv_msg()
            if msg_type == MSG_ERROR:
                log(f'Server error: {resp.decode()}')
                sys.exit(1)
            if msg_type != MSG_BATCH_NEEDED:
                log(f'Unexpected message type: {msg_type:#x}')
                sys.exit(1)

            num_needed = struct.unpack('!I', resp[:4])[0]
            needed_offsets = set()
            if num_needed:
                needed_offsets = set(struct.unpack(f'!{num_needed}I', resp[4:]))

            for off in sorted(needed_offsets):
                compressed = zlib.compress(blocks[off], 1)
                block_payload = struct.pack('!II', block_idx + off, len(compressed)) + compressed
                send_msg(MSG_BLOCK, block_payload)

            total_sent += num_needed
            total_skipped += actual_batch - num_needed

            pct = 100 * batch_end // total_blocks
            log(f'  [{pct:3d}%] blocks {block_idx}-{batch_end - 1}: '
                f'sent={num_needed}, skipped={actual_batch - num_needed}')

            block_idx = batch_end

    send_msg(MSG_DONE)
    log(f'Waiting for result... (total sent={total_sent}, skipped={total_skipped})')

    msg_type, payload = recv_msg()
    if msg_type == MSG_RESULT:
        result = json.loads(payload)
        log(f'Done: {result.get("status", "unknown")} — '
            f'stored={result.get("blocks_stored", 0)}, '
            f'reused={result.get("blocks_reused", 0)}, '
            f'manifest={result.get("manifest", "")}')
    elif msg_type == MSG_ERROR:
        log(f'Server error: {payload.decode()}')
        sys.exit(1)


if __name__ == '__main__':
    main()
