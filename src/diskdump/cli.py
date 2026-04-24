"""diskdump CLI — deduplicated disk image transfer over SSH."""
import argparse
import asyncio
import hashlib
import json
import os
import sys

from .blockstore import BlockStore
from .config import DEFAULT_BLOCK_SIZE
from .manifest import Manifest
from .orchestrator import Orchestrator
from .restore import restore


def get_base_dir():
    return os.environ.get('DISKDUMP_BASE', os.getcwd())


def parse_device(dev: str) -> str:
    if dev.startswith('/'):
        return dev
    if dev.startswith('sd') or dev.startswith('nvme') or dev.startswith('vd'):
        return f'/dev/{dev}'
    return dev


def parse_dump_args(argv: list) -> dict:
    """Parse dump args with per-target --sudo and --as flags.

    Usage: dump [--user USER] [--batch-size N] [--block-size N]
                TARGET [--sudo] [--as NAME] [TARGET [--sudo] [--as NAME] ...]

    --sudo and --as must follow a target. Error if they appear before any target.
    A TARGET is host:path[:path...]. Each path within a target becomes a separate
    dump job, and --sudo/--as apply to all paths in that target.
    """
    targets = []
    names = {}
    sudo_hosts = set()
    user = None
    batch_blocks = 80
    block_size = DEFAULT_BLOCK_SIZE

    current_target = None
    i = 0
    while i < len(argv):
        arg = argv[i]

        if arg in ('--user', '-u'):
            i += 1
            if i >= len(argv):
                print('--user requires a value', file=sys.stderr)
                sys.exit(1)
            user = argv[i]
        elif arg == '--batch-size':
            i += 1
            if i >= len(argv):
                print('--batch-size requires a value', file=sys.stderr)
                sys.exit(1)
            batch_blocks = int(argv[i])
        elif arg == '--block-size':
            i += 1
            if i >= len(argv):
                print('--block-size requires a value', file=sys.stderr)
                sys.exit(1)
            block_size = int(argv[i])
        elif arg == '--sudo':
            if current_target is None:
                print('--sudo must follow a target', file=sys.stderr)
                sys.exit(1)
            sudo_hosts.add(current_target[0])
        elif arg == '--as':
            if current_target is None:
                print('--as must follow a target', file=sys.stderr)
                sys.exit(1)
            i += 1
            if i >= len(argv):
                print('--as requires a value', file=sys.stderr)
                sys.exit(1)
            hostname, devices = current_target
            for dev in devices:
                names[f'{hostname}:{dev}'] = argv[i]
        elif ':' in arg:
            if current_target is not None:
                targets.append(current_target)
            parts = arg.split(':')
            hostname = parts[0]
            devices = [parse_device(p) for p in parts[1:]]
            current_target = (hostname, devices)
        else:
            print(f'Unexpected argument: {arg}', file=sys.stderr)
            sys.exit(1)
        i += 1

    if current_target is not None:
        targets.append(current_target)

    return {
        'targets': targets,
        'names': names,
        'sudo_hosts': sudo_hosts,
        'user': user,
        'batch_blocks': batch_blocks,
        'block_size': block_size,
    }


def cmd_dump(dump_argv: list):
    base_dir = get_base_dir()
    parsed = parse_dump_args(dump_argv)
    targets = parsed['targets']
    names = parsed['names']
    sudo_hosts = parsed['sudo_hosts']

    if not targets:
        print('No targets specified', file=sys.stderr)
        sys.exit(1)

    print('Targets:', file=sys.stderr)
    for host, devs in targets:
        for d in devs:
            key = f'{host}:{d}'
            flags = []
            if host in sudo_hosts:
                flags.append('sudo')
            if key in names:
                flags.append(f'as={names[key]}')
            suffix = f' [{", ".join(flags)}]' if flags else ''
            print(f'  {key}{suffix}', file=sys.stderr)

    orch = Orchestrator(base_dir, block_size=parsed['block_size'], ssh_user=parsed['user'],
                        sudo_hosts=sudo_hosts, batch_blocks=parsed['batch_blocks'])
    results = asyncio.run(orch.dump_targets(targets, names=names))

    print('\n=== Summary ===', file=sys.stderr)
    for key, result in results.items():
        if result['status'] == 'ok':
            total = result['blocks_stored'] + result['blocks_reused']
            dedup_pct = 100 * result['blocks_reused'] // max(total, 1)
            print(f'  {key}: {result["manifest"]} '
                  f'(stored={result["blocks_stored"]}, reused={result["blocks_reused"]}, dedup={dedup_pct}%)',
                  file=sys.stderr)


def _resolve_manifest(base_dir, path):
    if os.path.isabs(path):
        return path
    under_manifests = os.path.join(base_dir, '.blockstore', 'manifests', path)
    if os.path.exists(under_manifests):
        return under_manifests
    return os.path.join(base_dir, path)


def cmd_restore(args):
    base_dir = get_base_dir()
    blockstore = BlockStore(base_dir)
    manifest_path = _resolve_manifest(base_dir, args.manifest)

    if args.output:
        print(f'Restoring {args.manifest} -> {args.output}', file=sys.stderr)
        restore(manifest_path, args.output, blockstore)
    else:
        restore(manifest_path, sys.stdout.buffer, blockstore)


def cmd_info(args):
    base_dir = get_base_dir()
    manifest_path = _resolve_manifest(base_dir, args.manifest)

    m = Manifest.read(manifest_path)
    blockstore = BlockStore(base_dir)

    unique_hashes = set(m.hashes)
    missing = sum(1 for h in unique_hashes if not blockstore.has_block(h))

    print(f'Source:       {m.source}')
    print(f'Date:         {m.date}')
    print(f'Block size:   {m.block_size:,}')
    print(f'Total blocks: {m.total_blocks:,}')
    print(f'Total size:   {m.total_size:,} bytes ({m.total_size / (1024**3):.2f} GB)')
    print(f'Unique blocks:{len(unique_hashes):,}')
    if missing:
        print(f'MISSING:      {missing} blocks not in store!')
    else:
        print(f'All blocks present in store')


def cmd_status(args):
    base_dir = get_base_dir()
    blockstore = BlockStore(base_dir)
    if not os.path.exists(blockstore.base):
        print('Block store not initialized. Run a dump first.', file=sys.stderr)
        sys.exit(1)

    stats = blockstore.stats()
    config = blockstore.load_config()

    manifests = _find_manifests(blockstore.manifests_dir)

    print(f'Block store:  {blockstore.base}')
    print(f'Block size:   {config["block_size"]:,}')
    print(f'Total blocks: {stats["total_blocks"]:,}')
    print(f'Compressed:   {stats["total_compressed_bytes"]:,} bytes '
          f'({stats["total_compressed_bytes"] / (1024**3):.2f} GB)')
    print(f'Manifests:    {len(manifests)}')
    for m_path in sorted(manifests):
        rel = os.path.relpath(m_path, blockstore.manifests_dir)
        print(f'  {rel}')


def _find_manifests(manifests_dir):
    manifests = []
    if not os.path.isdir(manifests_dir):
        return manifests
    for root, dirs, files in os.walk(manifests_dir):
        for f in files:
            if f.endswith('.manifest'):
                manifests.append(os.path.join(root, f))
    return manifests


def cmd_gc(args):
    base_dir = get_base_dir()
    blockstore = BlockStore(base_dir)

    manifests = _find_manifests(blockstore.manifests_dir)

    referenced = set()
    for m_path in manifests:
        m = Manifest.read(m_path)
        referenced.update(m.hashes)

    all_hashes = blockstore.all_hashes()
    orphaned = all_hashes - referenced

    if not orphaned:
        print('No orphaned blocks found.')
        return

    if args.dry_run:
        print(f'Would remove {len(orphaned)} orphaned blocks')
        return

    print(f'Removing {len(orphaned)} orphaned blocks...')
    for h in orphaned:
        blockstore.remove_block(h)
    print('Done.')


def cmd_verify(args):
    base_dir = get_base_dir()
    blockstore = BlockStore(base_dir)
    errors = 0
    checked = 0

    for l1 in os.scandir(blockstore.base):
        if not l1.is_dir() or len(l1.name) != 2:
            continue
        for l2 in os.scandir(l1.path):
            if not l2.is_dir() or len(l2.name) != 2:
                continue
            for block_entry in os.scandir(l2.path):
                if not block_entry.name.endswith('.lz4'):
                    continue
                hash_hex = block_entry.name[:-4]
                checked += 1
                try:
                    data = blockstore.read_block(hash_hex)
                    actual_hash = hashlib.sha256(data).hexdigest()
                    if actual_hash != hash_hex:
                        print(f'CORRUPT: {hash_hex} (actual: {actual_hash})')
                        errors += 1
                except Exception as e:
                    print(f'ERROR reading {hash_hex}: {e}')
                    errors += 1
                if checked % 10000 == 0:
                    print(f'  checked {checked}...', file=sys.stderr)

    print(f'Verified {checked} blocks, {errors} errors.')


def cmd_reindex(args):
    base_dir = get_base_dir()
    blockstore = BlockStore(base_dir)
    print('Rebuilding block database from filesystem...', file=sys.stderr)
    count = blockstore.reindex()
    print(f'Indexed {count} blocks.', file=sys.stderr)


def cmd_import(args):
    base_dir = get_base_dir()
    blockstore = BlockStore(base_dir)
    blockstore.init(args.block_size)

    img_path = args.image
    if not os.path.isabs(img_path):
        img_path = os.path.join(base_dir, img_path)

    if not args.name:
        print('--as HOSTNAME:DEVICE is required', file=sys.stderr)
        sys.exit(1)

    parts = args.name.split(':')
    hostname = parts[0]
    device = parts[1] if len(parts) > 1 else '/dev/sda'

    total_size = os.path.getsize(img_path)
    total_blocks = (total_size + args.block_size - 1) // args.block_size

    print(f'Importing {img_path} ({total_size:,} bytes, {total_blocks} blocks)')

    hashes = []
    stored = 0
    reused = 0

    with open(img_path, 'rb') as f:
        for i in range(total_blocks):
            block = f.read(args.block_size)
            if not block:
                break
            h = hashlib.sha256(block).hexdigest()
            hashes.append(h)
            if blockstore.store_block(h, block):
                stored += 1
            else:
                reused += 1
            if (i + 1) % 10000 == 0 or i == total_blocks - 1:
                print(f'\r  {i + 1}/{total_blocks} ({100 * (i + 1) // total_blocks}%)', end='', file=sys.stderr)

    print(file=sys.stderr)

    manifest = Manifest()
    manifest.source = f'{hostname}:{device}'
    if args.date:
        manifest.date = args.date
    else:
        from datetime import datetime, timezone
        manifest.date = datetime.now(timezone.utc).isoformat()
    manifest.block_size = args.block_size
    manifest.total_blocks = len(hashes)
    manifest.total_size = total_size
    manifest.hashes = hashes

    manifest_path = Manifest.output_path(base_dir, hostname, device)
    manifest.write(manifest_path)

    rel = os.path.relpath(manifest_path, base_dir)
    print(f'Manifest: {rel}')
    print(f'Blocks: stored={stored}, reused={reused}')


def main():
    parser = argparse.ArgumentParser(description='Deduplicated disk image transfer over SSH')
    parser.add_argument('--block-size', type=int, default=DEFAULT_BLOCK_SIZE)
    subparsers = parser.add_subparsers(dest='command')

    subparsers.add_parser('dump', help='Dump disk images from remote machines')

    p_restore = subparsers.add_parser('restore', help='Restore image from manifest')
    p_restore.add_argument('manifest', help='Path to .manifest file')
    p_restore.add_argument('--output', '-o', help='Output file (default: stdout)')

    p_info = subparsers.add_parser('info', help='Show manifest info')
    p_info.add_argument('manifest')

    subparsers.add_parser('status', help='Show block store status')

    p_gc = subparsers.add_parser('gc', help='Remove orphaned blocks')
    p_gc.add_argument('--dry-run', action='store_true')

    subparsers.add_parser('verify', help='Verify block store integrity')
    subparsers.add_parser('reindex', help='Rebuild block database from filesystem')

    p_import = subparsers.add_parser('import', help='Import existing .img file')
    p_import.add_argument('image', help='Path to .img file')
    p_import.add_argument('--as', dest='name', required=True, help='hostname:device')
    p_import.add_argument('--date', help='Override date (ISO format)')

    if len(sys.argv) > 1 and sys.argv[1] == 'dump':
        cmd_dump(sys.argv[2:])
        return
    if len(sys.argv) > 1 and ':' in sys.argv[1] and sys.argv[1] not in ('--help', '-h'):
        cmd_dump(sys.argv[1:])
        return

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)
    elif args.command == 'restore':
        cmd_restore(args)
    elif args.command == 'info':
        cmd_info(args)
    elif args.command == 'status':
        cmd_status(args)
    elif args.command == 'gc':
        cmd_gc(args)
    elif args.command == 'verify':
        cmd_verify(args)
    elif args.command == 'reindex':
        cmd_reindex(args)
    elif args.command == 'import':
        cmd_import(args)
