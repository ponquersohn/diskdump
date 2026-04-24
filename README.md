# diskdump

Deduplicated disk image transfer over SSH. Transfers only unique blocks across machines and over time, stores them compressed in a content-addressable block store.

## How it works

1. Client script is deployed to remote machine via SCP
2. Client reads the disk/file in configurable batches (default 10MB)
3. For each batch: hashes blocks (SHA256), asks server which are needed
4. Only new blocks are sent (zlib-compressed on wire, lz4-compressed for storage)
5. Each dump is stored as a lightweight manifest file pointing into a shared global block store

Single-pass streaming — no second read of the source. Blocks stay in memory only for one batch.

## Usage

```bash
# Dump from remote machines (parallel, per-target --sudo)
diskdump dump server01:/dev/sda --sudo --as server01.sda server02:/dev/sda --as server02.sda

# Dump files (not block devices)
diskdump dump localhost:/path/to/file.img --as my-disk.img

# Restore
diskdump restore 2026/04/24/server01-sda.manifest -o restored.img
diskdump restore 2026/04/24/server01-sda.manifest | dd of=/dev/sda

# Import existing raw image into block store
diskdump import existing.img --as server01:/dev/sda

# Info & management
diskdump info 2026/04/24/server01-sda.manifest
diskdump status
diskdump verify
diskdump gc              # remove unreferenced blocks
diskdump gc --dry-run
```

## Options

| Flag | Description |
|------|-------------|
| `--sudo` | Use sudo on preceding target (repeatable per-target) |
| `--user`, `-u` | SSH user |
| `--block-size` | Block size in bytes (default 131072 = 128KB) |
| `--batch-size` | Blocks per batch (default 80 = 10MB) |
| `--as` | Custom manifest name (follows target it applies to) |

## Storage layout

```
disks/
  .blockstore/
    config.json
    ab/
      cd/
        abcdef...full_sha256_hash.lz4
  2026/
    04/
      24/
        server01-sda.manifest
```

- Blocks are content-addressed by SHA256, stored lz4-compressed
- Two-level directory fanout (first 2 bytes of hash = 65536 buckets)
- Manifests are plain text: header comments + one hash per line
- Global dedup: blocks shared across all dumps

## Dependencies

- Server (local): Python 3, `lz4` (`pip install lz4`)
- Client (remote): Python 3 only (stdlib — no external deps)

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```
