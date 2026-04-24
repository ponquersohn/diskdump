# diskdump

Deduplicated disk image transfer tool over SSH.

## Running

```bash
.venv/bin/diskdump <command> [args]
# or
.venv/bin/python -m diskdump <command> [args]
```

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/pip install pytest
```

## Testing

Tests are mandatory. Run before any commit:
```bash
.venv/bin/python -m pytest tests/ -v
```

## Package structure

```
src/diskdump/
  __main__.py       # python -m diskdump entry point
  cli.py            # CLI argument parsing, subcommand dispatch
  client.py         # self-contained remote client (zero deps, deployed via SCP)
  protocol.py       # binary wire protocol (length-prefixed messages over SSH stdin/stdout)
  blockstore.py     # content-addressable lz4 block store, filesystem-as-index
  manifest.py       # manifest read/write (plain text, one hash per line)
  orchestrator.py   # async SSH session management, parallel dumps
  restore.py        # image reconstruction from manifest + block store
  config.py         # constants
```

## Protocol

Streaming batched protocol over SSH stdin/stdout:
- Client reads batches (default 80 blocks = 10MB), hashes, sends BATCH_HASHES
- Server replies BATCH_NEEDED (which block offsets within the batch are new)
- Client sends only needed blocks from memory (already read, no re-read)
- Single pass through the source

Message types: INIT(0x01), BATCH_HASHES(0x02), BLOCK(0x03), DONE(0x04), BATCH_NEEDED(0x10), RESULT(0x11), ERROR(0xFF)

## Block store layout

Two-level directory fanout with full hash as filename:
```
.blockstore/ab/cd/abcdef...full_sha256.lz4
```

Atomic writes via tmp file + rename. Concurrent-safe.

## Manifest layout

Manifests live inside the block store, versioned per source:
```
.blockstore/manifests/hostname.sda/0.manifest
.blockstore/manifests/hostname.sda/1.manifest
.blockstore/manifests/hostname.sda/2.manifest
```

Each dump appends the next version number. Latest is always the highest.
