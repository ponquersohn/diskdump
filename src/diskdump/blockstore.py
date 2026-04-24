import json
import os
import sqlite3

import lz4.frame


class BlockStore:
    def __init__(self, base_dir: str):
        self.base = os.path.join(base_dir, '.blockstore')
        self._db = None

    @property
    def manifests_dir(self) -> str:
        return os.path.join(self.base, 'manifests')

    @property
    def _db_path(self) -> str:
        return os.path.join(self.base, 'blocks.db')

    def _conn(self) -> sqlite3.Connection:
        if self._db is None:
            self._db = sqlite3.connect(self._db_path)
            self._db.execute('PRAGMA journal_mode=WAL')
            self._db.execute('PRAGMA synchronous=NORMAL')
            self._db.execute(
                'CREATE TABLE IF NOT EXISTS blocks '
                '(hash TEXT PRIMARY KEY, compressed_size INTEGER NOT NULL)')
        return self._db

    def close(self):
        if self._db is not None:
            self._db.close()
            self._db = None

    def init(self, block_size: int = 131072):
        os.makedirs(self.base, exist_ok=True)
        self._conn()
        config_path = os.path.join(self.base, 'config.json')
        if os.path.exists(config_path):
            return
        config = {'block_size': block_size, 'hash_algo': 'sha256', 'version': 1}
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

    def load_config(self) -> dict:
        with open(os.path.join(self.base, 'config.json')) as f:
            return json.load(f)

    def _block_path(self, hash_hex: str) -> str:
        return os.path.join(self.base, hash_hex[:2], hash_hex[2:4], hash_hex + '.lz4')

    def has_block(self, hash_hex: str) -> bool:
        row = self._conn().execute(
            'SELECT 1 FROM blocks WHERE hash=?', (hash_hex,)).fetchone()
        return row is not None

    def store_block(self, hash_hex: str, data: bytes) -> bool:
        if self.has_block(hash_hex):
            return False
        path = self._block_path(hash_hex)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        compressed = lz4.frame.compress(data)
        tmp_path = path + f'.tmp.{os.getpid()}'
        try:
            with open(tmp_path, 'wb') as f:
                f.write(compressed)
            os.rename(tmp_path, path)
        except BaseException:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
        self._conn().execute(
            'INSERT OR IGNORE INTO blocks (hash, compressed_size) VALUES (?, ?)',
            (hash_hex, len(compressed)))
        self._conn().commit()
        return True

    def read_block(self, hash_hex: str) -> bytes:
        path = self._block_path(hash_hex)
        with open(path, 'rb') as f:
            return lz4.frame.decompress(f.read())

    def stats(self) -> dict:
        row = self._conn().execute(
            'SELECT COUNT(*), COALESCE(SUM(compressed_size), 0) FROM blocks').fetchone()
        return {
            'total_blocks': row[0],
            'total_compressed_bytes': row[1],
        }

    def all_hashes(self) -> set:
        rows = self._conn().execute('SELECT hash FROM blocks').fetchall()
        return {r[0] for r in rows}

    def remove_block(self, hash_hex: str) -> bool:
        path = self._block_path(hash_hex)
        existed = False
        if os.path.exists(path):
            os.unlink(path)
            existed = True
        self._conn().execute('DELETE FROM blocks WHERE hash=?', (hash_hex,))
        self._conn().commit()
        return existed

    def reindex(self):
        """Rebuild the database from the filesystem."""
        conn = self._conn()
        conn.execute('DELETE FROM blocks')
        count = 0
        for l1 in os.scandir(self.base):
            if not l1.is_dir() or len(l1.name) != 2:
                continue
            for l2 in os.scandir(l1.path):
                if not l2.is_dir() or len(l2.name) != 2:
                    continue
                for entry in os.scandir(l2.path):
                    if entry.name.endswith('.lz4'):
                        h = entry.name[:-4]
                        conn.execute(
                            'INSERT OR IGNORE INTO blocks (hash, compressed_size) VALUES (?, ?)',
                            (h, entry.stat().st_size))
                        count += 1
        conn.commit()
        return count
