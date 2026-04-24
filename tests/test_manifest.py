import os

from diskdump.manifest import Manifest


def test_write_and_read_roundtrip(tmp_path):
    m = Manifest()
    m.source = 'server01:/dev/sda'
    m.date = '2026-04-24T10:00:00Z'
    m.block_size = 131072
    m.total_blocks = 3
    m.total_size = 393216
    m.hashes = ['aa' * 32, 'bb' * 32, 'cc' * 32]

    path = str(tmp_path / 'test.manifest')
    m.write(path)

    m2 = Manifest.read(path)
    assert m2.source == m.source
    assert m2.date == m.date
    assert m2.block_size == m.block_size
    assert m2.total_blocks == m.total_blocks
    assert m2.total_size == m.total_size
    assert m2.hashes == m.hashes


def test_read_ignores_unknown_comments(tmp_path):
    path = str(tmp_path / 'test.manifest')
    with open(path, 'w') as f:
        f.write('# diskdump manifest v1\n')
        f.write('# source: host:/dev/sda\n')
        f.write('# some_future_field: value\n')
        f.write('# date: 2026-01-01\n')
        f.write('# block_size: 131072\n')
        f.write('# total_blocks: 1\n')
        f.write('# total_size: 131072\n')
        f.write('aa' * 32 + '\n')

    m = Manifest.read(path)
    assert m.source == 'host:/dev/sda'
    assert len(m.hashes) == 1


def test_output_path(tmp_path):
    base = str(tmp_path)
    path = Manifest.output_path(base, 'myhost', '/dev/sda')
    assert path == os.path.join(base, '.blockstore', 'manifests', 'myhost.sda', '0.manifest')


def test_output_path_file_device(tmp_path):
    base = str(tmp_path)
    path = Manifest.output_path(base, 'myhost', '/home/user/disk.img')
    assert '.blockstore/manifests/' in path
    assert '.manifest' in path


def test_output_path_increments_version(tmp_path):
    base = str(tmp_path)
    manifest_dir = os.path.join(base, '.blockstore', 'manifests', 'myhost.sda')
    os.makedirs(manifest_dir)

    m = Manifest()
    m.hashes = ['aa' * 32]

    for expected in range(3):
        path = Manifest.output_path(base, 'myhost', '/dev/sda')
        assert path.endswith(f'{expected}.manifest')
        m.write(path)


def test_next_version_empty(tmp_path):
    assert Manifest._next_version(str(tmp_path / 'nonexistent')) == 0


def test_next_version_with_gaps(tmp_path):
    manifest_dir = str(tmp_path / 'test')
    os.makedirs(manifest_dir)
    for n in [0, 1, 5]:
        with open(os.path.join(manifest_dir, f'{n}.manifest'), 'w') as f:
            f.write('')
    assert Manifest._next_version(manifest_dir) == 6


def test_write_creates_directories(tmp_path):
    path = str(tmp_path / 'a' / 'b' / 'c' / 'test.manifest')
    m = Manifest()
    m.hashes = ['aa' * 32]
    m.write(path)
    assert os.path.exists(path)


def test_total_blocks_inferred_from_hashes(tmp_path):
    path = str(tmp_path / 'test.manifest')
    with open(path, 'w') as f:
        f.write('# diskdump manifest v1\n')
        f.write('# source: host:/dev/sda\n')
        f.write('aa' * 32 + '\n')
        f.write('bb' * 32 + '\n')

    m = Manifest.read(path)
    assert m.total_blocks == 2
