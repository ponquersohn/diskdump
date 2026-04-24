from diskdump.cli import parse_dump_args, parse_device


class TestParseDevice:
    def test_absolute_path(self):
        assert parse_device('/dev/sda') == '/dev/sda'

    def test_sda_shorthand(self):
        assert parse_device('sda') == '/dev/sda'

    def test_nvme_shorthand(self):
        assert parse_device('nvme0n1') == '/dev/nvme0n1'

    def test_vda_shorthand(self):
        assert parse_device('vda') == '/dev/vda'

    def test_file_path(self):
        assert parse_device('/home/user/disk.img') == '/home/user/disk.img'

    def test_relative_name(self):
        assert parse_device('somefile.img') == 'somefile.img'


class TestParseDumpArgs:
    def test_single_target(self):
        result = parse_dump_args(['server01:/dev/sda'])
        assert result['targets'] == [('server01', ['/dev/sda'])]
        assert result['sudo_hosts'] == set()
        assert result['names'] == {}

    def test_multiple_targets(self):
        result = parse_dump_args(['server01:/dev/sda', 'server02:/dev/sdb'])
        assert len(result['targets']) == 2
        assert result['targets'][0] == ('server01', ['/dev/sda'])
        assert result['targets'][1] == ('server02', ['/dev/sdb'])

    def test_multiple_devices_per_host(self):
        result = parse_dump_args(['server01:/dev/sda:/dev/sdb'])
        assert result['targets'] == [('server01', ['/dev/sda', '/dev/sdb'])]

    def test_sudo_per_target(self):
        result = parse_dump_args([
            'server01:/dev/sda', '--sudo',
            'server02:/dev/sda',
        ])
        assert 'server01' in result['sudo_hosts']
        assert 'server02' not in result['sudo_hosts']

    def test_sudo_multiple_targets(self):
        result = parse_dump_args([
            'server01:/dev/sda', '--sudo',
            'server02:/dev/sda', '--sudo',
        ])
        assert result['sudo_hosts'] == {'server01', 'server02'}

    def test_as_per_target(self):
        result = parse_dump_args([
            'server01:/dev/sda', '--as', 'name1',
            'server02:/dev/sda', '--as', 'name2',
        ])
        assert result['names']['server01:/dev/sda'] == 'name1'
        assert result['names']['server02:/dev/sda'] == 'name2'

    def test_sudo_and_as_combined(self):
        result = parse_dump_args([
            'server01:/dev/sda', '--sudo', '--as', 'name1',
            'server02:/dev/sda', '--as', 'name2',
        ])
        assert 'server01' in result['sudo_hosts']
        assert 'server02' not in result['sudo_hosts']
        assert result['names']['server01:/dev/sda'] == 'name1'
        assert result['names']['server02:/dev/sda'] == 'name2'

    def test_sudo_before_target_exits(self):
        try:
            parse_dump_args(['--sudo', 'server01:/dev/sda'])
            assert False, 'Should have called sys.exit'
        except SystemExit:
            pass

    def test_as_before_target_exits(self):
        try:
            parse_dump_args(['--as', 'name', 'server01:/dev/sda'])
            assert False, 'Should have called sys.exit'
        except SystemExit:
            pass

    def test_user_flag(self):
        result = parse_dump_args(['--user', 'admin', 'server01:/dev/sda'])
        assert result['user'] == 'admin'

    def test_user_flag_short(self):
        result = parse_dump_args(['-u', 'admin', 'server01:/dev/sda'])
        assert result['user'] == 'admin'

    def test_batch_size(self):
        result = parse_dump_args(['--batch-size', '160', 'server01:/dev/sda'])
        assert result['batch_blocks'] == 160

    def test_block_size(self):
        result = parse_dump_args(['--block-size', '65536', 'server01:/dev/sda'])
        assert result['block_size'] == 65536

    def test_defaults(self):
        result = parse_dump_args(['server01:/dev/sda'])
        assert result['user'] is None
        assert result['batch_blocks'] == 80
        assert result['block_size'] == 131072

    def test_device_shorthand_expansion(self):
        result = parse_dump_args(['server01:sda:sdb'])
        assert result['targets'] == [('server01', ['/dev/sda', '/dev/sdb'])]

    def test_file_path_target(self):
        result = parse_dump_args(['localhost:/home/user/disk.img'])
        assert result['targets'] == [('localhost', ['/home/user/disk.img'])]

    def test_as_applies_to_all_devices_in_target(self):
        result = parse_dump_args(['server01:/dev/sda:/dev/sdb', '--as', 'myname'])
        assert result['names']['server01:/dev/sda'] == 'myname'
        assert result['names']['server01:/dev/sdb'] == 'myname'

    def test_no_targets(self):
        result = parse_dump_args([])
        assert result['targets'] == []

    def test_global_flags_before_targets(self):
        result = parse_dump_args([
            '--user', 'admin', '--batch-size', '40',
            'server01:/dev/sda', '--sudo',
        ])
        assert result['user'] == 'admin'
        assert result['batch_blocks'] == 40
        assert 'server01' in result['sudo_hosts']
