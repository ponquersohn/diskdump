import os


class Manifest:
    def __init__(self):
        self.source: str = ''
        self.date: str = ''
        self.block_size: int = 131072
        self.total_blocks: int = 0
        self.total_size: int = 0
        self.hashes: list = []

    def write(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = path + f'.tmp.{os.getpid()}'
        with open(tmp_path, 'w') as f:
            f.write(f'# diskdump manifest v1\n')
            f.write(f'# source: {self.source}\n')
            f.write(f'# date: {self.date}\n')
            f.write(f'# block_size: {self.block_size}\n')
            f.write(f'# total_blocks: {self.total_blocks}\n')
            f.write(f'# total_size: {self.total_size}\n')
            for h in self.hashes:
                f.write(h + '\n')
        os.rename(tmp_path, path)

    @classmethod
    def read(cls, path: str) -> 'Manifest':
        m = cls()
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if line.startswith('# source: '):
                    m.source = line[10:]
                elif line.startswith('# date: '):
                    m.date = line[8:]
                elif line.startswith('# block_size: '):
                    m.block_size = int(line[14:])
                elif line.startswith('# total_blocks: '):
                    m.total_blocks = int(line[16:])
                elif line.startswith('# total_size: '):
                    m.total_size = int(line[14:])
                elif line.startswith('#'):
                    continue
                else:
                    m.hashes.append(line)
        if not m.total_blocks:
            m.total_blocks = len(m.hashes)
        return m

    @staticmethod
    def _next_version(manifest_dir: str) -> int:
        if not os.path.isdir(manifest_dir):
            return 0
        highest = -1
        for name in os.listdir(manifest_dir):
            base, ext = os.path.splitext(name)
            if ext == '.manifest' and base.isdigit():
                highest = max(highest, int(base))
        return highest + 1

    @staticmethod
    def output_path(base_dir: str, hostname: str, device: str) -> str:
        dev_name = device.replace('/dev/', '').replace('/', '.')
        name = f'{hostname}.{dev_name}'
        manifest_dir = os.path.join(base_dir, '.blockstore', 'manifests', name)
        version = Manifest._next_version(manifest_dir)
        return os.path.join(manifest_dir, f'{version}.manifest')
