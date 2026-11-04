#!/usr/bin/env python3
"""Export the already-tested shipping server without building or starting it."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import stat
import subprocess
import tarfile
import tempfile
import tomllib
import uuid

ROOT = Path(__file__).resolve().parent.parent
SOURCE = 'https://github.com/mtandersson/notion-knowedge'
BINARY = 'notion-knowledge-server'


def docker(*args, **kwargs):
    return subprocess.run(['docker', *args], check=True, text=True, timeout=120, **kwargs)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def package(image, revision, output, runner=docker):
    require(re.fullmatch('[0-9a-f]{40}', revision), 'Exact source revision required')
    version = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']['version']
    metadata = json.loads(runner('image', 'inspect', image, capture_output=True).stdout)[0]
    image_id = metadata['Id']
    require(re.fullmatch('sha256:[0-9a-f]{64}', image_id), 'Immutable local image ID required')
    require(metadata['Os'] == 'linux' and metadata['Architecture'] == 'amd64', 'Expected Linux amd64 runtime')
    config = metadata['Config']
    require(config['Entrypoint'] == ['/usr/local/bin/' + BINARY]
            and config['User'] == '65532:65532', 'Expected final non-root runtime image')
    for key, expected in {'source': SOURCE, 'version': version, 'revision': revision}.items():
        require(config['Labels'].get('org.opencontainers.image.' + key) == expected, 'Wrong ' + key + ' label')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    destination = output / f'{BINARY}-linux-amd64-webhook-worker-{revision}.tar.gz'
    require(not destination.exists(), 'Refusing to overwrite an artifact')
    name = 'nk-artifact-' + uuid.uuid4().hex
    created = False
    with tempfile.TemporaryDirectory(prefix='nk-container-artifact-') as temporary:
        temporary = Path(temporary)
        try:
            runner('create', '--name', name, '--pull=never', '--network=none', '--read-only', image_id,
                   stdout=subprocess.DEVNULL)
            created = True
            binary = temporary / BINARY
            runner('cp', name + ':/usr/local/bin/' + BINARY, str(binary), stdout=subprocess.DEVNULL)
            info = binary.lstat()
            require(stat.S_ISREG(info.st_mode) and info.st_mode & 0o111, 'Expected regular executable')
            with binary.open('rb') as stream:
                header = stream.read(20)
            require(header[:6] == b'\x7fELF\x02\x01' and header[18:20] == b'\x3e\x00', 'Expected amd64 ELF64 executable')
            with binary.open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            manifest = {'schema_version': 1, 'source': SOURCE, 'revision': revision, 'version': version,
                        'platform': 'linux/amd64', 'cargo_features': ['webhook-worker'], 'image_id': image_id,
                        'binary': {'file': BINARY, 'bytes': info.st_size, 'sha256': digest}}
            archive_path = temporary / 'server.tar.gz'
            with tarfile.open(archive_path, 'w:gz') as archive:
                data = (json.dumps(manifest, sort_keys=True) + '\n').encode()
                entry = tarfile.TarInfo('manifest.json')
                entry.size = len(data)
                entry.mode = 0o644
                archive.addfile(entry, io.BytesIO(data))
                entry = tarfile.TarInfo(BINARY)
                entry.size = info.st_size
                entry.mode = 0o755
                with binary.open('rb') as stream:
                    archive.addfile(entry, stream)
        finally:
            if created:
                runner('rm', '-f', '--volumes', name, stdout=subprocess.DEVNULL)
        archive_path.replace(destination)
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', default='notion-knowledge:smoke')
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', type=Path, default=ROOT / 'dist/container-server')
    args = parser.parse_args()
    print(package(args.image, args.revision, args.output))


if __name__ == '__main__':
    main()
