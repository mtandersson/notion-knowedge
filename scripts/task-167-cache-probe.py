"""Task-owned temporary probes; never part of production CI."""
from pathlib import Path
import os, shutil, subprocess, tempfile, time
root=Path.cwd()
revision=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
import tomllib
version=tomllib.loads((root/'Cargo.toml').read_text())['workspace']['package']['version']
for variant in ['source','lockfile','builder-pin']:
    with tempfile.TemporaryDirectory() as tmp:
        context=Path(tmp)
        for name in ['Cargo.toml','Cargo.lock','Dockerfile','.dockerignore']:
            shutil.copy2(root/name,context/name)
        shutil.copytree(root/'crates',context/'crates')
        if variant=='source':
            with (context/'crates/server/src/main.rs').open('a') as f:
                f.write('\n// Task-owned Docker source invalidation probe.\n')
        elif variant=='lockfile':
            with (context/'Cargo.lock').open('a') as f:
                f.write('\n# Task-owned Docker dependency-input invalidation probe.\n')
        else:
            dockerfile=context/'Dockerfile'
            digest=subprocess.check_output(['docker','buildx','imagetools','inspect','rust:1.98.0-bookworm','--format','{{.Manifest.Digest}}'],text=True).strip()
            assert digest.startswith('sha256:') and len(digest)==71, digest
            original=dockerfile.read_text().splitlines()[1]
            replacement=f'FROM rust:1.98.0-bookworm@{digest} AS toolchain'
            print(f'PROBE BUILDER {replacement}',flush=True)
            dockerfile.write_text(dockerfile.read_text().replace(original,replacement))
        image=f'notion-knowledge:probe-{variant}'
        start=time.monotonic()
        print(f'PROBE {variant} START revision={revision} version={version}',flush=True)
        subprocess.run(['docker','buildx','build','--progress=plain','--load','--build-arg',f'VERSION={version}',
                        '--build-arg',f'REVISION={revision}','--tag',image,str(context)],check=True)
        print(f'PROBE {variant} BUILD_SECONDS={time.monotonic()-start:.2f}',flush=True)
        builder='notion-knowledge:smoke-builder'
        if variant=='builder-pin':
            builder='notion-knowledge:probe-toolchain'
            subprocess.run(['docker','buildx','build','--progress=plain','--load','--target','probe-builder','--tag',builder,str(context)],check=True)
        subprocess.run(['python3','scripts/smoke-container.py','--prebuilt','--image',image,
                        '--builder-image',builder],check=True)
        print(f'PROBE {variant} PASSED TOTAL_SECONDS={time.monotonic()-start:.2f}',flush=True)
