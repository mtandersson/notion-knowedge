"""Task-owned temporary probes; never part of production CI."""
from pathlib import Path
import hashlib, json, os, re, shutil, subprocess, tempfile, time
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
                f.write('\n// Task-owned #185 Docker source invalidation probe.\n')
        elif variant=='lockfile':
            with (context/'Cargo.lock').open('a') as f:
                f.write('\n# Task-owned #185 Docker dependency-input invalidation probe.\n')
        else:
            dockerfile=context/'Dockerfile'
            digest='sha256:82150a52ec202c1b14d7817e14516c392bb7f5cfebd88f1ed531cb37ebd39922'
            assert digest.startswith('sha256:') and len(digest)==71, digest
            original=dockerfile.read_text().splitlines()[1]
            replacement=f'FROM rust:1.98.0-bookworm@{digest} AS toolchain'
            print(f'PROBE BUILDER {replacement}',flush=True)
            dockerfile.write_text(dockerfile.read_text().replace(original,replacement))
        image=f'notion-knowledge:probe-{variant}'
        start=time.monotonic()
        print(f'PROBE {variant} START revision={revision} version={version}',flush=True)
        command=['docker','buildx','build','--progress=plain','--load','--build-arg',f'VERSION={version}',
                 '--build-arg',f'REVISION={revision}','--tag',image]
        cache_from=os.environ.get('PROBE_CACHE_FROM')
        if cache_from:
            command += ['--cache-from', cache_from]
        command.append(str(context))
        process=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        lines=[]
        for line in process.stdout:
            lines.append(line); print(line,end='',flush=True)
        assert process.wait()==0, f'{variant} image build failed'
        compiles=len(re.findall(r'\bCompiling \S+ v',''.join(lines)))
        assert compiles>0, f'{variant} unexpectedly reused the unchanged compiler result'
        container=subprocess.check_output(['docker','create',image],text=True).strip()
        try:
            binary=context/'.probe-binary'
            subprocess.run(['docker','cp',container+':/usr/local/bin/notion-knowledge-server',str(binary)],check=True)
            data=binary.read_bytes()
            print('INVALIDATION_RESULT '+json.dumps({'variant':variant,'compiled_crates':compiles,
                  'binary_sha256':hashlib.sha256(data).hexdigest(),'revision':revision,'cache_from':cache_from}),flush=True)
        finally:
            subprocess.run(['docker','rm',container],check=True,stdout=subprocess.DEVNULL)
        print(f'PROBE {variant} BUILD_SECONDS={time.monotonic()-start:.2f}',flush=True)
        builder='notion-knowledge:smoke-builder'
        if variant=='builder-pin':
            builder='notion-knowledge:probe-toolchain'
            subprocess.run(['docker','buildx','build','--progress=plain','--load','--target','probe-builder','--tag',builder,str(context)],check=True)
        subprocess.run(['python3','scripts/smoke-container.py','--prebuilt','--image',image,
                        '--builder-image',builder],check=True)
        print(f'PROBE {variant} PASSED TOTAL_SECONDS={time.monotonic()-start:.2f}',flush=True)
