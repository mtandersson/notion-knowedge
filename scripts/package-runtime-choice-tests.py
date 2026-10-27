#!/usr/bin/env python3
"""Package the exact Cargo-built retrieval test harness and runtime fixture."""
import json
from pathlib import Path
import sys
import tarfile

messages, output = map(Path, sys.argv[1:])
executables = []
for line in messages.read_text().splitlines():
    item = json.loads(line)
    if (item.get('reason') == 'compiler-artifact'
            and item.get('target', {}).get('name') == 'notion_knowledge_retrieval'
            and item.get('profile', {}).get('test')
            and item.get('executable')):
        executables.append(Path(item['executable']))
if len(executables) != 1 or not executables[0].is_file():
    raise SystemExit('expected exactly one retrieval test executable')
runtime = Path('target/debug/examples/lexical-runtime')
if not runtime.is_file():
    raise SystemExit('missing lexical runtime fixture')
with tarfile.open(output, 'w') as archive:
    archive.add(executables[0], arcname='runtime-choice/retrieval-tests')
    archive.add(runtime, arcname='runtime-choice/lexical-runtime')
