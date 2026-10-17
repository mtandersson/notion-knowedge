#!/usr/bin/env python3
"""Run the local Qwen adapter and record Linux child peak RSS beside timings."""
import json
from pathlib import Path
import resource
import subprocess
import sys


def main():
    if len(sys.argv) != 4 or not sys.platform.startswith('linux'):
        raise SystemExit('usage (Linux): qwen-eval-measure.py ADAPTER ASSETS NEW_MEASUREMENTS_JSON')
    adapter, assets, measurements = sys.argv[1:]
    # Inherit stdin/stdout: the ranking protocol remains the adapter's output.
    result = subprocess.run([adapter, assets, measurements], check=False)
    if result.returncode:
        raise SystemExit(result.returncode)
    path = Path(measurements)
    record = json.loads(path.read_text(encoding='utf-8'))
    record['peak_process_rss_kib'] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    record['memory_method'] = 'Linux child ru_maxrss; whole adapter including load and inference'
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
