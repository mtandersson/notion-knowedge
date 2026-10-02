import importlib.util, json, sys, time
spec=importlib.util.spec_from_file_location("smoke", "scripts/smoke-container.py")
smoke=importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)
original=smoke.docker
def measured(*args, **kwargs):
    start=time.monotonic()
    try: return original(*args, **kwargs)
    finally: print("BENCH_DOCKER " + json.dumps({"operation": args[0], "arguments": list(args[:4]), "seconds": time.monotonic()-start}), flush=True)
smoke.docker=measured
start=time.monotonic()
try: smoke.main()
finally: print("BENCH_SMOKE " + json.dumps({"seconds": time.monotonic()-start}), flush=True)
