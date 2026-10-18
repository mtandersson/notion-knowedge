#!/usr/bin/env python3
"""Disposable local Docker/TLS overload proof; never production capacity acceptance.

Requires an already-built local server image, Docker Compose, OpenSSL and NGINX.
Uses only loopback ports and a nonsecret bootstrap. Port 3000 must be free.
"""
import argparse
import importlib.util
import http.client
import hashlib
import json
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--nginx', required=True)
parser.add_argument('--image', required=True)
args = parser.parse_args()
nginx = str(Path(args.nginx).resolve(strict=True))
spec = importlib.util.spec_from_file_location('resources', Path(__file__).with_name('production-resources.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
p = Path(tempfile.mkdtemp(prefix='nk-resource-proof-'))
project = p.name
policy = {key: 100 for key in m.FIELDS}
policy.update(image=args.image, cpus=.5, memory_bytes=134217728, pids=64,
              nofile=128, tmp_bytes=1048576, log_bytes=1048576, log_files=2,
              proxy_connections=64, per_client_connections=2,
              requests_per_second=2, request_burst=1, header_bytes=4096,
              body_bytes=1024, header_timeout_seconds=1,
              body_timeout_seconds=1, write_timeout_seconds=1,
              upstream_timeout_seconds=2)
(p/'policy.json').write_text(json.dumps(policy))
policy = m.policy(p/'policy.json', fixture=True)
(p/'service.env').write_text('NK_NOTION_AUTH=none\n')
m.render(policy, p/'service.env', p/'generated')
# A libc-free fixture avoids modifying the reviewed server image.
source = Path(__file__).resolve().parent.parent/'deploy/production/resource-probe.c'
subprocess.run(['cc','-nostdlib','-static','-ffreestanding','-fno-builtin','-fno-stack-protector','-O2',str(source),'-o',str(p/'resource-probe')],check=True)
compose_path=p/'generated/compose.json'
compose_data=json.loads(compose_path.read_text())
compose_data['services']['knowledge']['volumes']=[str(p/'resource-probe')+':/resource-probe:ro']
compose_path.write_text(json.dumps(compose_data))
generated = p/'generated/nginx.conf'
compose = ['docker', 'compose', '-p', project, '-f', str(p/'generated/compose.json')]
subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                '-subj', '/CN=knowledge.example.com', '-addext', 'subjectAltName=DNS:knowledge.example.com',
                '-keyout', str(p/'key.pem'), '-out', str(p/'cert.pem')],
               check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
s=socket.socket();s.bind(('127.0.0.1',0));port=s.getsockname()[1];s.close()
conf=generated.read_text().replace('listen 443 ssl default_server;',f'listen 127.0.0.1:{port} ssl default_server;').replace('/etc/letsencrypt/live/knowledge.example.com/fullchain.pem',str(p/'cert.pem')).replace('/etc/letsencrypt/live/knowledge.example.com/privkey.pem',str(p/'key.pem'))
conf=f'pid {p}/nginx.pid;\nerror_log stderr warn;\n'+conf
conf=conf.replace('http {', 'http {\n'+ '\n'.join(f'{name}_temp_path {p/name};' for name in ['client_body','proxy','fastcgi','uwsgi','scgi']),1)
(p/'proxy.conf').write_text(conf)
subprocess.run([nginx,'-t','-e','stderr','-p',str(p)+'/', '-c',str(p/'proxy.conf')],check=True,capture_output=True)
proc=subprocess.Popen([nginx,'-e','stderr','-p',str(p)+'/', '-c',str(p/'proxy.conf'),'-g','daemon off;'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
ctx=ssl.create_default_context(cafile=str(p/'cert.pem'))
def connect():return ctx.wrap_socket(socket.create_connection(('127.0.0.1',port),timeout=3),server_hostname='knowledge.example.com')
def request(path='/mcp',headers='',body='',method='GET'):
 with connect() as s:
  text=f'{method} {path} HTTP/1.1\r\nHost: knowledge.example.com\r\nConnection: close\r\n{headers}\r\n{body}'
  s.sendall(text.encode());return int(s.recv(4096).split()[1])
try:
 subprocess.run(compose+['up', '-d'], check=True, stdout=subprocess.DEVNULL)
 container=subprocess.check_output(compose+['ps','-q','knowledge'], text=True).strip()
 effective=m.inspect(policy,container)
 # Actual unprivileged process exhaustion and cleanup within the same cgroup.
 probe=subprocess.run(['docker','exec',container,'/resource-probe'],capture_output=True,text=True,check=True)
 assert 'tmpfs-exhaustion-recovered' in probe.stdout
 assert 'fd-exhaustion-recovered' in probe.stdout
 assert 'pid-exhaustion-recovered' in probe.stdout
 kernel=dict(line.split('=',1) for line in probe.stdout.splitlines() if '=' in line)
 assert kernel['memory.max']==str(policy['memory_bytes'])
 assert kernel['memory.swap.max']=='0'
 assert kernel['pids.max']==str(policy['pids'])
 quota,period=map(int,kernel['cpu.max'].split())
 assert quota/period==policy['cpus']
 for _ in range(100):
  try:
   health=http.client.HTTPConnection('127.0.0.1',3000,timeout=.5)
   health.request('GET','/livez')
   if health.getresponse().status==200: break
  except OSError: pass
  finally: health.close()
  time.sleep(.05)
 else: raise RuntimeError('local fixture health did not recover')
 for _ in range(100):
  try:s=connect();s.close();break
  except OSError:time.sleep(.02)
 baseline=request()
 assert baseline==406, baseline
 assert request('/livez')==404
 assert request(headers='Origin: https://evil.example\r\n')==403
 assert request(headers='Content-Length: 1025\r\n',method='POST')==413
 assert request(headers='X-Large: '+ 'a'*8192+'\r\n')==400
 time.sleep(1.1)
 outcomes=[request() for _ in range(8)]
 assert 429 in outcomes,outcomes
 time.sleep(1.1)
 recovered=request();assert recovered==baseline,recovered
 with connect() as slow:
  slow.sendall(b'GET /mcp HTTP/1.1\r\nHost: knowledge.example.com\r\nX-Slow: ')
  time.sleep(1.2)
  # NGINX closes a timed-out incomplete header without granting backend work.
  try:closed=not slow.recv(4096)
  except (ConnectionResetError,ssl.SSLError):closed=True
  assert closed
 time.sleep(1.1)
 assert request()==baseline
 health=http.client.HTTPConnection('127.0.0.1',3000,timeout=2)
 health.request('GET','/livez');assert health.getresponse().status==200;health.close()
 print(json.dumps({'scope':'disposable local TLS proxy only; not deployed capacity','nginx':subprocess.run([nginx,'-v'],capture_output=True,text=True,check=True).stderr.strip(),'applied_config_sha256':hashlib.sha256(conf.encode()).hexdigest(),'proxy_limits':{k:v for k,v in policy.items() if k.startswith('proxy_') or k.startswith('per_client_') or k.endswith('_seconds') or k in ['requests_per_second','request_burst','header_bytes','body_bytes']},'checks':['actual tmpfs/FD/PID exhaustion and health recovery','syntax','private diagnostics404','foreign Origin403','oversized body413','oversized header400','rate overload429','rate recovery','slow header closed','post-timeout recovery'],'rate_statuses':outcomes,'recovery_status':recovered,'container':effective,'kernel_cgroup_v2':kernel}))
finally:
 proc.terminate();proc.wait(timeout=5)
 subprocess.run(compose+['down','--remove-orphans'],check=True,stdout=subprocess.DEVNULL)
