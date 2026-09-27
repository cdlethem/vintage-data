"""Network-namespace loopback relay to the only mounted model capability socket."""
from pathlib import Path
import select
import socket
import socketserver
import subprocess
import sys
import threading

class Relay(socketserver.BaseRequestHandler):
    def handle(self):
        with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as peer:
            peer.settimeout(300)
            self.request.settimeout(300)
            peer.connect('/model.sock')
            sockets=[self.request,peer]
            while True:
                ready,_,_=select.select(sockets,[],[],300)
                if not ready:return
                for source in ready:
                    data=source.recv(65536)
                    if not data:return
                    (peer if source is self.request else self.request).sendall(data)

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address=True
    daemon_threads=True

def probe():
    for path, message in [('/home/colin', 'host home exposed'),
                          ('/var/run/docker.sock', 'Docker socket exposed'),
                          ('/work/.git', 'Git metadata exposed'),
                          ('/run/user', 'host service sockets exposed'),
                          ('/usr/local', 'local host installations exposed')]:
        if Path(path).exists():
            raise RuntimeError(message)
    for address in [('1.1.1.1',443),('127.0.0.1',8082),('127.0.0.1',5432)]:
        try:
            with socket.create_connection(address,timeout=1):pass
        except OSError:continue
        raise RuntimeError('unexpected network access')
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as connection:
        connection.settimeout(5)
        connection.connect('/model.sock')
        connection.sendall(b'GET /v1/models HTTP/1.0\r\nHost: localhost\r\n\r\n')
        response = b''
        while b'\r\n' not in response and len(response) < 4096:
            chunk = connection.recv(4096 - len(response))
            if not chunk:
                break
            response += chunk
        status = response.split(b'\r\n', 1)[0].split()
        if len(status) < 2 or status[1] != b'200':
            raise RuntimeError('model gateway unreachable')
    print('sandbox probe: host paths, service sockets and egress denied; scoped gateway reachable')

if __name__=='__main__':
    if sys.argv[1:]==['--probe']:probe()
    else:
        with Server(('127.0.0.1',18080),Relay) as server:
            threading.Thread(target=server.serve_forever,daemon=True).start()
            command=sys.argv[1:]
            if command and command[0]=='--':command=command[1:]
            raise SystemExit(subprocess.call(command))
