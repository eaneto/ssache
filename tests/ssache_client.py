import os
import signal
import socket as s
import subprocess
import time
from typing import Optional

import psutil

CRLF = "\r\n"


def initialize_ssache(port: int = 7777, args: Optional[str] = None):
    os.environ["RUST_LOG"] = "info"
    command = ["./target/release/ssache", "-p", str(port)]
    if args is not None:
        command.extend(args.split(" "))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # TODO: Instead of sleeping, loop waiting for the sucess log
    time.sleep(0.5)
    return process.pid


def find_and_kill_ssache_process():
    # Workaround to kill the running ssache process and restart it
    processes = psutil.process_iter()

    name = "ssache"
    ssache_process = [p for p in processes if name in p.name()][0]
    os.kill(ssache_process.pid, signal.SIGTERM)


def kill_ssache(pid):
    os.kill(pid, signal.SIGTERM)
    time.sleep(0.5)


class SsacheClient:
    IP = "127.0.0.1"

    def connect(self, port: int = 7777):
        self.__socket = s.socket(s.AF_INET, s.SOCK_STREAM)
        self.__socket.connect((self.IP, port))

    def unknown(self):
        request = f"UNKNOWN{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def ping(self, message=None):
        if message is None:
            request = f"PING{CRLF}"
        else:
            request = f"PING {message}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def get(self, key):
        request = f"GET {key}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def set(self, key, value):
        request = f"SET {key} {value}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def expire(self, key, ttl):
        request = f"EXPIRE {key} {ttl}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def incr(self, key):
        request = f"INCR {key}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def decr(self, key):
        request = f"DECR {key}{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def save(self):
        request = f"SAVE{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def load(self):
        request = f"LOAD{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)

    def quit(self):
        request = f"QUIT{CRLF}"
        self.__socket.send(request.encode("utf-8"))
        return self.__socket.recv(1024)
