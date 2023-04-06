import os
import signal
import socket as s
import subprocess
import time

CRLF = "\r\n"


def initialize_ssache():
    os.environ["RUST_LOG"] = "info"
    command = ["./target/release/ssache"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # TODO: Instead of sleeping, loop waiting for the sucess log
    time.sleep(0.5)
    return process.pid


def kill_ssache(pid):
    os.kill(pid, signal.SIGTERM)
    time.sleep(0.5)


class SsacheClient:
    IP = "127.0.0.1"
    PORT = 7777

    def connect(self):
        self.__socket = s.socket(s.AF_INET, s.SOCK_STREAM)
        self.__socket.connect((self.IP, self.PORT))

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
