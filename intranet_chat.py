import argparse
import os
import socket
import sys
import threading
from datetime import datetime
from typing import Optional
import getpass


class ChatLogger:
    def __init__(self, log_dir: str, peer_label: str):
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)
        date = datetime.now().strftime("%Y%m%d")
        fname = f"chat_{peer_label}_{date}.log"
        self.path = os.path.join(self.log_dir, fname)
        self.lock = threading.Lock()

    def write(self, direction: str, username: str, text: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {direction} {username}: {text}\n"
        with self.lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line)


class ChatSession:
    def __init__(self, sock: socket.socket, local_username: str, peer_addr: tuple, logger: ChatLogger):
        self.sock = sock
        self.local_username = local_username
        self.peer_addr = peer_addr
        self.logger = logger
        self.running = threading.Event()
        self.running.set()

    def start_receiver(self):
        t = threading.Thread(target=self._recv_loop, daemon=True)
        t.start()

    def _recv_loop(self):
        while self.running.is_set():
            try:
                data = self.sock.recv(4096)
            except Exception:
                break
            if not data:
                break
            try:
                text = data.decode("utf-8", errors="replace").rstrip("\n")
            except Exception:
                text = ""
            self.logger.write("recv", f"{self.peer_addr[0]}", text)
            print(f"{self.peer_addr[0]}> {text}")
        self.running.clear()

    def send_text(self, text: str):
        payload = (text + "\n").encode("utf-8")
        try:
            self.sock.sendall(payload)
        except Exception:
            return False
        self.logger.write("sent", self.local_username, text)
        return True

    def close(self):
        self.running.clear()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


def start_server(port: int, username: str, log_dir: str, message: Optional[str]):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(1)
    print(f"listening on 0.0.0.0:{port}")
    conn, addr = srv.accept()
    try:
        srv.close()
    except Exception:
        pass
    peer_label = f"{addr[0]}_{addr[1]}"
    logger = ChatLogger(log_dir, peer_label)
    session = ChatSession(conn, username, addr, logger)
    print(f"connected to {addr[0]}:{addr[1]}")
    session.start_receiver()
    if message is not None:
        session.send_text(message)
        session.close()
        return
    try:
        for line in sys.stdin:
            line = line.rstrip("\n")
            if not session.running.is_set():
                break
            if line.strip() == "/quit":
                break
            if line:
                ok = session.send_text(line)
                if not ok:
                    break
    except KeyboardInterrupt:
        pass
    session.close()


def start_client(host: str, port: int, username: str, log_dir: str, message: Optional[str]):
    cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cli.connect((host, port))
    peer_label = f"{host}_{port}"
    logger = ChatLogger(log_dir, peer_label)
    session = ChatSession(cli, username, (host, port), logger)
    print(f"connected to {host}:{port}")
    session.start_receiver()
    if message is not None:
        session.send_text(message)
        session.close()
        return
    try:
        for line in sys.stdin:
            line = line.rstrip("\n")
            if not session.running.is_set():
                break
            if line.strip() == "/quit":
                break
            if line:
                ok = session.send_text(line)
                if not ok:
                    break
    except KeyboardInterrupt:
        pass
    session.close()


def parse_args():
    p = argparse.ArgumentParser(prog="intranet_chat", add_help=True)
    p.add_argument("--mode", choices=["server", "client"], required=True)
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--host", type=str)
    p.add_argument("--username", type=str, default=getpass.getuser())
    p.add_argument("--log-dir", type=str, default=os.path.join(os.getcwd(), "chat_logs"))
    p.add_argument("--message", type=str)
    return p.parse_args()


def main():
    args = parse_args()
    if args.mode == "server":
        start_server(args.port, args.username, args.log_dir, args.message)
    else:
        if not args.host:
            print("--host is required in client mode")
            sys.exit(2)
        start_client(args.host, args.port, args.username, args.log_dir, args.message)


if __name__ == "__main__":
    main()
