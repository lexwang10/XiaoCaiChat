import argparse
import curses
import socket
import threading
import time
from typing import Optional
import getpass
import os

from intranet_chat import ChatLogger


class TuiClient:
    def __init__(self, host: str, port: int, username: str, log_dir: str, room: str):
        self.host = host
        self.port = port
        self.username = username
        self.room = room
        self.sock: Optional[socket.socket] = None
        self.reader: Optional[threading.Thread] = None
        self.running = threading.Event()
        self.running.set()
        self.lines = []
        self.lock = threading.Lock()
        self.input_buf = ""
        self.logger = ChatLogger(log_dir, f"{host}_{port}")

    def connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        self.sock = s
        hello = f"HELLO {self.username} {self.room}\n".encode("utf-8")
        self.sock.sendall(hello)
        t = threading.Thread(target=self._recv_loop, daemon=True)
        t.start()
        self.reader = t

    def _recv_loop(self):
        f = self.sock.makefile("r", encoding="utf-8", newline="\n")
        for line in f:
            text = line.rstrip("\n")
            if not text:
                continue
            self.logger.write("recv", self.host, text)
            with self.lock:
                self.lines.append(text)
        self.running.clear()

    def send_text(self, text: str):
        try:
            self.sock.sendall((text + "\n").encode("utf-8"))
            self.logger.write("sent", self.username, text)
        except Exception:
            pass

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


def run_tui(stdscr, client: TuiClient):
    curses.curs_set(1)
    stdscr.nodelay(True)
    height, width = stdscr.getmaxyx()
    log_h = height - 3

    while client.running.is_set():
        stdscr.clear()
        stdscr.addstr(0, 0, f"群聊 {client.username} @ {client.host}:{client.port}")
        stdscr.hline(1, 0, ord("-"), width)

        with client.lock:
            view = client.lines[-(log_h-2):]
        y = 2
        for line in view:
            if y >= log_h:
                break
            stdscr.addstr(y, 0, line[:width-1])
            y += 1

        stdscr.hline(log_h, 0, ord("-"), width)
        stdscr.addstr(log_h+1, 0, "> " + client.input_buf[:width-3])
        stdscr.refresh()

        try:
            ch = stdscr.getch()
        except Exception:
            ch = -1
        if ch == -1:
            time.sleep(0.05)
            continue
        if ch in (10, 13):
            text = client.input_buf.strip()
            if text:
                client.send_text(text)
            client.input_buf = ""
        elif ch in (curses.KEY_BACKSPACE, 127):
            client.input_buf = client.input_buf[:-1]
        elif ch == 3:
            break
        elif 32 <= ch <= 126:
            client.input_buf += chr(ch)

    client.close()


def parse_args():
    p = argparse.ArgumentParser(prog="tui_chat_client", add_help=True)
    p.add_argument("--host", type=str, required=True)
    p.add_argument("--port", type=int, default=5001)
    p.add_argument("--username", type=str, default=getpass.getuser())
    p.add_argument("--log-dir", type=str, default=os.path.join(os.getcwd(), "chat_logs"))
    p.add_argument("--room", type=str, default="general")
    return p.parse_args()


def main():
    args = parse_args()
    client = TuiClient(args.host, args.port, args.username, args.log_dir, args.room)
    client.connect()
    curses.wrapper(run_tui, client)


if __name__ == "__main__":
    main()
