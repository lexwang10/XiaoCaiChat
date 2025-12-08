import argparse
import socket
import threading
import queue
import tkinter as tk
import tkinter.scrolledtext as scrolledtext
from typing import Optional
from datetime import datetime
import getpass
import os

from intranet_chat import ChatLogger


class GuiClient:
    def __init__(self, host: str, port: int, username: str, log_dir: str):
        self.host = host
        self.port = port
        self.username = username
        self.sock = None
        self.reader = None
        self.q = queue.Queue()
        label = f"{host}_{port}"
        self.logger = ChatLogger(log_dir, label)

    def connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        self.sock = s
        hello = f"HELLO {self.username}\n".encode("utf-8")
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
            self.q.put(text)

    def send_text(self, text: str):
        payload = (text + "\n").encode("utf-8")
        try:
            self.sock.sendall(payload)
            self.logger.write("sent", self.username, text)
        except Exception:
            pass

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


class ChatApp:
    def __init__(self, host: str, port: int, username: str, log_dir: str):
        self.client = GuiClient(host, port, username, log_dir)
        self.root = tk.Tk()
        self.root.title(f"群聊 - {username} @ {host}:{port}")
        self.view = scrolledtext.ScrolledText(self.root, wrap="word", state="disabled", width=80, height=24)
        self.view.grid(row=0, column=0, columnspan=3, padx=8, pady=8, sticky="nsew")
        self.entry = tk.Entry(self.root, width=70)
        self.entry.grid(row=1, column=0, padx=8, pady=8, sticky="ew")
        self.btn = tk.Button(self.root, text="发送", command=self.on_send)
        self.btn.grid(row=1, column=1, padx=8, pady=8)
        self.quit = tk.Button(self.root, text="退出", command=self.on_quit)
        self.quit.grid(row=1, column=2, padx=8, pady=8)
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.entry.bind("<Return>", self.on_send_return)
        self.root.protocol("WM_DELETE_WINDOW", self.on_quit)
        self.root.after(100, self.poll)
        self.client.connect()

    def append(self, text: str):
        self.view.configure(state="normal")
        self.view.insert("end", text + "\n")
        self.view.see("end")
        self.view.configure(state="disabled")

    def poll(self):
        try:
            while True:
                msg = self.client.q.get_nowait()
                self.append(msg)
        except queue.Empty:
            pass
        self.root.after(100, self.poll)

    def on_send(self):
        text = self.entry.get().strip()
        if not text:
            return
        self.client.send_text(text)
        self.entry.delete(0, "end")

    def on_send_return(self, event):
        self.on_send()

    def on_quit(self):
        self.client.close()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


def parse_args():
    p = argparse.ArgumentParser(prog="gui_chat_client", add_help=True)
    p.add_argument("--host", type=str, required=True)
    p.add_argument("--port", type=int, default=5001)
    p.add_argument("--username", type=str, default=getpass.getuser())
    p.add_argument("--log-dir", type=str, default=os.path.join(os.getcwd(), "chat_logs"))
    return p.parse_args()


def main():
    args = parse_args()
    app = ChatApp(args.host, args.port, args.username, args.log_dir)
    app.run()


if __name__ == "__main__":
    main()

