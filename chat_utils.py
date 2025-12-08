import os
import threading
from datetime import datetime


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

