import os
import threading
from datetime import datetime


class ChatLogger:
    def __init__(self, log_dir: str, peer_label: str):
        self.log_dir = log_dir
        try:
            os.makedirs(self.log_dir, exist_ok=True)
        except Exception:
            try:
                self.log_dir = os.path.expanduser("~/Library/Application Support/XiaoCaiChat/logs")
                os.makedirs(self.log_dir, exist_ok=True)
            except Exception:
                # last resort: temp dir
                import tempfile
                self.log_dir = os.path.join(tempfile.gettempdir(), "XiaoCaiChat")
                try:
                    os.makedirs(self.log_dir, exist_ok=True)
                except Exception:
                    pass
        date = datetime.now().strftime("%Y%m%d")
        fname = f"chat_{peer_label}_{date}.log"
        self.path = os.path.join(self.log_dir, fname)
        self.lock = threading.Lock()

    def write(self, direction: str, username: str, text: str):
        ts = datetime.now().strftime("%H:%M:%S")
        if text.startswith("PONG ") or text.startswith("[ACK] "):
            return
        if text.startswith("[FILE] "):
            s = text.strip()
            tokens = s.split(" ")
            if len(tokens) >= 4:
                mime = tokens[-2]
                b64 = tokens[-1]
                name = " ".join(tokens[1:-2])
                line = f"[{ts}] {direction} {username}: [FILE] {name} {mime} len={len(b64)}\n"
            else:
                line = f"[{ts}] {direction} {username}: [FILE]\n"
        else:
            msg = text if len(text) <= 512 else (text[:512] + "...")
            line = f"[{ts}] {direction} {username}: {msg}\n"
        with self.lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line)
