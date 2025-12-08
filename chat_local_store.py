import os
import sqlite3
import time


class LocalStore:
    def __init__(self, base_dir: str, username: str):
        self.root = os.path.join(base_dir, username)
        os.makedirs(self.root, exist_ok=True)
        self.path = os.path.join(self.root, "local.db")
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, conv TEXT, sender TEXT, ts INTEGER, kind TEXT, text TEXT, self INTEGER)"
        )
        self.db.commit()

    def add(self, conv: str, sender: str, text: str, kind: str, is_self: bool, ts: int = None):
        if ts is None:
            ts = int(time.time())
        try:
            self.db.execute(
                "INSERT INTO messages (conv, sender, ts, kind, text, self) VALUES (?,?,?,?,?,?)",
                (conv, sender, ts, kind, text, 1 if is_self else 0),
            )
            self.db.commit()
        except Exception:
            pass

    def recent(self, conv: str, limit: int = 100):
        try:
            cur = self.db.execute(
                "SELECT sender, ts, kind, text, self FROM messages WHERE conv=? ORDER BY id DESC LIMIT ?",
                (conv, limit),
            )
            rows = cur.fetchall()
            rows.reverse()
            return rows
        except Exception:
            return []

    def peers(self):
        try:
            cur = self.db.execute("SELECT DISTINCT conv FROM messages WHERE conv LIKE 'dm:%'")
            names = []
            for (conv,) in cur.fetchall():
                names.append(conv.split(":", 1)[1])
            return names
        except Exception:
            return []

