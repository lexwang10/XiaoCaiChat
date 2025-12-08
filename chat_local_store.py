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
            cur = self.db.execute(
                "INSERT INTO messages (conv, sender, ts, kind, text, self) VALUES (?,?,?,?,?,?)",
                (conv, sender, ts, kind, text, 1 if is_self else 0),
            )
            self.db.commit()
            try:
                return cur.lastrowid
            except Exception:
                return None
        except Exception:
            return None

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

    def recent_with_id(self, conv: str, limit: int = 100):
        try:
            cur = self.db.execute(
                "SELECT id, sender, ts, kind, text, self FROM messages WHERE conv=? ORDER BY id DESC LIMIT ?",
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

    def delete_message(self, conv: str, sender: str, kind: str, text: str, is_self: bool, filename: str = None, mime: str = None):
        try:
            if kind == "file" and filename and mime:
                # stored text could be "[FILE] name mime" or include payload; match prefix
                self.db.execute(
                    "DELETE FROM messages WHERE conv=? AND sender=? AND kind='file' AND text LIKE ? AND self=?",
                    (conv, sender, f"[FILE] {filename} {mime}%", 1 if is_self else 0),
                )
            else:
                self.db.execute(
                    "DELETE FROM messages WHERE conv=? AND sender=? AND kind=? AND text=? AND self=?",
                    (conv, sender, kind, text, 1 if is_self else 0),
                )
            self.db.commit()
        except Exception:
            pass

    def delete_conv(self, conv: str):
        try:
            self.db.execute("DELETE FROM messages WHERE conv=?", (conv,))
            self.db.commit()
        except Exception:
            pass
