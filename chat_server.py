import argparse
import socket
import threading
from datetime import datetime
import sqlite3
import time
import os
import base64
import hmac
import hashlib
import json
import sys
import uuid
import cgi
import io
import urllib.parse
SERVER_VERSION = "1.0.2"
try:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
except Exception:
    BaseHTTPRequestHandler = None
    ThreadingHTTPServer = None
import webbrowser
try:
    from PySide6 import QtWidgets, QtGui, QtCore
except Exception:
    QtWidgets = None
    QtGui = None
    QtCore = None


class UnreadStore:
    def __init__(self):
        self._mem = {}
        self._client = None
        url = os.environ.get("REDIS_URL")
        if url:
            try:
                import redis
                self._client = redis.Redis.from_url(url)
            except Exception:
                self._client = None

    def inc(self, user: str, conv: str):
        if self._client:
            try:
                self._client.hincrby(f"unread:{user}", conv, 1)
                return
            except Exception:
                pass
        m = self._mem.setdefault(user, {})
        m[conv] = m.get(conv, 0) + 1

    def reset(self, user: str, conv: str):
        if self._client:
            try:
                self._client.hset(f"unread:{user}", conv, 0)
                return
            except Exception:
                pass
        m = self._mem.setdefault(user, {})
        m[conv] = 0

    def all(self, user: str):
        if self._client:
            try:
                d = self._client.hgetall(f"unread:{user}")
                out = {}
                for k, v in d.items():
                    try:
                        out[k.decode("utf-8")] = int(v)
                    except Exception:
                        pass
                return out
            except Exception:
                pass
        return dict(self._mem.get(user, {}))


class Hub:
    def __init__(self):
        self.rooms = {"世界": {}}
        self.conn_info = {}
        self.lock = threading.Lock()
        self.store = UnreadStore()
        self.avatars = {}
        self.avatar_data = {}
        self.room_names = {"世界": "世界"}
        self.room_members = {}
        try:
            self._load_rooms()
        except Exception:
            pass

    def add(self, conn: socket.socket, username: str, room: str) -> bool:
        # Check permissions
        allowed = self.room_members.get(room)
        if allowed is not None and username not in allowed:
            # Not allowed in this room
            return False

        with self.lock:
            room_map = self.rooms.setdefault(room, {})
            room_map[conn] = username
            self.conn_info[conn] = (room, username)
        av = self.avatars.get(username) or ""
        self.broadcast_sys(room, f"[SYS] JOIN {room} {username} {av}")
        self.broadcast_users(room)
        with self.lock:
            pass
        try:
            name = self.room_names.get(room, room)
            self.broadcast_sys(room, f"[SYS] ROOM_NAME {room} {name}")
        except Exception:
            pass
        return True

    def remove(self, conn: socket.socket):
        room = None
        username = None
        with self.lock:
            if conn in self.conn_info:
                room, username = self.conn_info[conn]
                del self.conn_info[conn]
                if room in self.rooms and conn in self.rooms[room]:
                    del self.rooms[room][conn]
        if room and username:
            self.broadcast_sys(room, f"[SYS] LEAVE {room} {username}")
            self.broadcast_users(room)

    def kick(self, username: str):
        to_remove = []
        with self.lock:
            for room_map in self.rooms.values():
                for conn, user in room_map.items():
                    if user == username:
                        to_remove.append(conn)
        for conn in to_remove:
            self.remove(conn)
            try:
                conn.sendall(b"[SYS] KICKED_LOGIN_CONFLICT\n")
            except Exception:
                pass
            try:
                conn.shutdown(socket.SHUT_RDWR)
                conn.close()
            except Exception:
                pass

    def users(self, room: str):
        with self.lock:
            return list(self.rooms.get(room, {}).values())

    def broadcast_text(self, room: str, origin: socket.socket, username: str, text: str):
        try:
            if text.startswith("FILE_META "):
                toks = text.split(" ")
                if len(toks) >= 5:
                    name = " ".join(toks[1:-3])
                    mime = toks[-3]
                    try:
                        tot = int(toks[-2])
                    except Exception:
                        tot = 0
                    md5 = toks[-1]
                    print(f"[srv] group FILE_META from={username} room={room} name={name} mime={mime} size={tot} md5={md5}")
            elif text.startswith("FILE_BEGIN "):
                toks = text.split(" ")
                if len(toks) >= 4:
                    name = " ".join(toks[1:-2])
                    mime = toks[-2]
                    try:
                        tot = int(toks[-1])
                    except Exception:
                        tot = 0
                    print(f"[srv] group FILE_BEGIN from={username} room={room} name={name} mime={mime} size={tot}")
            elif text.startswith("FILE_CHUNK "):
                toks = text.split(" ", 2)
                if len(toks) >= 3:
                    off = 0
                    b64 = ""
                    is_old = False
                    try:
                        off = int(toks[1])
                        b64 = toks[2]
                        is_old = True
                    except Exception:
                        is_old = False
                    
                    if is_old:
                        print(f"[srv] group FILE_CHUNK from={username} room={room} off={off} len={len(b64)}")
                    else:
                        # New format: FILE_CHUNK filename offset b64
                        filename = toks[1]
                        rest = toks[2].split(" ", 1)
                        if len(rest) >= 2:
                            try:
                                off = int(rest[0])
                            except Exception:
                                off = 0
                            b64 = rest[1]
                            print(f"[srv] group FILE_CHUNK from={username} room={room} name={filename} off={off} len={len(b64)}")
            elif text.startswith("FILE_END"):
                print(f"[srv] group FILE_END from={username} room={room}")
            elif text.startswith("FILE_ACK "):
                toks = text.split(" ")
                if len(toks) >= 4:
                    md5 = toks[1]
                    try:
                        off = int(toks[2])
                    except Exception:
                        off = 0
                    try:
                        wrote = int(toks[3])
                    except Exception:
                        wrote = 0
                    print(f"[srv] group FILE_ACK from={username} room={room} md5={md5} off={off} wrote={wrote}")
            elif text.startswith("FILE_HAVE "):
                toks = text.split(" ", 3)
                if len(toks) >= 4:
                    md5 = toks[1]
                    try:
                        written = int(toks[2])
                    except Exception:
                        written = 0
                    status = toks[3]
                    print(f"[srv] group FILE_HAVE from={username} room={room} md5={md5} written={written} status={status}")
            elif text.startswith("FILE_QUERY "):
                toks = text.split(" ", 1)
                md5 = toks[1] if len(toks) >= 2 else ""
                print(f"[srv] group FILE_QUERY from={username} room={room} md5={md5}")
            elif text.startswith("FILE_CANCEL "):
                toks = text.split(" ", 1)
                name = toks[1] if len(toks) >= 2 else ""
                print(f"[srv] group FILE_CANCEL from={username} room={room} name={name}")
            elif text.startswith("[FILE] "):
                toks = text.split(" ")
                if len(toks) >= 4:
                    mime = toks[-2]
                    b64 = toks[-1]
                    name = " ".join(toks[1:-2])
                    print(f"[srv] group INLINE_FILE from={username} room={room} name={name} mime={mime} len={len(b64)}")
        except Exception:
            pass
        msg = f"{username}> {text}\n".encode("utf-8")
        targets = []
        with self.lock:
            m = self.rooms.get(room, {})
            for c, u in list(m.items()):
                if c is origin:
                    continue
                targets.append((c, u))
        if text.startswith("FILE_ACK "):
            try:
                origin.sendall(msg)
                print(f"[srv] group SENT_ACK to={username}")
            except Exception:
                pass
        else:
            try:
                print(f"[srv] group FWD room={room} from={username} targets={len(targets)} head={text[:80]}")
            except Exception:
                pass
            for c, u in targets:
                try:
                    c.sendall(msg)
                    try:
                        print(f"[srv] group SENT to={u}")
                    except Exception:
                        pass
                except Exception:
                    pass
        save_message(conv_group(room), username, text)
        for _, u in targets:
            self.store.inc(u, conv_group(room))

    def broadcast_sys(self, room: str, line: str):
        payload = (line + "\n").encode("utf-8")
        with self.lock:
            conns = list(self.rooms.get(room, {}).keys())
        for c in conns:
            try:
                c.sendall(payload)
            except Exception:
                pass

    def broadcast_users(self, room: str):
        names = self.users(room)
        parts = []
        for u in names:
            fn = self.avatars.get(u)
            if fn:
                parts.append(f"{u}:{fn}")
            else:
                parts.append(u)
        users = ",".join(parts)
        self.broadcast_sys(room, f"[SYS] USERS {room} {users}")

    def set_avatar(self, room: str, username: str, filename: str):
        with self.lock:
            self.avatars[username] = filename
        self.broadcast_sys(room, f"[SYS] AVATAR {room} {username} {filename}")
        self.broadcast_users(room)

    def set_avatar_data(self, room: str, username: str, filename: str, mime: str, b64: str):
        with self.lock:
            self.avatars[username] = filename
            self.avatar_data[username] = (filename, mime, b64)
        self.broadcast_sys(room, f"[SYS] AVATAR {room} {username} {filename}")
        self.broadcast_users(room)

    def send_dm(self, room: str, origin: socket.socket, origin_user: str, target_user: str, text: str):
        try:
            if text.startswith("FILE_META "):
                toks = text.split(" ")
                if len(toks) >= 5:
                    name = " ".join(toks[1:-3])
                    mime = toks[-3]
                    try:
                        tot = int(toks[-2])
                    except Exception:
                        tot = 0
                    md5 = toks[-1]
                    print(f"[srv] dm FILE_META from={origin_user} to={target_user} name={name} mime={mime} size={tot} md5={md5}")
            elif text.startswith("FILE_BEGIN "):
                toks = text.split(" ")
                if len(toks) >= 4:
                    name = " ".join(toks[1:-2])
                    mime = toks[-2]
                    try:
                        tot = int(toks[-1])
                    except Exception:
                        tot = 0
                    print(f"[srv] dm FILE_BEGIN from={origin_user} to={target_user} name={name} mime={mime} size={tot}")
            elif text.startswith("FILE_CHUNK "):
                toks = text.split(" ", 2)
                if len(toks) >= 3:
                    off = 0
                    b64 = ""
                    is_old = False
                    try:
                        off = int(toks[1])
                        b64 = toks[2]
                        is_old = True
                    except Exception:
                        is_old = False
                    
                    if is_old:
                        print(f"[srv] dm FILE_CHUNK from={origin_user} to={target_user} off={off} len={len(b64)}")
                    else:
                        filename = toks[1]
                        rest = toks[2].split(" ", 1)
                        if len(rest) >= 2:
                            try:
                                off = int(rest[0])
                            except Exception:
                                off = 0
                            b64 = rest[1]
                            print(f"[srv] dm FILE_CHUNK from={origin_user} to={target_user} name={filename} off={off} len={len(b64)}")
            elif text.startswith("FILE_END"):
                print(f"[srv] dm FILE_END from={origin_user} to={target_user}")
            elif text.startswith("FILE_ACK "):
                toks = text.split(" ")
                if len(toks) >= 4:
                    md5 = toks[1]
                    try:
                        off = int(toks[2])
                    except Exception:
                        off = 0
                    try:
                        wrote = int(toks[3])
                    except Exception:
                        wrote = 0
                    print(f"[srv] dm FILE_ACK from={origin_user} to={target_user} md5={md5} off={off} wrote={wrote}")
            elif text.startswith("FILE_HAVE "):
                toks = text.split(" ", 3)
                if len(toks) >= 4:
                    md5 = toks[1]
                    try:
                        written = int(toks[2])
                    except Exception:
                        written = 0
                    status = toks[3]
                    print(f"[srv] dm FILE_HAVE from={origin_user} to={target_user} md5={md5} written={written} status={status}")
            elif text.startswith("FILE_QUERY "):
                toks = text.split(" ", 1)
                md5 = toks[1] if len(toks) >= 2 else ""
                print(f"[srv] dm FILE_QUERY from={origin_user} to={target_user} md5={md5}")
            elif text.startswith("FILE_CANCEL "):
                toks = text.split(" ", 1)
                name = toks[1] if len(toks) >= 2 else ""
                print(f"[srv] dm FILE_CANCEL from={origin_user} to={target_user} name={name}")
            elif text.startswith("[FILE] "):
                toks = text.split(" ")
                if len(toks) >= 4:
                    mime = toks[-2]
                    b64 = toks[-1]
                    name = " ".join(toks[1:-2])
                    print(f"[srv] dm INLINE_FILE from={origin_user} to={target_user} name={name} mime={mime} len={len(b64)}")
        except Exception:
            pass
        targets = []
        with self.lock:
            for c, u in self.rooms.get(room, {}).items():
                if u == target_user:
                    targets.append(c)
        try:
            print(f"[srv] dm FWD room={room} from={origin_user} to={target_user} targets={len(targets)} head={text[:80]}")
        except Exception:
            pass
        payload_target = f"[DM] FROM {origin_user} {text}\n".encode("utf-8")
        payload_origin = f"[DM] TO {target_user} {text}\n".encode("utf-8")
        for c in targets:
            try:
                c.sendall(payload_target)
                try:
                    print(f"[srv] dm SENT to={target_user}")
                except Exception:
                    pass
            except Exception:
                pass
        # Reduce backpressure: do not echo giant payloads back to origin
        if not (text.startswith("FILE_CHUNK ") or text.startswith("[FILE] ")):
            try:
                origin.sendall(payload_origin)
            except Exception:
                pass
        save_message(conv_dm(origin_user, target_user), origin_user, text)
        self.store.inc(target_user, conv_dm(origin_user, target_user))

    def _load_rooms(self):
        try:
            rooms, names, members = _load_rooms_json()
            if rooms:
                with self.lock:
                    for r in rooms:
                        if r not in self.rooms:
                            self.rooms[r] = {}
            if names:
                with self.lock:
                    for r, n in names.items():
                        self.room_names[r] = n
            if members:
                with self.lock:
                    for r, m in members.items():
                        self.room_members[r] = set(m)
        except Exception:
            pass

    def _save_rooms(self):
        try:
            with self.lock:
                rooms = list(self.rooms.keys())
                names = dict(self.room_names)
                members = {r: list(m) for r, m in self.room_members.items()}
            _save_rooms_json(rooms, names, members)
        except Exception:
            pass

    def delete_user(self, username: str):
        # 1. Remove from all rooms in memory and disconnect
        with self.lock:
            for room_id, room_map in self.rooms.items():
                to_remove = []
                for conn, user in room_map.items():
                    if user == username:
                        to_remove.append(conn)
                for conn in to_remove:
                    del room_map[conn]
                    if conn in self.conn_info:
                        del self.conn_info[conn]
                    try:
                        conn.shutdown(socket.SHUT_RDWR)
                        conn.close()
                    except Exception:
                        pass
        
        # 2. Remove from room_members (persistent membership)
        changed_rooms = False
        with self.lock:
            for room_id in self.room_members:
                if username in self.room_members[room_id]:
                    self.room_members[room_id].remove(username)
                    changed_rooms = True
            
            # 3. Remove avatar
            if username in self.avatars:
                del self.avatars[username]
            if username in self.avatar_data:
                del self.avatar_data[username]

        if changed_rooms:
            self._save_rooms()
        
        # 4. Remove from REGISTERED_USERS
        global REGISTERED_USERS
        if username in REGISTERED_USERS:
            REGISTERED_USERS.remove(username)
            _save_users()

    def _unread_inc(self, user: str, conv: str):
        self.store.inc(user, conv)

    def _unread_reset(self, user: str, conv: str):
        self.store.reset(user, conv)

    def _unread_all(self, user: str):
        return self.store.all(user)


_data_dir = os.path.expanduser("~/Library/Application Support/XiaoCaiChatServer")
try:
    os.makedirs(_data_dir, exist_ok=True)
except Exception:
    pass
CONFIG_JSON = os.path.join(_data_dir, "config.json")
SERVER_CONFIG = {"retention_days": 7}
ADMIN_PASSWORD = "123!@#qwe"
ADMIN_SESSIONS = set()
USERS_JSON = os.path.join(_data_dir, "users.json")
REGISTERED_USERS = set()

def _load_config():
    try:
        if os.path.isfile(CONFIG_JSON):
            with open(CONFIG_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                SERVER_CONFIG.update(data)
    except Exception:
        pass

def _save_config():
    try:
        with open(CONFIG_JSON, "w", encoding="utf-8") as f:
            json.dump(SERVER_CONFIG, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

_load_config()

FILES_DIR = os.path.join(_data_dir, "files")
try:
    os.makedirs(FILES_DIR, exist_ok=True)
except Exception:
    pass
DB_PATH = os.path.join(_data_dir, "chat_history.db")
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute(
    "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, conv TEXT, sender TEXT, ts INTEGER, text TEXT)"
)
db.commit()
def _load_users():
    global REGISTERED_USERS
    try:
        if os.path.isfile(USERS_JSON):
            with open(USERS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            users = data.get("users") or []
            REGISTERED_USERS = set([str(u) for u in users if isinstance(u, str)])
        else:
            REGISTERED_USERS = set()
    except Exception:
        REGISTERED_USERS = set()
def _save_users():
    try:
        data = {"users": sorted(list(REGISTERED_USERS))}
        with open(USERS_JSON, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def conv_group(room: str):
    return f"group:{room}"

def conv_dm(a: str, b: str):
    x, y = sorted([a, b])
    return f"dm:{x}&{y}"

def save_message(conv: str, sender: str, text: str):
    try:
        if text.startswith("FILE_CHUNK ") or text.startswith("FILE_ACK "):
            return
        if len(text) >= 262144:
            return
        ts = int(time.time())
        db.execute("INSERT INTO messages (conv, sender, ts, text) VALUES (?,?,?,?)", (conv, sender, ts, text))
        db.commit()
    except Exception:
        pass

ROOMS_JSON = os.path.join(_data_dir, "rooms.json")

def _load_rooms_json():
    try:
        if os.path.isfile(ROOMS_JSON):
            with open(ROOMS_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
            rooms = data.get("rooms") or []
            names = data.get("names") or {}
            members = data.get("members") or {}
            return rooms, names, members
    except Exception:
        pass
    return None, None, None

def _save_rooms_json(rooms: list[str], names: dict[str,str], members: dict[str, list[str]]):
    try:
        data = {"rooms": rooms, "names": names, "members": members}
        with open(ROOMS_JSON, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def load_recent(conv: str, limit: int):
    try:
        cur = db.execute("SELECT sender, ts, text FROM messages WHERE conv=? ORDER BY id DESC LIMIT ?", (conv, limit))
        rows = cur.fetchall()
        rows.reverse()
        return rows
    except Exception:
        return []


def handle_client(conn: socket.socket, addr, hub: Hub):
    f = conn.makefile("r", encoding="utf-8", newline="\n")
    username = addr[0]
    room = "世界"
    if not REGISTERED_USERS:
        try:
            _load_users()
        except Exception:
            pass
    def parse_dm(line: str):
        s = line.strip()
        if not s:
            return None
        if s[:2].upper() == "DM" and (len(s) == 2 or s[2] == " "):
            parts = s.split(" ", 2)
            if len(parts) >= 3 and parts[1]:
                return (parts[1], parts[2])
        return None
    def parse_seq(line: str):
        s = line.strip()
        if s.startswith("SEQ "):
            parts = s.split(" ", 2)
            if len(parts) >= 2:
                try:
                    n = int(parts[1])
                except Exception:
                    n = None
                rest = parts[2] if len(parts) >= 3 else ""
                return (n, rest)
        return (None, line)
    def parse_hist(line: str):
        s = line.strip()
        if s.startswith("HIST "):
            parts = s.split()
            if len(parts) >= 2 and parts[1].upper() == "GROUP":
                n = int(parts[2]) if len(parts) >= 3 else 50
                return ("GROUP", room, n)
            if len(parts) >= 3 and parts[1].upper() == "DM":
                peer = parts[2]
                n = int(parts[3]) if len(parts) >= 4 else 50
                return ("DM", peer, n)
        return None
    def parse_unread(line: str):
        s = line.strip()
        if s.upper() == "UNREAD":
            return True
        return False
    def parse_read(line: str):
        s = line.strip()
        if s.startswith("READ "):
            parts = s.split()
            if len(parts) >= 2 and parts[1].upper() == "GROUP":
                return ("GROUP", room)
            if len(parts) >= 3 and parts[1].upper() == "DM":
                return ("DM", parts[2])
        return None
    def parse_avatar_upload(line: str):
        s = line.strip()
        if s.startswith("AVATAR_UPLOAD "):
            parts = s.split(" ", 3)
            if len(parts) >= 4:
                return (parts[1], parts[2], parts[3])
        return None
    def parse_avatar_req(line: str):
        s = line.strip()
        if s.startswith("AVATAR_REQ "):
            parts = s.split()
            if len(parts) >= 2:
                return parts[1]
        return None
    def parse_avatar(line: str):
        s = line.strip()
        if s.startswith("AVATAR "):
            parts = s.split()
            if len(parts) >= 2:
                return parts[1]
        return None
    SECRET = os.environ.get("CHAT_SECRET")
    JWT_SECRET = os.environ.get("JWT_SECRET")
    authed = (SECRET is None and JWT_SECRET is None)

    def b64url_decode(s: str):
        pad = "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s + pad)

    def try_auth(body: str):
        if JWT_SECRET and body.startswith("AUTH_JWT "):
            token = body.split(" ", 1)[1]
            parts = token.split(".")
            if len(parts) != 3:
                return None
            try:
                header = json.loads(b64url_decode(parts[0]).decode("utf-8"))
                payload = json.loads(b64url_decode(parts[1]).decode("utf-8"))
            except Exception:
                return None
            if header.get("alg") != "HS256":
                return None
            mac = hmac.new(JWT_SECRET.encode("utf-8"), (parts[0] + "." + parts[1]).encode("utf-8"), hashlib.sha256).digest()
            sig = base64.urlsafe_b64encode(mac).rstrip(b"=")
            if not hmac.compare_digest(sig, parts[2].encode("utf-8")):
                return None
            sub = payload.get("sub")
            if not sub:
                return None
            return (sub, True)
        if SECRET and body.startswith("AUTH "):
            parts = body.split()
            if len(parts) >= 4:
                user = parts[1]
                ts = parts[2]
                mac_hex = parts[3]
                want = hmac.new(SECRET.encode("utf-8"), f"{user}:{ts}".encode("utf-8"), hashlib.sha256).hexdigest()
                if hmac.compare_digest(want, mac_hex):
                    return (user, True)
            # legacy: full secret
            if body.strip() == f"AUTH {SECRET}":
                return (username, True)
        return None
    try:
        first = f.readline()
        if first.startswith("NAME_CHECK "):
            parts = first.strip().split()
            r = parts[1] if len(parts) >= 2 else room
            u = parts[2] if len(parts) >= 3 else username
            taken = (u in REGISTERED_USERS)
            try:
                if taken:
                    conn.sendall(f"[SYS] NAME_TAKEN {r} {u}\n".encode("utf-8"))
                else:
                    conn.sendall(f"[SYS] NAME_OK {r} {u}\n".encode("utf-8"))
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            return
        if first.startswith("HELLO "):
            parts = first.strip().split()
            if len(parts) >= 3:
                username = parts[1] or addr[0]
                room = parts[2] or "世界"
            else:
                username = first[len("HELLO ") :].rstrip("\n") or addr[0]
            if username in hub.users(room):
                hub.kick(username)
            if username not in REGISTERED_USERS:
                try:
                    REGISTERED_USERS.add(username)
                    _save_users()
                except Exception:
                    pass
            if not hub.add(conn, username, room):
                try:
                    conn.sendall(f"[SYS] ERROR Permission denied for room {room}\n".encode("utf-8"))
                    conn.close()
                except Exception:
                    pass
                return
            print(f"joined {username}@{addr[0]}:{addr[1]} room={room}")
            if len(parts) >= 4 and parts[3]:
                hub.set_avatar(room, username, parts[3])
        else:
            text = first.rstrip("\n")
            if username in hub.users(room):
                hub.kick(username)
            if username not in REGISTERED_USERS:
                try:
                    REGISTERED_USERS.add(username)
                    _save_users()
                except Exception:
                    pass
            if not hub.add(conn, username, room):
                try:
                    conn.sendall(f"[SYS] ERROR Permission denied for room {room}\n".encode("utf-8"))
                    conn.close()
                except Exception:
                    pass
                return
            print(f"joined {username}@{addr[0]}:{addr[1]} room={room}")
            if text:
                seq, body = parse_seq(text)
                if body.startswith("PING "):
                    try:
                        conn.sendall(("PONG " + body[5:] + "\n").encode("utf-8"))
                    except Exception:
                        pass
                else:
                    do_process = True
                    if not authed:
                        auth_res = try_auth(body)
                        if auth_res:
                            username, authed = auth_res[0], True
                        else:
                            do_process = False
                            if seq is not None:
                                try:
                                    conn.sendall((f"[ACK] {seq}\n").encode("utf-8"))
                                except Exception:
                                    pass
                    if do_process:
                        h = parse_hist(body)
                        if h:
                            kind, p, n = h
                            if kind == "GROUP":
                                for sender, ts, txt in load_recent(conv_group(room), n):
                                    try:
                                        conn.sendall(f"[SYS] HISTORY GROUP {room} {sender} {ts} {txt}\n".encode("utf-8"))
                                    except Exception:
                                        pass
                            else:
                                for sender, ts, txt in load_recent(conv_dm(username, p), n):
                                    try:
                                        conn.sendall(f"[SYS] HISTORY DM {p} {sender} {ts} {txt}\n".encode("utf-8"))
                                    except Exception:
                                        pass
                        else:
                            up = parse_avatar_upload(body)
                            if up:
                                fn, mime, b64 = up
                                hub.set_avatar_data(room, username, fn, mime, b64)
                            else:
                                req_user = parse_avatar_req(body)
                                if req_user:
                                    d = hub.avatar_data.get(req_user)
                                    if d:
                                        fn, mime, b64 = d
                                        try:
                                            conn.sendall(f"[SYS] AVATAR_DATA {room} {req_user} {fn} {mime} {b64}\n".encode("utf-8"))
                                        except Exception:
                                            pass
                                elif parse_unread(body):
                                    for conv, cnt in hub._unread_all(username).items():
                                        try:
                                            conn.sendall(f"[SYS] UNREAD {conv} {cnt}\n".encode("utf-8"))
                                        except Exception:
                                            pass
                    else:
                        r = parse_read(body)
                        if r:
                            kind, p = r
                            if kind == "GROUP":
                                hub._unread_reset(username, conv_group(room))
                            else:
                                hub._unread_reset(username, conv_dm(username, p))
                        else:
                            av = parse_avatar(body)
                            if av:
                                hub.set_avatar(room, username, av)
                            else:
                                dm = parse_dm(body)
                                if dm:
                                    target, payload = dm
                                    hub.send_dm(room, conn, username, target, payload)
                                else:
                                    if body.startswith("MSG "):
                                        hub.broadcast_text(room, conn, username, body[4:])
                                    elif body.startswith("AVATAR_UPLOAD ") or body.startswith("AVATAR_REQ "):
                                        pass
                                    else:
                                        hub.broadcast_text(room, conn, username, body)
                        if seq is not None and do_process:
                            try:
                                conn.sendall((f"[ACK] {seq}\n").encode("utf-8"))
                            except Exception:
                                pass
            for line in f:
                t = line.rstrip("\n")
                if not t:
                    continue
                seq, body = parse_seq(t)
                if body.startswith("PING "):
                    try:
                        conn.sendall(("PONG " + body[5:] + "\n").encode("utf-8"))
                    except Exception:
                        pass
                    continue
                if not authed:
                    auth_res = try_auth(body)
                    if auth_res:
                        username, authed = auth_res[0], True
                    else:
                        if seq is not None:
                            try:
                                conn.sendall((f"[ACK] {seq}\n").encode("utf-8"))
                            except Exception:
                                pass
                        continue
                h = parse_hist(body)
                if h:
                    kind, p, n = h
                    if kind == "GROUP":
                        for sender, ts, txt in load_recent(conv_group(room), n):
                            try:
                                conn.sendall(f"[SYS] HISTORY GROUP {room} {sender} {ts} {txt}\n".encode("utf-8"))
                            except Exception:
                                pass
                    else:
                        for sender, ts, txt in load_recent(conv_dm(username, p), n):
                            try:
                                conn.sendall(f"[SYS] HISTORY DM {p} {sender} {ts} {txt}\n".encode("utf-8"))
                            except Exception:
                                pass
                    if seq is not None:
                        try:
                            conn.sendall((f"[ACK] {seq}\n").encode("utf-8"))
                        except Exception:
                            pass
                    continue
                up = parse_avatar_upload(body)
                if up:
                    fn, mime, b64 = up
                    hub.set_avatar_data(room, username, fn, mime, b64)
                else:
                    req_user = parse_avatar_req(body)
                    if req_user:
                        d = hub.avatar_data.get(req_user)
                        if d:
                            fn, mime, b64 = d
                            try:
                                conn.sendall(f"[SYS] AVATAR_DATA {room} {req_user} {fn} {mime} {b64}\n".encode("utf-8"))
                            except Exception:
                                pass
                    elif parse_unread(body):
                        for conv, cnt in hub._unread_all(username).items():
                            try:
                                conn.sendall(f"[SYS] UNREAD {conv} {cnt}\n".encode("utf-8"))
                            except Exception:
                                pass
                    else:
                        r = parse_read(body)
                        if r:
                            kind, p = r
                            if kind == "GROUP":
                                hub._unread_reset(username, conv_group(room))
                            else:
                                hub._unread_reset(username, conv_dm(username, p))
                        else:
                            av = parse_avatar(body)
                            if av:
                                hub.set_avatar(room, username, av)
                            else:
                                dm = parse_dm(body)
                                if dm:
                                    target, payload = dm
                                    hub.send_dm(room, conn, username, target, payload)
                                else:
                                    if body.startswith("MSG "):
                                        hub.broadcast_text(room, conn, username, body[4:])
                                    elif body.startswith("AVATAR_UPLOAD ") or body.startswith("AVATAR_REQ "):
                                        pass
                                    else:
                                        hub.broadcast_text(room, conn, username, body)
                if seq is not None:
                    try:
                        conn.sendall((f"[ACK] {seq}\n").encode("utf-8"))
                    except Exception:
                        pass
            return
        hub.add(conn, username, room)
        print(f"joined {username}@{addr[0]}:{addr[1]} room={room}")
        for line in f:
            t = line.rstrip("\n")
            if not t:
                continue
            seq, body = parse_seq(t)
            try:
                print(f"recv body: len={len(body)} head={body[:80]}")
            except Exception:
                pass
            if body.startswith("PING "):
                try:
                    conn.sendall(("PONG " + body[5:] + "\n").encode("utf-8"))
                except Exception:
                    pass
                try:
                    conn.sendall(("PONG " + body[5:] + "\n").encode("utf-8"))
                except Exception:
                    pass
                continue
            if not authed:
                auth_res = try_auth(body)
                if auth_res:
                    username, authed = auth_res[0], True
                else:
                    if seq is not None:
                        try:
                            conn.sendall((f"[ACK] {seq}\n").encode("utf-8"))
                        except Exception:
                            pass
                    continue
            h = parse_hist(body)
            if h:
                kind, p, n = h
                if kind == "GROUP":
                    for sender, ts, txt in load_recent(conv_group(room), n):
                        try:
                            conn.sendall(f"[SYS] HISTORY GROUP {room} {sender} {ts} {txt}\n".encode("utf-8"))
                        except Exception:
                            pass
                else:
                    for sender, ts, txt in load_recent(conv_dm(username, p), n):
                        try:
                            conn.sendall(f"[SYS] HISTORY DM {p} {sender} {ts} {txt}\n".encode("utf-8"))
                        except Exception:
                            pass
                if seq is not None:
                    try:
                        conn.sendall((f"[ACK] {seq}\n").encode("utf-8"))
                    except Exception:
                        pass
                continue
            if parse_unread(body):
                for conv, cnt in hub._unread_all(username).items():
                    try:
                        conn.sendall(f"[SYS] UNREAD {conv} {cnt}\n".encode("utf-8"))
                    except Exception:
                        pass
            else:
                r = parse_read(body)
                if r:
                    kind, p = r
                    if kind == "GROUP":
                        hub._unread_reset(username, conv_group(room))
                    else:
                        hub._unread_reset(username, conv_dm(username, p))
                else:
                    dm = parse_dm(body)
                    if dm:
                        target, payload = dm
                        hub.send_dm(room, conn, username, target, payload)
                    else:
                        if body.startswith("MSG "):
                            hub.broadcast_text(room, conn, username, body[4:])
                        elif body.startswith("AVATAR_UPLOAD ") or body.startswith("AVATAR_REQ "):
                            pass
                        else:
                            hub.broadcast_text(room, conn, username, body)
            if seq is not None:
                try:
                    conn.sendall((f"[ACK] {seq}\n").encode("utf-8"))
                except Exception:
                    pass
    except Exception:
        pass
    finally:
        hub.remove(conn)
        try:
            conn.close()
        except Exception:
            pass
        print(f"left {username}@{addr[0]}:{addr[1]} room={room}")


def _cleanup_files_loop():
    while True:
        try:
            if os.path.isdir(FILES_DIR):
                now = time.time()
                days = SERVER_CONFIG.get("retention_days", 7)
                limit = days * 24 * 3600
                for f in os.listdir(FILES_DIR):
                    p = os.path.join(FILES_DIR, f)
                    if os.path.isfile(p):
                        try:
                            if now - os.path.getmtime(p) > limit:
                                os.remove(p)
                                print(f"[SYS] Removed old file {f}")
                        except Exception:
                            pass
        except Exception:
            pass
        time.sleep(24 * 3600)


def start_server(host: str, port: int):
    _load_users()
    hub = Hub()
    threading.Thread(target=_cleanup_files_loop, daemon=True).start()
    try:
        if ThreadingHTTPServer and BaseHTTPRequestHandler:
            def start_status_server():
                class H(BaseHTTPRequestHandler):
                    def _get_file_info(self):
                        try:
                            p = self.path.split("?", 1)[0]
                            if not p.startswith("/files/"):
                                return None, None, None
                            fid = p.split("/", 2)[2]
                            if not fid:
                                return None, None, None
                            for entry in os.listdir(FILES_DIR):
                                if entry.startswith(str(fid) + "__"):
                                    fname = entry.split("__", 1)[1]
                                    fpath = os.path.join(FILES_DIR, entry)
                                    return fid, fname, fpath
                        except Exception:
                            pass
                        return None, None, None

                    def _check_auth(self):
                        try:
                            cookie_header = self.headers.get("Cookie")
                            if not cookie_header:
                                return False
                            for cookie in cookie_header.split(";"):
                                cookie = cookie.strip()
                                if cookie.startswith("admin_session="):
                                    token = cookie.split("=", 1)[1]
                                    if token in ADMIN_SESSIONS:
                                        return True
                        except Exception:
                            pass
                        return False

                    def _login_page(self, error=""):
                        html = f"""
                        <html><head><meta charset="utf-8"><title>XiaoCaiChat Server Login</title>
                        <style>body{{font-family:-apple-system,Helvetica,Arial,sans-serif;padding:40px;display:flex;justify-content:center;align-items:center;height:100vh;background-color:#f5f5f5;margin:0}}.login-box{{background:white;padding:30px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,0.1);width:300px}}h1{{font-size:20px;margin:0 0 20px;text-align:center}}input{{width:100%;padding:10px;margin-bottom:10px;border:1px solid #ddd;border-radius:4px;box-sizing:border-box}}button{{width:100%;padding:10px;background:#007bff;color:white;border:none;border-radius:4px;cursor:pointer}}button:hover{{background:#0056b3}}.error{{color:red;font-size:14px;margin-bottom:10px;text-align:center}}</style>
                        </head><body>
                        <div class="login-box">
                            <h1>管理员登录</h1>
                            {f'<div class="error">{error}</div>' if error else ''}
                            <form method="post" action="/api/admin_login">
                                <input type="password" name="password" placeholder="请输入密码" required>
                                <button type="submit">登录</button>
                            </form>
                        </div>
                        </body></html>
                        """
                        b = html.encode("utf-8")
                        self.send_response(200)
                        self.send_header("Content-Type", "text/html; charset=utf-8")
                        self.send_header("Content-Length", str(len(b)))
                        self.end_headers()
                        try:
                            self.wfile.write(b)
                        except Exception:
                            pass

                    def do_HEAD(self):
                        try:
                            p = self.path.split("?", 1)[0]
                            if p.startswith("/files/"):
                                fid, fname, fpath = self._get_file_info()
                                if not (fname and fpath and os.path.isfile(fpath)):
                                    self.send_response(404)
                                    self.end_headers()
                                    return
                                try:
                                    sz = os.path.getsize(fpath)
                                except Exception:
                                    sz = 0
                                self.send_response(200)
                                self.send_header("Content-Type", "application/octet-stream")
                                self.send_header("Content-Length", str(sz))
                                try:
                                    self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
                                except Exception:
                                    pass
                                self.end_headers()
                                return
                            self.send_response(404)
                            self.end_headers()
                        except Exception:
                            pass

                    def do_GET(self):
                        try:
                            p = self.path.split("?", 1)[0]
                            if p.startswith("/files/"):
                                fid, fname, fpath = self._get_file_info()
                                if not (fname and fpath and os.path.isfile(fpath)):
                                    try:
                                        print(f"[HTTP] GET /files - not found fid={fid}")
                                    except Exception:
                                        pass
                                    self.send_response(404)
                                    self.end_headers()
                                    return
                                try:
                                    with open(fpath, "rb") as f:
                                        data = f.read()
                                except Exception:
                                    data = b""
                                try:
                                    print(f"[HTTP] GET /files - serve fid={fid} name={fname} size={len(data)}")
                                except Exception:
                                    pass
                                self.send_response(200)
                                self.send_header("Content-Type", "application/octet-stream")
                                self.send_header("Content-Length", str(len(data)))
                                try:
                                    self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
                                except Exception:
                                    pass
                                self.end_headers()
                                try:
                                    self.wfile.write(data)
                                except Exception:
                                    pass
                                return
                            if p == "/api/status":
                                rooms = list(hub.rooms.keys())
                                user_filter = None
                                try:
                                    qs = self.path.split("?", 1)
                                    if len(qs) > 1:
                                        for pair in qs[1].split("&"):
                                            if pair.startswith("user="):
                                                user_filter = pair.split("=", 1)[1]
                                                break
                                except Exception:
                                    pass
                                
                                final_rooms = []
                                for r in rooms:
                                    members = hub.room_members.get(r)
                                    if members is not None and user_filter:
                                        if user_filter not in members:
                                            continue
                                    final_rooms.append(r)
                                    
                                data = {"rooms": [{"id": r, "name": hub.room_names.get(r, r), "users": hub.users(r)} for r in final_rooms]}
                                b = json.dumps(data, ensure_ascii=False).encode("utf-8")
                                self.send_response(200)
                                self.send_header("Content-Type", "application/json; charset=utf-8")
                                self.send_header("Content-Length", str(len(b)))
                                self.end_headers()
                                try:
                                    self.wfile.write(b)
                                except Exception:
                                    pass
                            else:
                                if not self._check_auth():
                                    self._login_page()
                                    return
                                rooms = list(hub.rooms.keys())
                                name_map = {r: hub.room_names.get(r, r) for r in rooms}
                                msg_alert = ""
                                try:
                                    qs = self.path.split("?", 1)
                                    if len(qs) > 1:
                                        for pair in qs[1].split("&"):
                                            if pair == "msg=updated":
                                                msg_alert = "<script>alert('更新成功');window.location.href='/';</script>"
                                                break
                                            if pair == "msg=retention_updated":
                                                msg_alert = "<script>alert('设置保存成功');window.location.href='/';</script>"
                                                break
                                            if pair == "msg=name_updated":
                                                msg_alert = "<script>alert('设置房间名称成功');window.location.href='/';</script>"
                                                break
                                except Exception:
                                    pass
                                html = ["<html><head><meta charset=\"utf-8\"><title>XiaoCaiChat Server</title>",
                                        "<style>body{font-family:-apple-system,Helvetica,Arial,sans-serif;padding:20px}h1{font-size:20px;margin:0 0 14px}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:6px 10px}input,button{font-size:14px;padding:6px 10px;margin:4px}form{margin:12px 0}</style>",
                                        "</head><body>",
                                        msg_alert,
                                        "<h1>群聊后台管理</h1>",
                                        "<form method=\"post\" action=\"/api/quit\"><button type=\"submit\">关闭服务器</button></form>",
                                        f"<form method=\"post\" action=\"/api/set_retention\" style=\"border:1px solid #ddd;padding:10px;margin:10px 0;max_width:300px\"><div><b>系统设置</b></div><div style=\"margin-top:8px\"><label>文件保留天数: <input name=\"days\" type=\"number\" value=\"{SERVER_CONFIG.get('retention_days', 7)}\" style=\"width:60px\"></label> <button type=\"submit\">保存</button></div></form>",
                                        "<form method=\"post\" action=\"/api/add_room\"><input name=\"room\" placeholder=\"新房间ID\"><button type=\"submit\">添加房间</button></form>",
                                        "<table><tr><th>房间ID</th><th>显示名称</th><th>成员限制</th><th>在线用户</th><th>操作</th></tr>"]
                                for r in rooms:
                                    u = hub.users(r)
                                    m = hub.room_members.get(r)
                                    
                                    current_m = m if m else set()
                                    reg_users = sorted(list(REGISTERED_USERS))
                                    checks = []
                                    is_world = (r == "世界")
                                    
                                    for user in reg_users:
                                        if is_world:
                                            checks.append(f'<label style="display:block;margin:2px 0"><input type="checkbox" name="members" value="{user}" checked disabled> {user}</label>')
                                        else:
                                            checked = " checked" if user in current_m else ""
                                            checks.append(f'<label style="display:block;margin:2px 0"><input type="checkbox" name="members" value="{user}"{checked}> {user}</label>')
                                    
                                    if not is_world:
                                        for user in current_m:
                                            if user not in REGISTERED_USERS:
                                                checks.append(f'<label style="display:block;margin:2px 0"><input type="checkbox" name="members" value="{user}" checked> {user} (未注册)</label>')
                                    
                                    checks_html = "".join(checks) if checks else "<div style='color:#999'>无用户</div>"
                                    
                                    update_btn = "<button type=\"submit\" disabled>更新</button>" if is_world else "<button type=\"submit\">更新</button>"
                                    set_name_html = "<div style='color:#999;padding:6px 0;font-size:13px'>默认房间不可更改名称</div>" if is_world else f"<form method=\"post\" action=\"/api/set_room_name\"><input type=\"hidden\" name=\"room\" value=\"{r}\"><input name=\"name\" placeholder=\"新的显示名称\"><button type=\"submit\">设置名称</button></form>"
                                    
                                    del_btn = "<button type=\"submit\" disabled>删除房间</button>" if len(u) > 0 else "<button type=\"submit\" onclick=\"return confirm('确定要删除该房间吗？');\">删除房间</button>"
                                    del_form_html = "" if is_world else f"<form method=\"post\" action=\"/api/delete_room\"><input type=\"hidden\" name=\"room\" value=\"{r}\">{del_btn}</form>"
                                    
                                    html.append(
                                        f"<tr><td>{r}</td><td>{name_map.get(r,r)}</td>"
                                        f"<td><form method=\"post\" action=\"/api/set_room_members\" style=\"margin:0\"><input type=\"hidden\" name=\"room\" value=\"{r}\"><div style=\"max-height:150px;overflow-y:auto;border:1px solid #ddd;padding:5px;margin-bottom:5px;font-size:13px\">{checks_html}</div>{update_btn}</form></td>"
                                        f"<td>{len(u)}</td><td>"
                                        f"{set_name_html}"
                                        f"{del_form_html}"
                                        f"</td></tr>"
                                    )
                                html.append("</table>")
                                html.append("</body></html>")
                                b = "".join(html).encode("utf-8")
                                self.send_response(200)
                                self.send_header("Content-Type", "text/html; charset=utf-8")
                                self.send_header("Content-Length", str(len(b)))
                                self.end_headers()
                                try:
                                    self.wfile.write(b)
                                except Exception:
                                    pass
                        except Exception:
                            try:
                                self.send_response(500)
                                self.end_headers()
                            except Exception:
                                pass
                    def log_message(self, fmt, *args):
                        try:
                            pass
                        except Exception:
                            pass
                    def _read_form(self):
                        try:
                            n = int(self.headers.get("Content-Length") or "0")
                        except Exception:
                            n = 0
                        try:
                            body = self.rfile.read(n) if n > 0 else b""
                        except Exception:
                            body = b""
                        try:
                            s = body.decode("utf-8")
                            d = {}
                            for p in s.split("&"):
                                if not p:
                                    continue
                                kv = p.split("=", 1)
                                k = urllib.parse.unquote_plus(kv[0])
                                v = urllib.parse.unquote_plus(kv[1]) if len(kv) > 1 else ""
                                if k in d:
                                    d[k].append(v)
                                else:
                                    d[k] = [v]
                            return d
                        except Exception:
                            return {}
                    def do_POST(self):
                        try:
                            p = self.path
                            if p == "/api/admin_login":
                                form = self._read_form()
                                pwd = form.get("password", [""])[-1]
                                if pwd == ADMIN_PASSWORD:
                                    token = str(uuid.uuid4())
                                    ADMIN_SESSIONS.add(token)
                                    self.send_response(302)
                                    self.send_header("Location", "/")
                                    self.send_header("Set-Cookie", f"admin_session={token}; Path=/; HttpOnly")
                                    self.end_headers()
                                else:
                                    self._login_page(error="密码错误")
                                return

                            # Protect admin APIs
                            if p in ["/api/add_room", "/api/set_room_name", "/api/set_room_members", "/api/delete_room", "/api/delete_user", "/api/set_retention", "/api/quit"]:
                                if not self._check_auth():
                                    self.send_response(403)
                                    self.end_headers()
                                    return

                            if p == "/api/upload_file":
                                # read raw body first to allow robust parsing
                                try:
                                    n = int(self.headers.get("Content-Length") or "0")
                                except Exception:
                                    n = 0
                                try:
                                    raw = self.rfile.read(n) if n > 0 else b""
                                except Exception:
                                    raw = b""
                                # primary: cgi.FieldStorage over buffered body
                                try:
                                    form = cgi.FieldStorage(
                                        fp=io.BytesIO(raw),
                                        headers=self.headers,
                                        environ={
                                            'REQUEST_METHOD': 'POST',
                                            'CONTENT_TYPE': self.headers.get('Content-Type') or '',
                                            'CONTENT_LENGTH': str(len(raw)),
                                        }
                                    )
                                except Exception:
                                    form = None
                                try:
                                    print(f"[HTTP] POST /api/upload_file - ct={self.headers.get('Content-Type')} cl={self.headers.get('Content-Length')}")
                                except Exception:
                                    pass
                                room = ""
                                sender = ""
                                filename = ""
                                payload = b""
                                try:
                                    room = form.getvalue("room") or ""
                                except Exception:
                                    room = ""
                                try:
                                    sender = form.getvalue("sender") or ""
                                except Exception:
                                    sender = ""
                                try:
                                    fileitem = form["file"] if form and "file" in form else None
                                except Exception:
                                    fileitem = None
                                try:
                                    filename = (fileitem.filename if fileitem and getattr(fileitem, "filename", None) else "") or "file"
                                except Exception:
                                    filename = "file"
                                try:
                                    payload = (fileitem.file.read() if fileitem and hasattr(fileitem, "file") else b"")
                                except Exception:
                                    payload = b""
                                try:
                                    print(f"[HTTP] POST /api/upload_file - room={room} sender={sender} filename={filename} payload_len={len(payload)}")
                                except Exception:
                                    pass
                                # fallback: manual multipart parse when payload missing
                                try:
                                    ct = self.headers.get("Content-Type") or ""
                                    if (not payload or filename == "file") and ct.startswith("multipart/form-data;"):
                                        bmark = "boundary="
                                        bpos = ct.find(bmark)
                                        boundary = ct[bpos+len(bmark):].strip() if bpos >= 0 else ""
                                        if boundary:
                                            def _parse_multipart(data: bytes, boundary_str: str):
                                                out = {}
                                                sep = ("--" + boundary_str).encode("utf-8")
                                                parts = data.split(sep)
                                                for part in parts:
                                                    part = part.strip(b"\r\n")
                                                    if not part or part == b"--":
                                                        continue
                                                    try:
                                                        head, body = part.split(b"\r\n\r\n", 1)
                                                    except Exception:
                                                        continue
                                                    headers = head.decode("utf-8", errors="ignore").split("\r\n")
                                                    disp = ""
                                                    for h in headers:
                                                        if h.lower().startswith("content-disposition:"):
                                                            disp = h
                                                            break
                                                    name = None
                                                    fname = None
                                                    if disp:
                                                        # parse name="..." and filename="..."
                                                        try:
                                                            for token in disp.split(";"):
                                                                token = token.strip()
                                                                if token.startswith("name="):
                                                                    v = token.split("=",1)[1].strip().strip("\"")
                                                                    name = v
                                                                elif token.startswith("filename="):
                                                                    v = token.split("=",1)[1].strip().strip("\"")
                                                                    fname = v
                                                        except Exception:
                                                            pass
                                                    # body may end with \r\n before next boundary
                                                    data_bytes = body.rstrip(b"\r\n")
                                                    if name:
                                                        out[name] = {"filename": fname, "data": data_bytes}
                                                return out
                                            parsed = _parse_multipart(raw, boundary)
                                            try:
                                                room = (room or "").strip() or (parsed.get("room", {}).get("data") or b"").decode("utf-8", errors="ignore")
                                            except Exception:
                                                pass
                                            try:
                                                sender = (sender or "").strip() or (parsed.get("sender", {}).get("data") or b"").decode("utf-8", errors="ignore")
                                            except Exception:
                                                pass
                                            try:
                                                finfo = parsed.get("file") or {}
                                                if finfo:
                                                    filename = (finfo.get("filename") or "") or "file"
                                                    payload = finfo.get("data") or b""
                                            except Exception:
                                                pass
                                            try:
                                                print(f"[HTTP] POST /api/upload_file - fallback parsed room={room} sender={sender} filename={filename} payload_len={len(payload)}")
                                            except Exception:
                                                pass
                                except Exception:
                                    pass
                                try:
                                    os.makedirs(FILES_DIR, exist_ok=True)
                                except Exception:
                                    pass
                                fid = uuid.uuid4().hex
                                base = os.path.basename(filename)
                                save = fid + "__" + base
                                fpath = os.path.join(FILES_DIR, save)
                                try:
                                    with open(fpath, "wb") as f:
                                        f.write(payload)
                                except Exception:
                                    try:
                                        print(f"[HTTP] POST /api/upload_file - write failed fpath={fpath}")
                                    except Exception:
                                        pass
                                size = len(payload) if isinstance(payload, (bytes, bytearray)) else 0
                                host_hdr = self.headers.get("Host") or ""
                                url = f"http://{host_hdr}/files/{fid}" if host_hdr else f"/files/{fid}"
                                try:
                                    if room:
                                        hub.broadcast_sys(room, f"[SYS] FILE_LINK {room} {sender} {base} {size} {url}")
                                        print(f"[SYS] FILE_LINK {room} {sender} {base} {size} {url}")
                                except Exception:
                                    pass
                                resp = {"file_id": fid, "file_name": base, "size": size, "url": url}
                                b = json.dumps(resp, ensure_ascii=False).encode("utf-8")
                                self.send_response(200)
                                self.send_header("Content-Type", "application/json; charset=utf-8")
                                self.send_header("Content-Length", str(len(b)))
                                self.end_headers()
                                try:
                                    self.wfile.write(b)
                                except Exception:
                                    pass
                                return
                            if p == "/api/add_room":
                                form = self._read_form()
                                room = form.get("room", [""])[-1]
                                if room:
                                    with hub.lock:
                                        if room not in hub.rooms:
                                            hub.rooms[room] = {}
                                        if room not in hub.room_names:
                                            hub.room_names[room] = room
                                    try:
                                        hub._save_rooms()
                                    except Exception:
                                        pass
                                self.send_response(302)
                                self.send_header("Location", "/")
                                self.end_headers()
                                return
                            if p == "/api/set_room_name":
                                form = self._read_form()
                                room = form.get("room", [""])[-1]
                                name = form.get("name", [""])[-1]
                                if room and name and room != "世界":
                                    with hub.lock:
                                        hub.room_names[room] = name
                                    try:
                                        hub.broadcast_sys(room, f"[SYS] ROOM_NAME {room} {name}")
                                    except Exception:
                                        pass
                                    try:
                                        hub._save_rooms()
                                    except Exception:
                                        pass
                                self.send_response(302)
                                self.send_header("Location", "/?msg=name_updated")
                                self.end_headers()
                                return
                            if p == "/api/set_room_members":
                                form = self._read_form()
                                room = form.get("room", [""])[-1]
                                members_list = form.get("members", [])
                                if room:
                                    # World room is always open to everyone
                                    if room == "世界":
                                        with hub.lock:
                                            if room in hub.room_members:
                                                del hub.room_members[room]
                                        try:
                                            hub._save_rooms()
                                        except Exception:
                                            pass
                                    else:
                                        members_list = [x.strip() for x in members_list if x.strip()]
                                        with hub.lock:
                                            if members_list:
                                                hub.room_members[room] = set(members_list)
                                            else:
                                                if room in hub.room_members:
                                                    del hub.room_members[room]
                                        try:
                                            hub._save_rooms()
                                        except Exception:
                                            pass
                                self.send_response(302)
                                self.send_header("Location", "/?msg=updated")
                                self.end_headers()
                                return
                            if p == "/api/delete_room":
                                form = self._read_form()
                                room = form.get("room", [""])[-1]
                                if room and room != "世界":
                                    with hub.lock:
                                        users = hub.rooms.get(room, {})
                                        if room in hub.rooms and len(users) == 0:
                                            try:
                                                del hub.rooms[room]
                                            except Exception:
                                                pass
                                        if room in hub.room_names:
                                            try:
                                                del hub.room_names[room]
                                            except Exception:
                                                pass
                                    try:
                                        hub._save_rooms()
                                    except Exception:
                                        pass
                                self.send_response(302)
                                self.send_header("Location", "/")
                                self.end_headers()
                                return
                            if p == "/api/delete_user":
                                form = self._read_form()
                                username = form.get("username", [""])[-1]
                                if username:
                                    hub.delete_user(username)
                                self.send_response(200)
                                self.send_header("Content-Type", "application/json; charset=utf-8")
                                self.end_headers()
                                try:
                                    self.wfile.write(b'{"status":"ok"}')
                                except Exception:
                                    pass
                                return
                            if p == "/api/set_retention":
                                form = self._read_form()
                                try:
                                    days_val = form.get("days", ["7"])[-1]
                                    days = int(days_val)
                                    if days < 1: days = 1
                                    SERVER_CONFIG["retention_days"] = days
                                    _save_config()
                                except Exception:
                                    pass
                                self.send_response(302)
                                self.send_header("Location", "/?msg=retention_updated")
                                self.end_headers()
                                return
                            if p == "/api/quit":
                                self.send_response(200)
                                self.end_headers()
                                try:
                                    os._exit(0)
                                except Exception:
                                    pass
                            self.send_response(404)
                            self.end_headers()
                        except Exception:
                            try:
                                self.send_response(500)
                                self.end_headers()
                            except Exception:
                                pass
                try:
                    srv = ThreadingHTTPServer((host, 34568), H)
                except Exception:
                    return
                def _loop():
                    try:
                        srv.serve_forever()
                    except Exception:
                        pass
                    try:
                        srv.server_close()
                    except Exception:
                        pass
                t = threading.Thread(target=_loop, daemon=True)
                t.start()
            threading.Thread(target=start_status_server, daemon=True).start()
    except Exception:
        pass
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(100)
    print(f"group chat listening on {host}:{port}")
    try:
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr, hub), daemon=True)
            t.start()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            srv.close()
        except Exception:
            pass
    
    


def parse_args():
    p = argparse.ArgumentParser(prog="chat_server", add_help=True)
    p.add_argument("--host", type=str, default="auto")
    p.add_argument("--port", type=int, default=34567)
    return p.parse_args()


def main():
    args = parse_args()
    host = args.host
    if not host or host == "auto":
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            host = s.getsockname()[0]
            s.close()
        except Exception:
            try:
                host = socket.gethostbyname(socket.gethostname())
            except Exception:
                host = "127.0.0.1"
    
    def _run_server():
        start_server(host, args.port)
    base_dir = getattr(sys, "_MEIPASS", os.getcwd())
    try:
        os.chdir(base_dir)
    except Exception:
        pass
    if QtWidgets and QtGui:
        try:
            t = threading.Thread(target=_run_server, daemon=True)
            t.start()
            app = QtWidgets.QApplication([])
            try:
                app.setQuitOnLastWindowClosed(False)
            except Exception:
                pass
            icon = None
            try:
                icon = QtGui.QIcon(os.path.join(os.getcwd(), "icons", "ui", "server_menu.png"))
            except Exception:
                pass
            tray = QtWidgets.QSystemTrayIcon(icon if icon and not icon.isNull() else QtGui.QIcon())
            menu = QtWidgets.QMenu()
            act_open = menu.addAction("打开后台")
            act_quit = menu.addAction("退出")
            def _open():
                try:
                    webbrowser.open(f"http://{host}:34568/")
                except Exception:
                    pass
            def _quit():
                try:
                    os._exit(0)
                except Exception:
                    pass
            act_open.triggered.connect(_open)
            act_quit.triggered.connect(_quit)
            tray.setContextMenu(menu)
            tray.setToolTip("XiaoCaiChat Server")
            tray.setVisible(True)
            app.exec()
            return
        except Exception:
            pass
    _run_server()


if __name__ == "__main__":
    main()
