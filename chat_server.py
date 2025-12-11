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
        self.rooms = {}
        self.conn_info = {}
        self.lock = threading.Lock()
        self.store = UnreadStore()
        self.avatars = {}
        self.avatar_data = {}

    def add(self, conn: socket.socket, username: str, room: str):
        with self.lock:
            room_map = self.rooms.setdefault(room, {})
            room_map[conn] = username
            self.conn_info[conn] = (room, username)
        av = self.avatars.get(username) or ""
        self.broadcast_sys(room, f"[SYS] JOIN {room} {username} {av}")
        self.broadcast_users(room)
        with self.lock:
            pass

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

    def users(self, room: str):
        with self.lock:
            return list(self.rooms.get(room, {}).values())

    def broadcast_text(self, room: str, origin: socket.socket, username: str, text: str):
        try:
            print(f"broadcast_text from {username}: len={len(text)} head={text[:80]}")
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
        for c, u in targets:
            try:
                c.sendall(msg)
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
            print(f"send_dm {origin_user} -> {target_user}: len={len(text)} head={text[:80]}")
        except Exception:
            pass
        targets = []
        with self.lock:
            for c, u in self.rooms.get(room, {}).items():
                if u == target_user:
                    targets.append(c)
        payload_target = f"[DM] FROM {origin_user} {text}\n".encode("utf-8")
        payload_origin = f"[DM] TO {target_user} {text}\n".encode("utf-8")
        for c in targets:
            try:
                c.sendall(payload_target)
            except Exception:
                pass
        try:
            origin.sendall(payload_origin)
        except Exception:
            pass
        save_message(conv_dm(origin_user, target_user), origin_user, text)
        self.store.inc(target_user, conv_dm(origin_user, target_user))

    def _unread_inc(self, user: str, conv: str):
        self.store.inc(user, conv)

    def _unread_reset(self, user: str, conv: str):
        self.store.reset(user, conv)

    def _unread_all(self, user: str):
        return self.store.all(user)


DB_PATH = os.path.join(os.getcwd(), "chat_history.db")
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute(
    "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, conv TEXT, sender TEXT, ts INTEGER, text TEXT)"
)
db.commit()

def conv_group(room: str):
    return f"group:{room}"

def conv_dm(a: str, b: str):
    x, y = sorted([a, b])
    return f"dm:{x}&{y}"

def save_message(conv: str, sender: str, text: str):
    try:
        ts = int(time.time())
        db.execute("INSERT INTO messages (conv, sender, ts, text) VALUES (?,?,?,?)", (conv, sender, ts, text))
        db.commit()
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
    room = "general"
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
        if first.startswith("HELLO "):
            parts = first.strip().split()
            if len(parts) >= 3:
                username = parts[1] or addr[0]
                room = parts[2] or "general"
            else:
                username = first[len("HELLO ") :].rstrip("\n") or addr[0]
            hub.add(conn, username, room)
            print(f"joined {username}@{addr[0]}:{addr[1]} room={room}")
            if len(parts) >= 4 and parts[3]:
                hub.set_avatar(room, username, parts[3])
        else:
            text = first.rstrip("\n")
            hub.add(conn, username, room)
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


def start_server(host: str, port: int):
    hub = Hub()
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
    p.add_argument("--host", type=str, default="0.0.0.0")
    p.add_argument("--port", type=int, default=5001)
    return p.parse_args()


def main():
    args = parse_args()
    start_server(args.host, args.port)


if __name__ == "__main__":
    main()
