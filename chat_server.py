import argparse
import socket
import threading
from datetime import datetime


class Hub:
    def __init__(self):
        self.rooms = {}
        self.conn_info = {}
        self.lock = threading.Lock()

    def add(self, conn: socket.socket, username: str, room: str):
        with self.lock:
            room_map = self.rooms.setdefault(room, {})
            room_map[conn] = username
            self.conn_info[conn] = (room, username)
        self.broadcast_sys(room, f"[SYS] JOIN {room} {username}")
        self.broadcast_users(room)

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
        msg = f"{username}> {text}\n".encode("utf-8")
        to_send = []
        with self.lock:
            for c in list(self.rooms.get(room, {}).keys()):
                if c is origin:
                    continue
                to_send.append(c)
        for c in to_send:
            try:
                c.sendall(msg)
            except Exception:
                pass

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
        users = ",".join(self.users(room))
        self.broadcast_sys(room, f"[SYS] USERS {room} {users}")


def handle_client(conn: socket.socket, addr, hub: Hub):
    f = conn.makefile("r", encoding="utf-8", newline="\n")
    username = addr[0]
    room = "general"
    try:
        first = f.readline()
        if first.startswith("HELLO "):
            parts = first.strip().split()
            if len(parts) >= 3:
                username = parts[1] or addr[0]
                room = parts[2] or "general"
            else:
                username = first[len("HELLO ") :].rstrip("\n") or addr[0]
        else:
            text = first.rstrip("\n")
            hub.add(conn, username, room)
            print(f"joined {username}@{addr[0]}:{addr[1]} room={room}")
            if text:
                hub.broadcast_text(room, conn, username, text)
            for line in f:
                t = line.rstrip("\n")
                if not t:
                    continue
                hub.broadcast_text(room, conn, username, t)
            return
        hub.add(conn, username, room)
        print(f"joined {username}@{addr[0]}:{addr[1]} room={room}")
        for line in f:
            t = line.rstrip("\n")
            if not t:
                continue
            hub.broadcast_text(room, conn, username, t)
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
