import argparse
import socket
from typing import Optional
import os
import getpass
import base64
import json
import hmac
import hashlib
import time

from PySide6 import QtCore, QtWidgets, QtGui
from chat_utils import ChatLogger
from chat_local_store import LocalStore


class Receiver(QtCore.QThread):
    received = QtCore.Signal(str)

    def __init__(self, sock: socket.socket):
        super().__init__()
        self.sock = sock
        self.running = True

    def run(self):
        f = self.sock.makefile("r", encoding="utf-8", newline="\n")
        for line in f:
            if not self.running:
                break
            t = line.rstrip("\n")
            if t:
                self.received.emit(t)

    def stop(self):
        self.running = False


class ChatModel(QtCore.QAbstractListModel):
    TextRole = QtCore.Qt.UserRole + 1
    SenderRole = QtCore.Qt.UserRole + 2
    KindRole = QtCore.Qt.UserRole + 3
    SelfRole = QtCore.Qt.UserRole + 4
    TimeRole = QtCore.Qt.UserRole + 5
    PixmapRole = QtCore.Qt.UserRole + 6
    FileNameRole = QtCore.Qt.UserRole + 7
    MimeRole = QtCore.Qt.UserRole + 8

    def __init__(self):
        super().__init__()
        self.items = []
        self.last_time = None

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self.items)

    def data(self, index, role):
        if not index.isValid():
            return None
        item = self.items[index.row()]
        if role == ChatModel.TextRole:
            return item["text"]
        if role == ChatModel.SenderRole:
            return item["sender"]
        if role == ChatModel.KindRole:
            return item["kind"]
        if role == ChatModel.SelfRole:
            return item["self"]
        if role == ChatModel.TimeRole:
            return item["time"]
        if role == ChatModel.PixmapRole:
            return item.get("pixmap")
        if role == ChatModel.FileNameRole:
            return item.get("filename")
        if role == ChatModel.MimeRole:
            return item.get("mime")
        return None

    def add(self, kind: str, sender: str, text: str, is_self: bool):
        now = QtCore.QDateTime.currentDateTime()
        self._maybe_time_separator(now)
        self.beginInsertRows(QtCore.QModelIndex(), len(self.items), len(self.items))
        self.items.append({"kind": kind, "sender": sender, "text": text, "self": is_self, "time": now})
        self.endInsertRows()

    def add_file(self, sender: str, filename: str, mime: str, pixmap: Optional[QtGui.QPixmap], is_self: bool):
        now = QtCore.QDateTime.currentDateTime()
        self._maybe_time_separator(now)
        self.beginInsertRows(QtCore.QModelIndex(), len(self.items), len(self.items))
        self.items.append({"kind": "file", "sender": sender, "text": filename, "self": is_self, "time": now, "pixmap": pixmap, "filename": filename, "mime": mime})
        self.endInsertRows()

    def _maybe_time_separator(self, now: QtCore.QDateTime):
        if self.last_time is None or self.last_time.secsTo(now) >= 300:
            ts = now.toString("HH:mm")
            self.beginInsertRows(QtCore.QModelIndex(), len(self.items), len(self.items))
            self.items.append({"kind": "sys", "sender": "", "text": f"—— {ts} ——", "self": False, "time": now})
            self.endInsertRows()
            self.last_time = now
        else:
            self.last_time = now


class BubbleDelegate(QtWidgets.QStyledItemDelegate):
    def paint(self, painter, option, index):
        kind = index.data(ChatModel.KindRole)
        text = index.data(ChatModel.TextRole)
        sender = index.data(ChatModel.SenderRole)
        is_self = bool(index.data(ChatModel.SelfRole))
        when = index.data(ChatModel.TimeRole)
        painter.save()
        painter.setRenderHint(QtGui.QPainter.Antialiasing, True)
        r = option.rect
        if kind == "sys":
            pen = QtGui.QPen(QtGui.QColor(120, 120, 120))
            painter.setPen(pen)
            fm = option.fontMetrics
            tw = fm.horizontalAdvance(text)
            x = r.x() + (r.width() - tw) // 2
            y = r.y() + fm.ascent() + 8
            painter.drawText(QtCore.QPoint(x, y), text)
            painter.restore()
            return
        if kind == "file":
            pix = index.data(ChatModel.PixmapRole)
            filename = index.data(ChatModel.FileNameRole)
            fm = option.fontMetrics
            pad = 12
            margin = 10
            maxw = int(r.width() * 0.5)
            if pix is not None:
                img_w = min(maxw, pix.width())
                img_h = int(pix.height() * (img_w / pix.width()))
                bubble_w = img_w + pad * 2
                bubble_h = img_h + pad * 2 + fm.height()
            else:
                br = fm.boundingRect(filename)
                bubble_w = br.width() + pad * 2
                bubble_h = br.height() + pad * 2
            if is_self:
                bubble_x = r.right() - bubble_w - margin
                bubble_color = QtGui.QColor(88, 185, 87)
            else:
                bubble_x = r.left() + margin
                bubble_color = QtGui.QColor(235, 235, 235)
            bubble_y = r.top() + 8
            bubble_rect = QtCore.QRect(bubble_x, bubble_y, bubble_w, bubble_h)
            painter.setPen(QtCore.Qt.NoPen)
            painter.setBrush(bubble_color)
            painter.drawRoundedRect(bubble_rect, 12, 12)
            content_rect = bubble_rect.adjusted(pad, pad, -pad, -pad)
            if pix is not None:
                painter.drawPixmap(QtCore.QRect(content_rect.x(), content_rect.y(), img_w, img_h), pix)
                painter.setPen(QtGui.QColor(0, 0, 0))
                painter.drawText(QtCore.QRect(content_rect.x(), content_rect.y() + img_h + 4, img_w, fm.height()), QtCore.Qt.AlignLeft, filename)
            else:
                painter.setPen(QtGui.QColor(0, 0, 0) if not is_self else QtGui.QColor(255, 255, 255))
                painter.drawText(content_rect, QtCore.Qt.TextWordWrap, filename)
            painter.restore()
            return
        maxw = int(r.width() * 0.65)
        fm = option.fontMetrics
        br = fm.boundingRect(0, 0, maxw, 0, QtCore.Qt.TextWordWrap, text)
        pad = 12
        bubble_w = br.width() + pad * 2
        bubble_h = br.height() + pad * 2
        margin = 10
        if is_self:
            bubble_x = r.right() - bubble_w - margin
            bubble_color = QtGui.QColor(88, 185, 87)
            text_color = QtGui.QColor(255, 255, 255)
            align = QtCore.Qt.AlignRight
        else:
            bubble_x = r.left() + margin
            bubble_color = QtGui.QColor(235, 235, 235)
            text_color = QtGui.QColor(0, 0, 0)
            align = QtCore.Qt.AlignLeft
        bubble_y = r.top() + 20
        bubble_rect = QtCore.QRect(bubble_x, bubble_y, bubble_w, bubble_h)
        painter.setPen(QtCore.Qt.NoPen)
        painter.setBrush(bubble_color)
        painter.drawRoundedRect(bubble_rect, 12, 12)
        painter.setPen(text_color)
        text_rect = bubble_rect.adjusted(pad, pad, -pad, -pad)
        painter.drawText(text_rect, QtCore.Qt.TextWordWrap | align, text)
        name_color = QtGui.QColor(120, 120, 120)
        painter.setPen(name_color)
        name_y = bubble_rect.top() - 4
        if not is_self:
            painter.drawText(QtCore.QRect(bubble_x, name_y - fm.height(), bubble_w, fm.height()), QtCore.Qt.AlignLeft, sender)
        time_text = when.toString("HH:mm") if isinstance(when, QtCore.QDateTime) else ""
        painter.setPen(QtGui.QColor(150, 150, 150))
        if is_self:
            painter.drawText(QtCore.QRect(bubble_rect.left(), bubble_rect.bottom() + 4, bubble_w, fm.height()), QtCore.Qt.AlignRight, time_text)
        else:
            painter.drawText(QtCore.QRect(bubble_rect.left(), bubble_rect.bottom() + 4, bubble_w, fm.height()), QtCore.Qt.AlignLeft, time_text)
        avatar_size = 22
        hue = (sum(ord(c) for c in sender) % 360)
        avatar_color = QtGui.QColor.fromHsl(hue, 160, 160)
        painter.setBrush(avatar_color)
        painter.setPen(QtCore.Qt.NoPen)
        if is_self:
            ax = bubble_rect.right() + 8
        else:
            ax = bubble_rect.left() - avatar_size - 8
        ay = bubble_rect.top()
        painter.drawEllipse(QtCore.QRect(ax, ay, avatar_size, avatar_size))
        painter.setPen(QtGui.QColor(255, 255, 255))
        letter = sender[:1] if sender else "?"
        painter.drawText(QtCore.QRect(ax, ay, avatar_size, avatar_size), QtCore.Qt.AlignCenter, letter)
        painter.restore()

    def sizeHint(self, option, index):
        kind = index.data(ChatModel.KindRole)
        text = index.data(ChatModel.TextRole)
        if kind == "sys":
            fm = option.fontMetrics
            h = fm.height() + 16
            return QtCore.QSize(option.rect.width(), h)
        fm = option.fontMetrics
        maxw = int(option.rect.width() * 0.65)
        br = fm.boundingRect(0, 0, maxw, 0, QtCore.Qt.TextWordWrap, text)
        h = br.height() + 28 + fm.height()
        return QtCore.QSize(option.rect.width(), h)


class ChatWindow(QtWidgets.QWidget):
    def __init__(self, host: str, port: int, username: str, log_dir: str, room: str):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.room = room
        self.sock: Optional[socket.socket] = None
        self.rx: Optional[Receiver] = None
        self.logger = ChatLogger(log_dir, f"{host}_{port}")
        self.dm_target: Optional[str] = None
        self.seq = 1
        self.store = LocalStore(log_dir, username)

        self.setWindowTitle(f"群聊 - {username} @ {host}:{port} / {room}")
        self.view = QtWidgets.QListView()
        self.view.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.conv_models = {}
        self.current_model = None
        self.view.setItemDelegate(BubbleDelegate())
        self.conv_list = QtWidgets.QListWidget()
        self.entry = QtWidgets.QLineEdit()
        self.send_btn = QtWidgets.QPushButton("发送")
        self.dm_label = QtWidgets.QLabel("私聊对象：无")
        self.clear_dm_btn = QtWidgets.QPushButton("清除私聊")
        self.send_file_btn = QtWidgets.QPushButton("发送文件")
        layout = QtWidgets.QHBoxLayout()
        left = QtWidgets.QVBoxLayout()
        left.addWidget(QtWidgets.QLabel("会话"))
        left.addWidget(self.conv_list, 1)
        left_container = QtWidgets.QWidget()
        left_container.setLayout(left)
        layout.addWidget(left_container, 2)
        right = QtWidgets.QVBoxLayout()
        right.addWidget(self.view, 5)
        info = QtWidgets.QHBoxLayout()
        info.addWidget(self.dm_label)
        info.addStretch(1)
        info.addWidget(self.clear_dm_btn)
        right.addLayout(info)
        h = QtWidgets.QHBoxLayout()
        h.addWidget(self.entry)
        h.addWidget(self.send_btn)
        h.addWidget(self.send_file_btn)
        right.addLayout(h)
        container = QtWidgets.QWidget()
        container.setLayout(right)
        layout.addWidget(container, 5)
        self.setLayout(layout)

        self.entry.returnPressed.connect(self.on_send)
        self.send_btn.clicked.connect(self.on_send)
        self.clear_dm_btn.clicked.connect(self.on_clear_dm)
        self.conv_list.itemDoubleClicked.connect(self.on_pick_conv)
        self.send_file_btn.clicked.connect(self.on_send_file)
        self._connect()
        self.conv_unread = {}
        self._init_conversations()
        self.conv_list.addItem(f"群聊:{self.room}")
        self._bootstrap_local()

    def _connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        attempts = 0
        while True:
            try:
                s.connect((self.host, self.port))
                break
            except Exception:
                attempts += 1
                if attempts >= 10:
                    raise
                time.sleep(0.5)
        self.sock = s
        hello = f"HELLO {self.username} {self.room}\n".encode("utf-8")
        self.sock.sendall(hello)
        self.rx = Receiver(self.sock)
        self.rx.received.connect(self.on_received)
        self.rx.start()
        self.current_conv = f"group:{self.room}"
        self._ensure_conv(self.current_conv)
        self.current_model = self.conv_models[self.current_conv]
        self.view.setModel(self.current_model)
        self.hb = QtCore.QTimer(self)
        self.hb.setInterval(30000)
        self.hb.timeout.connect(self._send_ping)
        self.hb.start()
        jwt_sec = os.environ.get("JWT_SECRET")
        if jwt_sec:
            try:
                header = {"alg":"HS256","typ":"JWT"}
                payload = {"sub": self.username, "iat": int(QtCore.QDateTime.currentSecsSinceEpoch())}
                h_b64 = base64.urlsafe_b64encode(json.dumps(header, separators=(",",":")).encode("utf-8")).rstrip(b'=')
                p_b64 = base64.urlsafe_b64encode(json.dumps(payload, separators=(",",":")).encode("utf-8")).rstrip(b'=')
                mac = hmac.new(jwt_sec.encode("utf-8"), h_b64 + b'.' + p_b64, hashlib.sha256).digest()
                s_b64 = base64.urlsafe_b64encode(mac).rstrip(b'=')
                token = h_b64.decode("utf-8") + "." + p_b64.decode("utf-8") + "." + s_b64.decode("utf-8")
                self._send_seq(f"AUTH_JWT {token}")
            except Exception:
                pass
        else:
            sec = os.environ.get("CHAT_SECRET")
            if sec:
                try:
                    ts = str(QtCore.QDateTime.currentMSecsSinceEpoch())
                    mac = hmac.new(sec.encode("utf-8"), f"{self.username}:{ts}".encode("utf-8"), hashlib.sha256).hexdigest()
                    self._send_seq(f"AUTH {self.username} {ts} {mac}")
                except Exception:
                    pass
        self._send_seq("UNREAD")

    def on_received(self, text: str):
        self.logger.write("recv", self.host, text)
        if text.startswith("PONG "):
            return
        if text.startswith("[ACK] "):
            try:
                n = int(text.split()[1])
                m = self.conv_models.get(self.current_conv)
                if m:
                    m.add("sys", "", f"已送达 {n}", False)
                    self.view.scrollToBottom()
            except Exception:
                pass
            return
        if text.startswith("[SYS] "):
            parts = text.split()
            if len(parts) >= 4 and parts[1] == "JOIN":
                room = parts[2]
                user = parts[3]
                if room == self.room:
                    m = self.conv_models.get(f"group:{self.room}")
                    if m:
                        m.add("sys", "", f"系统: {user} 加入 {room}", False)
                    self.store.add(f"group:{self.room}", "", f"系统: {user} 加入 {room}", "sys", False)
                    if user != self.username:
                        self._add_conv_dm(user)
                    self.view.scrollToBottom()
                return
            if len(parts) >= 4 and parts[1] == "LEAVE":
                room = parts[2]
                user = parts[3]
                if room == self.room:
                    m = self.conv_models.get(f"group:{self.room}")
                    if m:
                        m.add("sys", "", f"系统: {user} 离开 {room}", False)
                    self.store.add(f"group:{self.room}", "", f"系统: {user} 离开 {room}", "sys", False)
                    if user != self.username:
                        self._remove_conv_dm(user)
                    self.view.scrollToBottom()
                return
            if len(parts) >= 4 and parts[1] == "USERS":
                room = parts[2]
                users_csv = " ".join(parts[3:])
                if room == self.room:
                    users = [x for x in users_csv.split(",") if x]
                    for u in users:
                        found = False
                        for i in range(self.conv_list.count()):
                            if self.conv_list.item(i).text() == f"群聊:{self.room}":
                                found = True
                                break
                        if not found:
                            self.conv_list.addItem(f"群聊:{self.room}")
                        if u != self.username:
                            self._add_conv_dm(u)
                return
            if len(parts) >= 6 and parts[1] == "HISTORY":
                kind = parts[2]
                if kind == "GROUP" and len(parts) >= 6:
                    room = parts[3]
                    sender = parts[4]
                    ts = parts[5]
                    payload = " ".join(parts[6:]) if len(parts) > 6 else ""
                    self._ensure_conv(f"group:{room}")
                    if payload.startswith("[FILE] "):
                        fn, mime, b64 = self._parse_file(payload)
                        pix = self._pix_from_b64(mime, b64)
                        self._save_attachment(fn, b64)
                        self.conv_models[f"group:{room}"].add_file(sender, fn, mime, pix, sender == self.username)
                    else:
                        self.conv_models[f"group:{room}"].add("msg", sender, payload, sender == self.username)
                    return
                if kind == "DM" and len(parts) >= 6:
                    peer = parts[3]
                    sender = parts[4]
                    ts = parts[5]
                    payload = " ".join(parts[6:]) if len(parts) > 6 else ""
                    self._ensure_conv(f"dm:{peer}")
                    if payload.startswith("[FILE] "):
                        fn, mime, b64 = self._parse_file(payload)
                        pix = self._pix_from_b64(mime, b64)
                        self._save_attachment(fn, b64)
                        self.conv_models[f"dm:{peer}"].add_file(sender, fn, mime, pix, sender == self.username)
                    else:
                        self.conv_models[f"dm:{peer}"].add("msg", sender, payload, sender == self.username)
                    return
            if len(parts) >= 4 and parts[1] == "UNREAD":
                conv = parts[2]
                cnt = 0
                try:
                    cnt = int(parts[3])
                except Exception:
                    cnt = 0
                if conv.startswith("group:"):
                    key = conv
                else:
                    x, y = conv[len("dm:"):].split("&", 1)
                    key = f"dm:{y}" if x == self.username else f"dm:{x}"
                    self._add_conv_dm(key.split(":",1)[1])
                self._set_unread(key, cnt)
                return
        if text.startswith("[DM] "):
            parts = text.split(" ", 3)
            if len(parts) >= 4 and parts[1] == "FROM":
                name = parts[2]
                msg = parts[3]
                if msg.startswith("[FILE] "):
                    fn, mime, b64 = self._parse_file(msg)
                    pix = self._pix_from_b64(mime, b64)
                    self._save_attachment(fn, b64)
                    self._ensure_conv(f"dm:{name}")
                    self.conv_models[f"dm:{name}"].add_file(name, fn, mime, pix, False)
                    self.store.add(f"dm:{name}", name, f"[FILE] {fn} {mime}", "file", False)
                else:
                    self._ensure_conv(f"dm:{name}")
                    self.conv_models[f"dm:{name}"].add("msg", name, msg, False)
                    self.store.add(f"dm:{name}", name, msg, "msg", False)
                self.view.scrollToBottom()
                self._add_conv_dm(name)
                if self.current_conv != f"dm:{name}":
                    self._inc_unread(f"dm:{name}")
                return
            if len(parts) >= 4 and parts[1] == "TO":
                target = parts[2]
                msg = parts[3]
                if target != self.username:
                    return
                if msg.startswith("[FILE] "):
                    fn, mime, b64 = self._parse_file(msg)
                    pix = self._pix_from_b64(mime, b64)
                    self._save_attachment(fn, b64)
                    self._ensure_conv(f"dm:{target}")
                    self.conv_models[f"dm:{target}"].add_file(self.username, fn, mime, pix, True)
                    self.store.add(f"dm:{target}", self.username, f"[FILE] {fn} {mime}", "file", True)
                else:
                    self._ensure_conv(f"dm:{target}")
                    self.conv_models[f"dm:{target}"].add("msg", self.username, msg, True)
                    self.store.add(f"dm:{target}", self.username, msg, "msg", True)
                self.view.scrollToBottom()
                self._add_conv_dm(target)
                return
        if ">" in text:
            name, msg = text.split(">", 1)
            name = name.strip()
            msg = msg.strip()
            if msg.startswith("[FILE] "):
                fn, mime, b64 = self._parse_file(msg)
                pix = self._pix_from_b64(mime, b64)
                self._save_attachment(fn, b64)
                self._ensure_conv(f"group:{self.room}")
                self.conv_models[f"group:{self.room}"].add_file(name, fn, mime, pix, name == self.username)
                self.store.add(f"group:{self.room}", name, f"[FILE] {fn} {mime}", "file", name == self.username)
            else:
                self._ensure_conv(f"group:{self.room}")
                self.conv_models[f"group:{self.room}"].add("msg", name, msg, name == self.username)
                self.store.add(f"group:{self.room}", name, msg, "msg", name == self.username)
            self.view.scrollToBottom()
            if self.current_conv != f"group:{self.room}":
                self._inc_unread(f"group:{self.room}")

    def on_send(self):
        text = self.entry.text().strip()
        if not text:
            return
        try:
            if self.current_conv.startswith("dm:"):
                target = self.current_conv.split(":",1)[1]
                self._send_seq(f"DM {target} {text}")
                self.store.add(f"dm:{target}", self.username, text, "msg", True)
            else:
                self._send_seq(f"MSG {text}")
                self.store.add(f"group:{self.room}", self.username, text, "msg", True)
            self.logger.write("sent", self.username, text)
            self.entry.clear()
            if self.current_conv.startswith("group:"):
                self._ensure_conv(self.current_conv)
                self.conv_models[self.current_conv].add("msg", self.username, text, True)
                self.view.scrollToBottom()
        except Exception:
            pass

    def on_clear_dm(self):
        self.switch_conv(f"group:{self.room}")

    def on_pick_conv(self, item: QtWidgets.QListWidgetItem):
        text = item.text()
        if text.startswith("群聊:"):
            self.switch_conv(f"group:{self.room}")
        else:
            name = text.split(" (",1)[0]
            self.switch_conv(f"dm:{name}")

    def _add_conv_dm(self, name: str):
        exists = False
        for i in range(self.conv_list.count()):
            if self.conv_list.item(i).text() == name:
                exists = True
                break
        if not exists:
            self.conv_list.addItem(name)
        self._ensure_unread_key(f"dm:{name}")
        self._ensure_conv(f"dm:{name}")

    def _remove_conv_dm(self, name: str):
        for i in range(self.conv_list.count()):
            if self.conv_list.item(i).text() == name:
                self.conv_list.takeItem(i)
                break
        key = f"dm:{name}"
        if key in self.conv_unread:
            del self.conv_unread[key]
        if key in self.conv_models:
            del self.conv_models[key]

    def on_send_file(self):
        dlg = QtWidgets.QFileDialog(self)
        dlg.setFileMode(QtWidgets.QFileDialog.ExistingFile)
        if dlg.exec():
            files = dlg.selectedFiles()
            if files:
                path = files[0]
                with open(path, "rb") as f:
                    data = f.read()
                b64 = base64.b64encode(data).decode("ascii")
                name = os.path.basename(path)
                mime = self._guess_mime(path)
                payload_text = f"[FILE] {name} {mime} {b64}"
                try:
                    if self.current_conv.startswith("dm:"):
                        target = self.current_conv.split(":",1)[1]
                        self._send_seq(f"DM {target} {payload_text}")
                        self.store.add(f"dm:{target}", self.username, payload_text, "file", True)
                    else:
                        self._send_seq(f"MSG {payload_text}")
                        self.store.add(f"group:{self.room}", self.username, payload_text, "file", True)
                    self.logger.write("sent", self.username, payload_text)
                    pix = self._pix_from_b64(mime, b64)
                    self._ensure_conv(self.current_conv)
                    self.conv_models[self.current_conv].add_file(self.username, name, mime, pix, True)
                    self.view.scrollToBottom()
                except Exception:
                    pass

    def _init_conversations(self):
        self._ensure_unread_key(f"group:{self.room}")

    def _ensure_unread_key(self, key: str):
        if key not in self.conv_unread:
            self.conv_unread[key] = 0

    def _inc_unread(self, key: str):
        self._ensure_unread_key(key)
        self.conv_unread[key] += 1
        self._update_conv_title(key)

    def _reset_unread(self, key: str):
        self.conv_unread[key] = 0
        self._update_conv_title(key)

    def _update_conv_title(self, key: str):
        title = f"群聊:{self.room}" if key.startswith("group:") else key.split(":",1)[1]
        count = self.conv_unread.get(key, 0)
        text = f"{title} ({count})" if count > 0 else title
        for i in range(self.conv_list.count()):
            item = self.conv_list.item(i)
            base = item.text().split(" (",1)[0]
            if base == title:
                item.setText(text)
                break
    def _set_unread(self, key: str, cnt: int):
        self.conv_unread[key] = max(0, cnt)
        self._update_conv_title(key)

    def _ensure_conv(self, key: str):
        if key not in self.conv_models:
            self.conv_models[key] = ChatModel()

    def switch_conv(self, key: str):
        self._ensure_conv(key)
        self.current_conv = key
        if key.startswith("group:"):
            self.dm_target = None
            self.dm_label.setText("私聊对象：无")
        else:
            name = key.split(":",1)[1]
            self.dm_target = name
            self.dm_label.setText(f"私聊对象：{self.dm_target}")
        self.current_model = self.conv_models[key]
        self.view.setModel(self.current_model)
        self._reset_unread(key)
        if key.startswith("group:"):
            self._send_seq("READ GROUP")
        else:
            peer = key.split(":",1)[1]
            self._send_seq(f"READ DM {peer}")
        if len(self.current_model.items) == 0:
            conv = key
            for sender, ts, kind, text, selfflag in self.store.recent(conv, 100):
                if kind == "file" and text.startswith("[FILE] "):
                    fn, mime, _ = self._parse_file(text)
                    pix = None
                    self.current_model.add_file(sender, fn, mime, pix, bool(selfflag))
                elif kind == "sys":
                    self.current_model.add("sys", "", text, False)
                else:
                    self.current_model.add("msg", sender, text, bool(selfflag))
            if len(self.current_model.items) == 0:
                if key.startswith("group:"):
                    self._send_seq("HIST GROUP 50")
                else:
                    peer = key.split(":",1)[1]
                    self._send_seq(f"HIST DM {peer} 50")

    def _bootstrap_local(self):
        peers = self.store.peers()
        for p in peers:
            self._add_conv_dm(p)
        # preload group conv
        self._ensure_conv(f"group:{self.room}")
        for sender, ts, kind, text, selfflag in self.store.recent(f"group:{self.room}", 100):
            if kind == "file" and text.startswith("[FILE] "):
                fn, mime, _ = self._parse_file(text)
                pix = None
                self.conv_models[f"group:{self.room}"].add_file(sender, fn, mime, pix, bool(selfflag))
            elif kind == "sys":
                self.conv_models[f"group:{self.room}"].add("sys", "", text, False)
            else:
                self.conv_models[f"group:{self.room}"].add("msg", sender, text, bool(selfflag))
        if len(self.current_model.items) == 0:
            if self.current_conv.startswith("group:"):
                self._send_seq("HIST GROUP 50")
            else:
                peer = self.current_conv.split(":",1)[1]
                self._send_seq(f"HIST DM {peer} 50")

    def _send_seq(self, body: str):
        try:
            payload = f"SEQ {self.seq} {body}\n".encode("utf-8")
            self.sock.sendall(payload)
            self.seq += 1
        except Exception:
            pass

    def _send_ping(self):
        try:
            ts = str(QtCore.QDateTime.currentMSecsSinceEpoch())
            self.sock.sendall((f"PING {ts}\n").encode("utf-8"))
        except Exception:
            pass

    def _guess_mime(self, path: str) -> str:
        ext = os.path.splitext(path)[1].lower()
        if ext in [".png"]:
            return "image/png"
        if ext in [".jpg", ".jpeg"]:
            return "image/jpeg"
        if ext in [".gif"]:
            return "image/gif"
        if ext in [".txt"]:
            return "text/plain"
        return "application/octet-stream"

    def _pix_from_b64(self, mime: str, b64: str) -> Optional[QtGui.QPixmap]:
        if mime.startswith("image/"):
            try:
                data = base64.b64decode(b64)
                img = QtGui.QImage()
                img.loadFromData(data)
                return QtGui.QPixmap.fromImage(img)
            except Exception:
                return None
        return None

    def _parse_file(self, msg: str):
        parts = msg.split(" ", 3)
        name = parts[1] if len(parts) > 1 else "file"
        mime = parts[2] if len(parts) > 2 else "application/octet-stream"
        b64 = parts[3] if len(parts) > 3 else ""
        return name, mime, b64

    def _save_attachment(self, filename: str, b64: str):
        att_dir = os.path.join(os.getcwd(), "chat_logs", "attachments")
        os.makedirs(att_dir, exist_ok=True)
        try:
            data = base64.b64decode(b64)
            with open(os.path.join(att_dir, filename), "wb") as f:
                f.write(data)
        except Exception:
            pass

    def closeEvent(self, e):
        try:
            if self.rx:
                self.rx.stop()
            if self.sock:
                self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        super().closeEvent(e)


def parse_args():
    p = argparse.ArgumentParser(prog="qt_chat_client", add_help=True)
    p.add_argument("--host", type=str, required=True)
    p.add_argument("--port", type=int, default=5001)
    p.add_argument("--username", type=str, default=getpass.getuser())
    p.add_argument("--log-dir", type=str, default=os.path.join(os.getcwd(), "chat_logs"))
    p.add_argument("--room", type=str, default="general")
    return p.parse_args()


def main():
    args = parse_args()
    app = QtWidgets.QApplication([])
    win = ChatWindow(args.host, args.port, args.username, args.log_dir, args.room)
    win.resize(700, 500)
    win.show()
    app.exec()


if __name__ == "__main__":
    main()
