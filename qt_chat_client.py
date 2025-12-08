import argparse
import socket
from typing import Optional
import os
import getpass

from PySide6 import QtCore, QtWidgets, QtGui
from intranet_chat import ChatLogger


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

    def __init__(self):
        super().__init__()
        self.items = []

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
        return None

    def add(self, kind: str, sender: str, text: str, is_self: bool):
        self.beginInsertRows(QtCore.QModelIndex(), len(self.items), len(self.items))
        self.items.append({"kind": kind, "sender": sender, "text": text, "self": is_self, "time": QtCore.QDateTime.currentDateTime()})
        self.endInsertRows()


class BubbleDelegate(QtWidgets.QStyledItemDelegate):
    def paint(self, painter, option, index):
        kind = index.data(ChatModel.KindRole)
        text = index.data(ChatModel.TextRole)
        sender = index.data(ChatModel.SenderRole)
        is_self = bool(index.data(ChatModel.SelfRole))
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
        bubble_y = r.top() + 8
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

        self.setWindowTitle(f"群聊 - {username} @ {host}:{port} / {room}")
        self.view = QtWidgets.QListView()
        self.view.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.model = ChatModel()
        self.view.setModel(self.model)
        self.view.setItemDelegate(BubbleDelegate())
        self.user_list = QtWidgets.QListWidget()
        self.entry = QtWidgets.QLineEdit()
        self.send_btn = QtWidgets.QPushButton("发送")
        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.user_list, 1)
        right = QtWidgets.QVBoxLayout()
        right.addWidget(self.view, 5)
        h = QtWidgets.QHBoxLayout()
        h.addWidget(self.entry)
        h.addWidget(self.send_btn)
        right.addLayout(h)
        container = QtWidgets.QWidget()
        container.setLayout(right)
        layout.addWidget(container, 4)
        self.setLayout(layout)

        self.entry.returnPressed.connect(self.on_send)
        self.send_btn.clicked.connect(self.on_send)
        self._connect()

    def _connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        self.sock = s
        hello = f"HELLO {self.username} {self.room}\n".encode("utf-8")
        self.sock.sendall(hello)
        self.rx = Receiver(self.sock)
        self.rx.received.connect(self.on_received)
        self.rx.start()

    def on_received(self, text: str):
        self.logger.write("recv", self.host, text)
        if text.startswith("[SYS] "):
            parts = text.split()
            if len(parts) >= 4 and parts[1] == "JOIN":
                room = parts[2]
                user = parts[3]
                if room == self.room:
                    names = [self.user_list.item(i).text() for i in range(self.user_list.count())]
                    if user not in names:
                        self.user_list.addItem(user)
                    self.model.add("sys", "", f"系统: {user} 加入 {room}", False)
                    self.view.scrollToBottom()
                return
            if len(parts) >= 4 and parts[1] == "LEAVE":
                room = parts[2]
                user = parts[3]
                if room == self.room:
                    for i in range(self.user_list.count()):
                        if self.user_list.item(i).text() == user:
                            self.user_list.takeItem(i)
                            break
                    self.model.add("sys", "", f"系统: {user} 离开 {room}", False)
                    self.view.scrollToBottom()
                return
            if len(parts) >= 4 and parts[1] == "USERS":
                room = parts[2]
                users_csv = " ".join(parts[3:])
                if room == self.room:
                    users = [x for x in users_csv.split(",") if x]
                    self.user_list.clear()
                    for u in users:
                        self.user_list.addItem(u)
                return
        if ">" in text:
            name, msg = text.split(">", 1)
            name = name.strip()
            msg = msg.strip()
            self.model.add("msg", name, msg, name == self.username)
            self.view.scrollToBottom()

    def on_send(self):
        text = self.entry.text().strip()
        if not text:
            return
        try:
            self.sock.sendall((text + "\n").encode("utf-8"))
            self.logger.write("sent", self.username, text)
            self.entry.clear()
            self.model.add("msg", self.username, text, True)
            self.view.scrollToBottom()
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
