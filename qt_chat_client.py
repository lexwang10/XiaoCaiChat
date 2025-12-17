import argparse
import socket
import subprocess
from typing import Optional
import os
import sys
import shutil
import getpass
import base64
import json
import hmac
import hashlib
import time
import re
import math
import urllib.request
import urllib.error

from PySide6 import QtCore, QtWidgets, QtGui
APP_VERSION = "1.0.0"
import threading
try:
    import Cocoa
except ImportError:
    Cocoa = None
from chat_utils import ChatLogger
from chat_local_store import LocalStore


class Receiver(QtCore.QObject):
    received = QtCore.Signal(str)

    def __init__(self, sock: socket.socket):
        super().__init__()
        self.sock = sock
        self.running = False
        self.f = None
        self._thread: Optional[threading.Thread] = None

    @QtCore.Slot(str)
    def _emit_line(self, s: str):
        self.received.emit(s)

    def _loop(self):
        try:
            self.f = self.sock.makefile("r", encoding="utf-8", newline="\n")
        except Exception:
            self.f = None
        try:
            while self.running:
                if not self.f:
                    break
                line = self.f.readline()
                if not line:
                    break
                t = line.rstrip("\n")
                if t:
                    try:
                        QtCore.QMetaObject.invokeMethod(self, "_emit_line", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, t))
                    except Exception:
                        pass
            try:
                QtCore.QMetaObject.invokeMethod(self, "_emit_line", QtCore.Qt.QueuedConnection, QtCore.Q_ARG(str, "[SYS] DISCONNECT"))
            except Exception:
                pass
        except Exception:
            pass

    def start(self):
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def wait(self, msecs: int = 0):
        try:
            if self._thread:
                self._thread.join(timeout=(msecs/1000.0 if msecs and msecs > 0 else None))
        except Exception:
            pass

    def stop(self):
        self.running = False
        try:
            if self.f:
                try:
                    self.f.close()
                except Exception:
                    pass
            if self.sock:
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    self.sock.close()
                except Exception:
                    pass
        except Exception:
            pass


class ChatModel(QtCore.QAbstractListModel):
    TextRole = QtCore.Qt.UserRole + 1
    SenderRole = QtCore.Qt.UserRole + 2
    KindRole = QtCore.Qt.UserRole + 3
    SelfRole = QtCore.Qt.UserRole + 4
    TimeRole = QtCore.Qt.UserRole + 5
    PixmapRole = QtCore.Qt.UserRole + 6
    FileNameRole = QtCore.Qt.UserRole + 7
    MimeRole = QtCore.Qt.UserRole + 8
    AvatarRole = QtCore.Qt.UserRole + 9
    FileSizeRole = QtCore.Qt.UserRole + 10
    UploadSentRole = QtCore.Qt.UserRole + 11
    UploadStateRole = QtCore.Qt.UserRole + 12
    UploadAlphaRole = QtCore.Qt.UserRole + 13
    LinkUrlRole = QtCore.Qt.UserRole + 14

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
        if role == ChatModel.AvatarRole:
            return item.get("avatar")
        if role == ChatModel.FileSizeRole:
            return item.get("filesize")
        if role == ChatModel.UploadSentRole:
            return item.get("upload_sent")
        if role == ChatModel.UploadStateRole:
            return item.get("upload_state")
        if role == ChatModel.UploadAlphaRole:
            return item.get("upload_alpha")
        if role == ChatModel.LinkUrlRole:
            return item.get("link_url")
        return None

    def add(self, kind: str, sender: str, text: str, is_self: bool, avatar: Optional[QtGui.QPixmap] = None, ts: Optional[int] = None):
        now = QtCore.QDateTime.fromSecsSinceEpoch(int(ts)) if ts is not None else QtCore.QDateTime.currentDateTime()
        self._maybe_time_separator(now)
        self.beginInsertRows(QtCore.QModelIndex(), len(self.items), len(self.items))
        self.items.append({"kind": kind, "sender": sender, "text": text, "self": is_self, "time": now, "avatar": avatar})
        self.endInsertRows()

    def add_file(self, sender: str, filename: str, mime: str, pixmap: Optional[QtGui.QPixmap], is_self: bool, avatar: Optional[QtGui.QPixmap] = None, ts: Optional[int] = None, size_bytes: Optional[int] = None):
        now = QtCore.QDateTime.fromSecsSinceEpoch(int(ts)) if ts is not None else QtCore.QDateTime.currentDateTime()
        self._maybe_time_separator(now)
        self.beginInsertRows(QtCore.QModelIndex(), len(self.items), len(self.items))
        self.items.append({"kind": "file", "sender": sender, "text": filename, "self": is_self, "time": now, "pixmap": pixmap, "filename": filename, "mime": mime, "avatar": avatar, "filesize": (int(size_bytes) if size_bytes is not None else None), "upload_sent": None, "upload_state": None, "upload_alpha": None, "link_url": None})
        self.endInsertRows()
    def add_link(self, sender: str, filename: str, url: str, is_self: bool, avatar: Optional[QtGui.QPixmap] = None, ts: Optional[int] = None, size_bytes: Optional[int] = None):
        now = QtCore.QDateTime.fromSecsSinceEpoch(int(ts)) if ts is not None else QtCore.QDateTime.currentDateTime()
        self._maybe_time_separator(now)
        self.beginInsertRows(QtCore.QModelIndex(), len(self.items), len(self.items))
        self.items.append({"kind": "file", "sender": sender, "text": filename, "self": is_self, "time": now, "pixmap": None, "filename": filename, "mime": "application/x-download", "avatar": avatar, "filesize": (int(size_bytes) if size_bytes is not None else None), "upload_sent": None, "upload_state": None, "upload_alpha": None, "link_url": url})
        self.endInsertRows()
    def set_upload_progress(self, row: int, sent: Optional[int] = None, total: Optional[int] = None, state: Optional[str] = None):
        if 0 <= row < len(self.items):
            it = self.items[row]
            if sent is not None:
                it["upload_sent"] = int(max(0, sent))
            if total is not None:
                it["filesize"] = int(max(0, total))
            if state is not None:
                it["upload_state"] = state
            top = self.index(row)
            bottom = self.index(row)
            self.dataChanged.emit(top, bottom)
    def set_upload_alpha(self, row: int, alpha: Optional[int]):
        if 0 <= row < len(self.items):
            it = self.items[row]
            it["upload_alpha"] = (int(alpha) if alpha is not None else None)
            top = self.index(row)
            bottom = self.index(row)
            self.dataChanged.emit(top, bottom)

    def _maybe_time_separator(self, now: QtCore.QDateTime):
        should = False
        if self.last_time is None:
            should = True
        else:
            delta = abs(self.last_time.secsTo(now))
            # 3 minutes = 180 seconds
            if delta > 180 or self.last_time.date().daysTo(now.date()) != 0:
                should = True
        
        if should:
            cur_day = now.toString("yyyy-MM-dd")
            today_day = QtCore.QDate.currentDate().toString("yyyy-MM-dd")
            yesterday_day = QtCore.QDate.currentDate().addDays(-1).toString("yyyy-MM-dd")
            hhmm = now.toString("HH:mm")
            if cur_day == today_day:
                label = hhmm
            elif cur_day == yesterday_day:
                label = f"昨天 {hhmm}"
            else:
                label = f"{cur_day} {hhmm}"
            self.beginInsertRows(QtCore.QModelIndex(), len(self.items), len(self.items))
            self.items.append({"kind": "sys", "sender": "", "text": f"—— {label} ——", "self": False, "time": now})
            self.endInsertRows()
        self.last_time = now

    def clear(self):
        self.beginResetModel()
        self.items = []
        self.last_time = None
        self.endResetModel()

    def remove_row(self, row: int):
        if 0 <= row < len(self.items):
            self.beginRemoveRows(QtCore.QModelIndex(), row, row)
            del self.items[row]
            self.endRemoveRows()

    def set_sender_avatar(self, name: str, pixmap: QtGui.QPixmap):
        changed_start = None
        changed_end = None
        for i, it in enumerate(self.items):
            if it.get("sender") == name and it.get("kind") in ("msg", "file"):
                it["avatar"] = pixmap
                if changed_start is None:
                    changed_start = i
                changed_end = i
        if changed_start is not None:
            top = self.index(changed_start)
            bottom = self.index(changed_end)
            self.dataChanged.emit(top, bottom)


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
        try:
            hl_row = int(option.widget.property("search_highlight_row")) if option.widget and option.widget.property("search_highlight_row") is not None else -1
        except Exception:
            hl_row = -1
        is_hl = (index.row() == hl_row)
        try:
            kw = option.widget.property("search_keyword") or ""
        except Exception:
            kw = ""
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
            mime = (index.data(ChatModel.MimeRole) or "").lower()
            fm = option.fontMetrics
            margin = 10
            avatar_size = 22
            avatar_pad = 8
            maxw = int(r.width() * 0.5)
            if mime.startswith("image/") and isinstance(pix, QtGui.QPixmap):
                img_w = min(maxw, pix.width())
                img_h = int(pix.height() * (img_w / max(1, pix.width())))
                if is_self:
                    x = r.right() - img_w - margin - avatar_size - avatar_pad
                else:
                    x = r.left() + margin + avatar_size + avatar_pad
                y = r.top() + 8
                painter.save()
                path = QtGui.QPainterPath()
                path.addRoundedRect(QtCore.QRectF(x, y, img_w, img_h), 8, 8)
                painter.setClipPath(path)
                painter.drawPixmap(QtCore.QRect(x, y, img_w, img_h), pix)
                painter.restore()
                try:
                    sent = index.data(ChatModel.UploadSentRole) or 0
                    total = index.data(ChatModel.FileSizeRole) or 0
                    state = index.data(ChatModel.UploadStateRole) or ""
                    alpha = index.data(ChatModel.UploadAlphaRole) or 0
                    if total > 0 and is_self and (state in ("uploading", "paused", "fading") or alpha > 0):
                        pct = int(round((sent / total * 100))) if total > 0 else 0
                        pie_sz = 28
                        gap = 6
                        px = x - pie_sz - gap
                        py = y + (img_h - pie_sz) // 2
                        rect = QtCore.QRect(px, py, pie_sz, pie_sz)
                        if alpha and alpha > 0:
                            try:
                                painter.save()
                                painter.setOpacity(float(alpha) / 255.0)
                            except Exception:
                                pass
                        painter.setPen(QtGui.QPen(QtGui.QColor(200, 200, 200)))
                        painter.setBrush(QtGui.QColor(240, 240, 240))
                        painter.drawEllipse(rect)
                        painter.setBrush(QtGui.QColor(88, 185, 87))
                        painter.setPen(QtCore.Qt.NoPen)
                        span = int((sent / total) * 360 * 16) if total > 0 else 0
                        painter.drawPie(rect, 90 * 16, -span)
                        painter.setPen(QtGui.QColor(51, 51, 51))
                        fnt = QtGui.QFont(option.font)
                        try:
                            fnt.setPointSize(9)
                        except Exception:
                            pass
                        painter.setFont(fnt)
                        painter.drawText(rect, QtCore.Qt.AlignCenter, f"{pct}%")
                        if alpha and alpha > 0:
                            try:
                                painter.restore()
                            except Exception:
                                pass
                except Exception:
                    pass
                # draw avatar
                avatar = index.data(ChatModel.AvatarRole)
                ax = (r.right() - margin - avatar_size) if is_self else (r.left() + margin)
                ay = y
                if isinstance(avatar, QtGui.QPixmap):
                    painter.drawPixmap(QtCore.QRect(ax, ay, avatar_size, avatar_size), avatar.scaled(avatar_size, avatar_size, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))
                else:
                    hue = (sum(ord(c) for c in sender) % 360)
                    avatar_color = QtGui.QColor.fromHsl(hue, 160, 160)
                    painter.setBrush(avatar_color)
                    painter.setPen(QtCore.Qt.NoPen)
                    painter.drawEllipse(QtCore.QRect(ax, ay, avatar_size, avatar_size))
                    painter.setPen(QtGui.QColor(255, 255, 255))
                    letter = sender[:1] if sender else "?"
                    painter.drawText(QtCore.QRect(ax, ay, avatar_size, avatar_size), QtCore.Qt.AlignCenter, letter)
                painter.restore()
                return
            # non-image files: render transparent chip (name + size + icon)
            fm = option.fontMetrics
            pad = 12
            chip_w = min(maxw, 240)
            chip_h = 56
            chip_x = (r.right() - chip_w - margin - avatar_size - avatar_pad) if is_self else (r.left() + margin + avatar_size + avatar_pad)
            chip_y = r.top() + 8
            chip_rect = QtCore.QRect(chip_x, chip_y, chip_w, chip_h)
            painter.setPen(QtCore.Qt.NoPen)
            bg = QtGui.QColor(0, 0, 0, int(255 * 0.06))
            painter.setBrush(bg)
            painter.drawRoundedRect(chip_rect, 10, 10)
            # text block
            name_rect = QtCore.QRect(chip_rect.left() + pad, chip_rect.top() + pad - 2, chip_rect.width() - 22 - pad*2, fm.height())
            size_rect = QtCore.QRect(chip_rect.left() + pad, name_rect.bottom() + 4, chip_rect.width() - 22 - pad*2, fm.height())
            try:
                import html as _html
                fname = filename or ""
                name_doc = QtGui.QTextDocument()
                name_doc.setDefaultFont(option.font)
                name_doc.setDocumentMargin(0)
                name_doc.setTextWidth(name_rect.width())
                if kw:
                    pat2 = re.compile(re.escape(str(kw)), re.IGNORECASE)
                    parts2 = []
                    last2 = 0
                    for m2 in pat2.finditer(fname):
                        parts2.append(_html.escape(fname[last2:m2.start()]))
                        parts2.append("<span style='background:#fff59d'>" + _html.escape(m2.group(0)) + "</span>")
                        last2 = m2.end()
                    parts2.append(_html.escape(fname[last2:]))
                    name_doc.setHtml("".join(parts2))
                else:
                    name_doc.setHtml(_html.escape(fname))
                painter.save()
                painter.translate(name_rect.topLeft())
                name_doc.drawContents(painter, QtCore.QRectF(0, 0, name_rect.width(), name_rect.height()))
                painter.restore()
            except Exception:
                painter.setPen(QtGui.QColor(51, 51, 51))
                painter.setFont(option.font)
                painter.drawText(name_rect, QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter, filename)
            # size
            filesize = index.data(ChatModel.FileSizeRole) or 0
            def _hs(n):
                units = ["B","KB","MB","GB","TB"]
                i = 0
                f = float(n)
                while f >= 1024.0 and i < len(units)-1:
                    f /= 1024.0; i += 1
                return ("{:.1f}{}".format(f, units[i]) if i>0 else "{}{}".format(int(f), units[i]))
            painter.setPen(QtGui.QColor(119, 119, 119))
            painter.drawText(size_rect, QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter, _hs(int(filesize)))
            try:
                sent = index.data(ChatModel.UploadSentRole) or 0
                total = index.data(ChatModel.FileSizeRole) or 0
                state = index.data(ChatModel.UploadStateRole) or ""
                alpha = index.data(ChatModel.UploadAlphaRole) or 0
                if total > 0 and is_self and (state in ("uploading", "paused", "fading") or alpha > 0):
                    pct = int(round((sent / total * 100))) if total > 0 else 0
                    pie_sz = 28
                    gap = 6
                    px = chip_rect.left() - pie_sz - gap
                    py = chip_rect.top() + (chip_h - pie_sz) // 2
                    rect = QtCore.QRect(px, py, pie_sz, pie_sz)
                    if alpha and alpha > 0:
                        try:
                            painter.save()
                            painter.setOpacity(float(alpha) / 255.0)
                        except Exception:
                            pass
                    painter.setPen(QtGui.QPen(QtGui.QColor(200, 200, 200)))
                    painter.setBrush(QtGui.QColor(240, 240, 240))
                    painter.drawEllipse(rect)
                    painter.setBrush(QtGui.QColor(88, 185, 87))
                    painter.setPen(QtCore.Qt.NoPen)
                    span = int((sent / total) * 360 * 16) if total > 0 else 0
                    painter.drawPie(rect, 90 * 16, -span)
                    painter.setPen(QtGui.QColor(51, 51, 51))
                    fnt = QtGui.QFont(option.font)
                    try:
                        fnt.setPointSize(9)
                    except Exception:
                        pass
                    painter.setFont(fnt)
                    painter.drawText(rect, QtCore.Qt.AlignCenter, f"{pct}%")
                    if alpha and alpha > 0:
                        try:
                            painter.restore()
                        except Exception:
                            pass
            except Exception:
                pass
            # icon
            try:
                icon_path = os.path.join(os.getcwd(), "icons", "ui", "document.png")
                doc_pm = QtGui.QPixmap(icon_path)
                if not doc_pm.isNull():
                    doc_pm = doc_pm.scaled(22, 22, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
                    icon_rect = QtCore.QRect(chip_rect.right() - pad - 22, chip_rect.top() + (chip_h-22)//2, 22, 22)
                    painter.drawPixmap(icon_rect, doc_pm)
            except Exception:
                pass
            # avatar
            avatar = index.data(ChatModel.AvatarRole)
            ax = (r.right() - margin - avatar_size) if is_self else (r.left() + margin)
            ay = chip_rect.top()
            if isinstance(avatar, QtGui.QPixmap):
                painter.drawPixmap(QtCore.QRect(ax, ay, avatar_size, avatar_size), avatar.scaled(avatar_size, avatar_size, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))
            else:
                hue = (sum(ord(c) for c in sender) % 360)
                avatar_color = QtGui.QColor.fromHsl(hue, 160, 160)
                painter.setBrush(avatar_color)
                painter.setPen(QtCore.Qt.NoPen)
                painter.drawEllipse(QtCore.QRect(ax, ay, avatar_size, avatar_size))
                painter.setPen(QtGui.QColor(255, 255, 255))
                letter = sender[:1] if sender else "?"
                painter.drawText(QtCore.QRect(ax, ay, avatar_size, avatar_size), QtCore.Qt.AlignCenter, letter)
            painter.restore()
            return
        maxw = int(r.width() * 0.65)
        fdoc = QtGui.QFont(option.font)
        try:
            base_ps = int(fdoc.pointSize()) if fdoc.pointSize() > 0 else 14
            fdoc.setPointSize(base_ps + 2)
        except Exception:
            pass
        fm = QtGui.QFontMetrics(fdoc)
        doc = QtGui.QTextDocument()
        opt = QtGui.QTextOption()
        opt.setWrapMode(QtGui.QTextOption.WrapAtWordBoundaryOrAnywhere)
        doc.setDefaultFont(fdoc)
        doc.setDefaultTextOption(opt)
        doc.setDocumentMargin(0)
        w0 = fm.horizontalAdvance(text)
        text_w = min(w0 + 6, maxw)
        doc.setTextWidth(text_w)
        try:
            import html as _html
            s = text or ""
            base_sz = option.font.pointSize()
            if base_sz is None or base_sz <= 0:
                base_sz = 14
            big_sz = int(base_sz) + 8
            def _emoji_html(seg: str) -> str:
                try:
                    pat_emoji = re.compile(r'([\u2600-\u27BF\U0001F300-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FAFF](?:\uFE0F)?(?:\u200D[\u2600-\u27BF\U0001F300-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FAFF](?:\uFE0F)?)*)')
                    out = []
                    pos = 0
                    for m in pat_emoji.finditer(seg):
                        out.append(_html.escape(seg[pos:m.start()]))
                        out.append("<span style='font-size:{}pt;line-height:1.0em'>".format(big_sz) + _html.escape(m.group(0)) + "</span>")
                        pos = m.end()
                    out.append(_html.escape(seg[pos:]))
                    return "".join(out)
                except Exception:
                    return _html.escape(seg)
            if kw:
                pat = re.compile(re.escape(str(kw)), re.IGNORECASE)
                parts = []
                last = 0
                for m in pat.finditer(s):
                    parts.append(_emoji_html(s[last:m.start()]))
                    parts.append("<span style='background:#fff59d'>" + _html.escape(m.group(0)) + "</span>")
                    last = m.end()
                parts.append(_emoji_html(s[last:]))
                doc.setHtml("".join(parts))
            else:
                doc.setHtml(_emoji_html(s))
        except Exception:
            doc.setHtml(text or "")
        pad = 12
        bubble_w = int(text_w) + pad * 2
        bubble_h = int(doc.size().height()) + pad * 2
        margin = 10
        avatar_size = 22
        avatar_pad = 8
        if is_self:
            bubble_x = r.right() - bubble_w - margin - avatar_size - avatar_pad
            bubble_color = QtGui.QColor(88, 185, 87)
            text_color = QtGui.QColor(255, 255, 255)
            align = QtCore.Qt.AlignRight
        else:
            bubble_x = r.left() + margin + avatar_size + avatar_pad
            bubble_color = QtGui.QColor(235, 235, 235)
            text_color = QtGui.QColor(0, 0, 0)
            align = QtCore.Qt.AlignLeft
        bubble_y = r.top() + 26
        bubble_rect = QtCore.QRect(bubble_x, bubble_y, bubble_w, bubble_h)
        painter.setPen(QtCore.Qt.NoPen)
        painter.setBrush(bubble_color)
        painter.drawRoundedRect(bubble_rect, 12, 12)
        painter.setPen(text_color)
        text_rect = bubble_rect.adjusted(pad, pad, -pad, -pad)
        painter.save()
        painter.translate(text_rect.topLeft())
        doc.drawContents(painter, QtCore.QRectF(0, 0, text_rect.width(), text_rect.height()))
        painter.restore()
        name_color = QtGui.QColor(120, 120, 120)
        painter.setPen(name_color)
        name_y = bubble_rect.top() - 4
        if not is_self:
            painter.drawText(QtCore.QRect(bubble_x, name_y - fm.height(), bubble_w, fm.height()), QtCore.Qt.AlignLeft, sender)
        # no per-bubble time; time separators handled by system rows
        avatar = index.data(ChatModel.AvatarRole)
        if is_self:
            ax = r.right() - margin - avatar_size
        else:
            ax = r.left() + margin
        ay = bubble_rect.top()
        if isinstance(avatar, QtGui.QPixmap):
            painter.drawPixmap(QtCore.QRect(ax, ay, avatar_size, avatar_size), avatar.scaled(avatar_size, avatar_size, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))
        else:
            hue = (sum(ord(c) for c in sender) % 360)
            avatar_color = QtGui.QColor.fromHsl(hue, 160, 160)
            painter.setBrush(avatar_color)
            painter.setPen(QtCore.Qt.NoPen)
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
            h = fm.height() + 20
            return QtCore.QSize(option.rect.width(), h)
        if kind == "file":
            fm = option.fontMetrics
            try:
                vw = option.rect.width() if option.rect.width() > 0 else (option.widget.viewport().width() if option.widget else 0)
            except Exception:
                vw = option.rect.width()
            maxw = int(vw * 0.5)
            mime = (index.data(ChatModel.MimeRole) or "").lower()
            pix = index.data(ChatModel.PixmapRole)
            if mime.startswith("image/") and isinstance(pix, QtGui.QPixmap):
                img_w = min(maxw, pix.width())
                img_h = int(pix.height() * (img_w / max(1, pix.width())))
                spacing = 16
                h = img_h + spacing
                avatar_block = 22 + 24
                h = max(h, avatar_block)
                return QtCore.QSize(option.rect.width(), h)
            # non-image file chip height
            chip_h = 56
            spacing = 16
            avatar_block = 22 + 24
            h = chip_h + spacing
            h = max(h, avatar_block)
            return QtCore.QSize(option.rect.width(), h)
        fdoc = QtGui.QFont(option.font)
        try:
            base_ps2 = int(fdoc.pointSize()) if fdoc.pointSize() > 0 else 14
            fdoc.setPointSize(base_ps2 + 2)
        except Exception:
            pass
        fm = QtGui.QFontMetrics(fdoc)
        try:
            vw = option.rect.width() if option.rect.width() > 0 else (option.widget.viewport().width() if option.widget else 0)
        except Exception:
            vw = option.rect.width()
        maxw = int(vw * 0.65)
        doc = QtGui.QTextDocument()
        opt = QtGui.QTextOption()
        opt.setWrapMode(QtGui.QTextOption.WordWrap)
        doc.setDefaultFont(fdoc)
        doc.setDefaultTextOption(opt)
        doc.setDocumentMargin(0)
        w0 = fm.horizontalAdvance(text)
        text_w = min(w0, maxw)
        doc.setTextWidth(text_w)
        try:
            import html as _html
            s2 = text or ""
            base_sz2 = option.font.pointSize()
            if base_sz2 is None or base_sz2 <= 0:
                base_sz2 = 14
            big_sz2 = int(base_sz2) + 8
            def _emoji_html2(seg: str) -> str:
                try:
                    pat_emoji2 = re.compile(r'([\u2600-\u27BF\U0001F300-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FAFF](?:\uFE0F)?(?:\u200D[\u2600-\u27BF\U0001F300-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FAFF](?:\uFE0F)?)*)')
                    out2 = []
                    pos2 = 0
                    for m2 in pat_emoji2.finditer(seg):
                        out2.append(_html.escape(seg[pos2:m2.start()]))
                        out2.append("<span style='font-size:{}pt;line-height:1.0em'>".format(big_sz2) + _html.escape(m2.group(0)) + "</span>")
                        pos2 = m2.end()
                    out2.append(_html.escape(seg[pos2:]))
                    return "".join(out2)
                except Exception:
                    return _html.escape(seg)
            doc.setHtml(_emoji_html2(s2))
        except Exception:
            doc.setHtml(text or "")
        is_self = bool(index.data(ChatModel.SelfRole))
        pad = 12
        bubble_h = int(doc.size().height()) + pad * 2
        extra_top = 0 if is_self else fm.height()
        extra_bottom = fm.height()
        spacing = 16
        avatar_block = 22 + 24
        h = bubble_h + extra_top + extra_bottom + spacing
        h = max(h, avatar_block)
        return QtCore.QSize(option.rect.width(), h)


class ChatInput(QtWidgets.QTextEdit):
    imagePasted = QtCore.Signal(object, object, str, str)
    sendRequested = QtCore.Signal()
    fileChipCleared = QtCore.Signal()

    def __init__(self):
        super().__init__()
        self.setAcceptRichText(True)
        self.setLineWrapMode(QtWidgets.QTextEdit.WidgetWidth)
        self._last_paste_kind = ""
        self._last_paste_tick = 0
        self._skip_image_paste_until = 0
        self._file_chip_range = None

    def insertFromMimeData(self, source: QtCore.QMimeData):
        try:
            fmts = list(source.formats()) if source else []
        except Exception:
            pass
        try:
            now_ms = int(QtCore.QDateTime.currentMSecsSinceEpoch())
            if self._last_paste_kind == "urls" and (now_ms - self._last_paste_tick) < 600:
                return
        except Exception:
            pass
        try:
            if source and source.hasUrls():
                for u in source.urls():
                    p = u.toLocalFile()
                    
                    isf = bool(p) and os.path.isfile(p)
                    
                    if isf:
                        try:
                            reader = QtGui.QImageReader(p)
                            qimg = reader.read()
                            if isinstance(qimg, QtGui.QImage) and not qimg.isNull():
                                self._insert_preview_image(qimg)
                            else:
                                pm = QtGui.QPixmap(p)
                                if isinstance(pm, QtGui.QPixmap) and not pm.isNull():
                                    self._insert_preview_image(pm.toImage())
                            with open(p, "rb") as f:
                                data = f.read()
                            pm2 = QtGui.QPixmap(p)
                            name = os.path.basename(p)
                            mime = self._guess_mime(p)
                            if not pm2.isNull():
                                self.imagePasted.emit(data, pm2, mime, name)
                            else:
                                # Generic file with icon chip
                                self._insert_file_chip(name, len(data))
                                self.imagePasted.emit(data, None, mime, name)
                            try:
                                self._last_paste_kind = "urls"
                                self._last_paste_tick = int(QtCore.QDateTime.currentMSecsSinceEpoch())
                            except Exception:
                                pass
                            return
                        except Exception:
                            # Fallback: still emit as generic file with placeholder
                            name = os.path.basename(p) or "file"
                            mime = self._guess_mime(p)
                            with open(p, "rb") as f:
                                data = f.read()
                            self._insert_file_chip(name, len(data))
                            self.imagePasted.emit(data, None, mime, name)
                            self._last_paste_kind = "urls"
                            self._last_paste_tick = int(QtCore.QDateTime.currentMSecsSinceEpoch())
                            return
                    else:
                        # Fallback: insert raw file URL into editor so Enter can send it
                        try:
                            raw = u.toString()
                            if raw:
                                self.textCursor().insertText(raw)
                                self._last_paste_kind = "urls"
                                self._last_paste_tick = int(QtCore.QDateTime.currentMSecsSinceEpoch())
                                return
                        except Exception:
                            pass
        except Exception:
            pass
        # 2) 再处理剪贴板内嵌位图
        try:
            try:
                fmts2 = list(source.formats()) if source else []
            except Exception:
                fmts2 = []
            now_ms = int(QtCore.QDateTime.currentMSecsSinceEpoch())
            if source and source.hasImage() and ("text/uri-list" in fmts2 or "public.file-url" in fmts2 or (self._last_paste_kind == "urls" and (now_ms - self._last_paste_tick) < 2000)):
                
                return
            if source and source.hasImage():
                qobj = source.imageData()
                qimg = None
                if isinstance(qobj, QtGui.QImage):
                    qimg = qobj
                elif isinstance(qobj, QtGui.QPixmap):
                    qimg = qobj.toImage()
                elif hasattr(qobj, 'toImage'):
                    try:
                        qimg = qobj.toImage()
                    except Exception:
                        qimg = None
                if isinstance(qimg, QtGui.QImage) and not qimg.isNull():
                    self._insert_preview_image(qimg)
                    buf = QtCore.QBuffer()
                    buf.open(QtCore.QIODevice.WriteOnly)
                    qimg.save(buf, "PNG")
                    data = bytes(buf.data())
                    name = "paste_" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch())) + ".png"
                    pm = QtGui.QPixmap.fromImage(qimg)
                    self.imagePasted.emit(data, pm, "image/png", name)
                    return
        except Exception:
            pass
        try:
            if source and source.hasHtml():
                html = source.html() or ""
                marker = "data:image"
                idx = html.find(marker)
                if idx != -1 and "base64," in html[idx:]:
                    start = html.find("base64,", idx) + len("base64,")
                    end1 = html.find("'", start)
                    end2 = html.find('"', start)
                    ends = [e for e in [end1, end2] if e != -1]
                    end = min(ends) if ends else -1
                    b64 = html[start:end] if end != -1 else html[start:]
                    data = base64.b64decode(b64)
                    qimg = QtGui.QImage()
                    qimg.loadFromData(data)
                    if not qimg.isNull():
                        self._insert_preview_image(qimg)
                        pm = QtGui.QPixmap.fromImage(qimg)
                        name = "paste_" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch())) + ".png"
                        self.imagePasted.emit(data, pm, "image/png", name)
                        return
                if "file://" in html:
                    i = html.find("file://")
                    end = len(html)
                    for sep in ['"', "'", ")", " ", "\n"]:
                        j = html.find(sep, i)
                        if j != -1:
                            end = min(end, j)
                    url = html[i:end]
                    p = QtCore.QUrl(url).toLocalFile()
                    
                    if p and os.path.isfile(p):
                        reader = QtGui.QImageReader(p)
                        qimg = reader.read()
                        if isinstance(qimg, QtGui.QImage) and not qimg.isNull():
                            self._insert_preview_image(qimg)
                            with open(p, "rb") as f:
                                data = f.read()
                            pm = QtGui.QPixmap(p)
                            name = os.path.basename(p)
                            self.imagePasted.emit(data, pm, self._guess_mime(p), name)
                            return
                        else:
                            with open(p, "rb") as f:
                                data = f.read()
                            name = os.path.basename(p)
                            mime = self._guess_mime(p)
                            self._insert_file_chip(name, len(data))
                            self.imagePasted.emit(data, None, mime, name)
                            return
        except Exception:
            pass
        # 3) 其余按 HTML/text 路径处理
        try:
            if source and source.hasText():
                t = source.text() or ""
                if "file://" in t or t.lower().endswith((".png",".jpg",".jpeg",".gif")):
                    i = t.find("file://")
                    if i != -1:
                        url = t[i:]
                        p = QtCore.QUrl(url).toLocalFile()
                    else:
                        p = t.strip()
                
                    if p and os.path.isfile(p):
                        reader = QtGui.QImageReader(p)
                        qimg = reader.read()
                        if isinstance(qimg, QtGui.QImage) and not qimg.isNull():
                            self._insert_preview_image(qimg)
                        else:
                            pm = QtGui.QPixmap(p)
                            if isinstance(pm, QtGui.QPixmap) and not pm.isNull():
                                self._insert_preview_image(pm.toImage())
                        with open(p, "rb") as f:
                            data = f.read()
                        pm = QtGui.QPixmap(p)
                        name = os.path.basename(p)
                        if not pm.isNull():
                            self.imagePasted.emit(data, pm, self._guess_mime(p), name)
                        else:
                            self._insert_file_chip(name, len(data))
                            self.imagePasted.emit(data, None, self._guess_mime(p), name)
                        return
        except Exception:
            pass
        super().insertFromMimeData(source)

    def dragEnterEvent(self, ev: QtGui.QDragEnterEvent):
        try:
            md = ev.mimeData()
            ok = False
            if md.hasImage():
                ok = True
            elif md.hasUrls():
                for u in md.urls():
                    p = u.toLocalFile()
                    if p and os.path.isfile(p):
                        ext = os.path.splitext(p)[1].lower()
                        if ext in (".png", ".jpg", ".jpeg", ".gif"):
                            ok = True
                            break
            if ok:
                ev.acceptProposedAction()
                return
        except Exception:
            pass
        super().dragEnterEvent(ev)

    def dropEvent(self, ev: QtGui.QDropEvent):
        try:
            md = ev.mimeData()
            if md.hasImage():
                qobj = md.imageData()
                qimg = None
                if isinstance(qobj, QtGui.QImage):
                    qimg = qobj
                elif isinstance(qobj, QtGui.QPixmap):
                    qimg = qobj.toImage()
                elif hasattr(qobj, 'toImage'):
                    try:
                        qimg = qobj.toImage()
                    except Exception:
                        qimg = None
                if isinstance(qimg, QtGui.QImage) and not qimg.isNull():
                    self._insert_preview_image(qimg)
                    buf = QtCore.QBuffer(); buf.open(QtCore.QIODevice.WriteOnly); qimg.save(buf, "PNG")
                    data = bytes(buf.data())
                    name = "paste_" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch())) + ".png"
                    pm = QtGui.QPixmap.fromImage(qimg)
                    self.imagePasted.emit(data, pm, "image/png", name)
                    ev.acceptProposedAction()
                    return
            if md.hasUrls():
                for u in md.urls():
                    p = u.toLocalFile()
                    if p and os.path.isfile(p):
                        ext = os.path.splitext(p)[1].lower()
                        if ext in (".png", ".jpg", ".jpeg", ".gif"):
                            qimg = QtGui.QImage(p)
                            if not qimg.isNull():
                                self._insert_preview_image(qimg)
                            with open(p, "rb") as f:
                                data = f.read()
                            pm = QtGui.QPixmap(p)
                            name = os.path.basename(p)
                            mime = self._guess_mime(p)
                            self.imagePasted.emit(data, pm if not pm.isNull() else None, mime, name)
                            ev.acceptProposedAction()
                            return
                        else:
                            with open(p, "rb") as f:
                                data = f.read()
                            name = os.path.basename(p)
                            mime = self._guess_mime(p)
                            self._insert_file_chip(name, len(data))
                            self.imagePasted.emit(data, None, mime, name)
                            ev.acceptProposedAction()
                            return
        except Exception:
            pass
        super().dropEvent(ev)

    def keyPressEvent(self, ev: QtGui.QKeyEvent):
        try:
            if ev.key() in (QtCore.Qt.Key_Return, QtCore.Qt.Key_Enter):
                mods = ev.modifiers()
                if mods & QtCore.Qt.MetaModifier:
                    pass
                elif mods & QtCore.Qt.ControlModifier:
                    self.sendRequested.emit()
                    return
                elif not (mods & (QtCore.Qt.ShiftModifier | QtCore.Qt.AltModifier | QtCore.Qt.MetaModifier | QtCore.Qt.ControlModifier)):
                    self.sendRequested.emit()
                    return
            elif ev.key() == QtCore.Qt.Key_Backspace:
                if self._file_chip_range and not self.textCursor().hasSelection():
                    try:
                        s, e = self._file_chip_range
                        c = QtGui.QTextCursor(self.document())
                        c.setPosition(s)
                        c.setPosition(e, QtGui.QTextCursor.KeepAnchor)
                        c.removeSelectedText()
                        self._file_chip_range = None
                        try:
                            self.fileChipCleared.emit()
                        except Exception:
                            pass
                        return
                    except Exception:
                        pass
        except Exception:
            pass
        super().keyPressEvent(ev)

    def _insert_preview_datauri(self, data: bytes, mime: str):
        try:
            b64 = base64.b64encode(data).decode("ascii")
            w = self._preview_target_width()
            html = f"<img src='data:{mime};base64,{b64}' style='max-width:{w}px; height:auto;'/>"
            self.textCursor().insertHtml(html)
        except Exception:
            pass

    def _insert_preview_image(self, qimg: QtGui.QImage):
        try:
            if qimg.isNull():
                return
            name = "inline-" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch()))
            w = min(self._preview_target_width(), qimg.width())
            h = int(qimg.height() * (w / max(1, qimg.width())))
            fmt = QtGui.QTextImageFormat()
            fmt.setName(name)
            fmt.setWidth(float(w))
            fmt.setHeight(float(h))
            self.document().addResource(QtGui.QTextDocument.ImageResource, QtCore.QUrl(name), qimg)
            cur = self.textCursor()
            cur.insertImage(fmt)
        except Exception:
            try:
                self.textCursor().insertImage(qimg)
            except Exception:
                pass

    def _insert_document_icon(self, size: int = 18):
        try:
            icon_path = os.path.join(os.getcwd(), "icons", "ui", "document.png")
            pm = QtGui.QPixmap(icon_path)
            if pm.isNull():
                return
            pm = pm.scaled(size, size, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
            name = "doc-" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch()))
            fmt = QtGui.QTextImageFormat()
            fmt.setName(name)
            fmt.setWidth(float(pm.width()))
            fmt.setHeight(float(pm.height()))
            self.document().addResource(QtGui.QTextDocument.ImageResource, QtCore.QUrl(name), pm.toImage())
            cur = self.textCursor()
            cur.insertImage(fmt)
            cur.insertText(" ")
        except Exception:
            pass

    def _human_size(self, n: int) -> str:
        try:
            units = ["B", "KB", "MB", "GB", "TB"]
            i = 0
            f = float(n)
            while f >= 1024.0 and i < len(units) - 1:
                f /= 1024.0
                i += 1
            return ("{:.1f}{}".format(f, units[i]) if i > 0 else "{}{}".format(int(f), units[i]))
        except Exception:
            return "0B"

    def _insert_file_chip(self, name: str, size_bytes: int):
        try:
            size_str = self._human_size(int(size_bytes) if size_bytes is not None else 0)
            cur = self.textCursor()
            start_pos = cur.position()
            icon_b64 = ""
            try:
                pm = QtGui.QPixmap(os.path.join(os.getcwd(), "icons", "ui", "document.png"))
                if not pm.isNull():
                    pm = pm.scaled(22, 22, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
                    buf = QtCore.QBuffer(); buf.open(QtCore.QIODevice.WriteOnly)
                    pm.toImage().save(buf, "PNG")
                    icon_b64 = base64.b64encode(bytes(buf.data())).decode("ascii")
            except Exception:
                pass
            import html as _html
            esc_name = _html.escape(name or "")
            img_tag = (f"<img src='data:image/png;base64,{icon_b64}' style='width:22px;height:22px;'/>" if icon_b64 else "")
            html = (
                "<div style='display:inline-block;background-color:rgba(0,0,0,0.06);border-radius:10px;padding:10px 12px;margin:4px 0;width:400px;'>"
                "<table style='border-collapse:collapse;border:none;width:100%;'>"
                "<tr>"
                "<td style='vertical-align:middle;'>"
                f"<div style='font-weight:600;color:#333;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;'>{esc_name}</div>"
                f"<div style='font-size:12px;color:#777;margin-top:4px;'>{size_str}</div>"
                "</td>"
                f"<td style='vertical-align:middle;width:30px;text-align:right;'>{img_tag}</td>"
                "</tr>"
                "</table>"
                "</div>"
            )
            self.textCursor().insertHtml(html)
            end_pos = self.textCursor().position()
            if end_pos > start_pos:
                self._file_chip_range = (start_pos, end_pos)
        except Exception:
            # fallback to simple text
            try:
                self._insert_document_icon(18)
            except Exception:
                pass
            self.textCursor().insertText(f"[文件: {name}] {size_str}")

    def _preview_target_width(self) -> int:
        try:
            vw = self.viewport().width()
            w = int(vw * 0.8) if vw > 0 else 300
            return max(200, min(w, 600))
        except Exception:
            return 300

    def _guess_mime(self, path: str) -> str:
        try:
            ext = os.path.splitext(path)[1].lower()
            if ext in [".png"]:
                return "image/png"
            if ext in [".jpg", ".jpeg"]:
                return "image/jpeg"
            if ext in [".gif"]:
                return "image/gif"
            import mimetypes
            m, _ = mimetypes.guess_type(path)
            return m or "application/octet-stream"
        except Exception:
            return "application/octet-stream"

class SidebarItem(QtWidgets.QFrame):
    clicked = QtCore.Signal()

    def __init__(self, icon_path: str, text: str):
        super().__init__()
        self.setObjectName("SidebarItem")
        vl = QtWidgets.QVBoxLayout()
        try:
            vl.setContentsMargins(6, 6, 6, 6)
            vl.setSpacing(4)
        except Exception:
            pass
        self.icon = QtWidgets.QLabel()
        self.icon.setFixedSize(24, 24)
        try:
            self.icon.setPixmap(QtGui.QIcon(icon_path).pixmap(24, 24))
        except Exception:
            pass
        self.label = QtWidgets.QLabel(text)
        try:
            self.label.setAlignment(QtCore.Qt.AlignHCenter)
            self.label.setWordWrap(True)
        except Exception:
            pass
        self.badge = QtWidgets.QLabel()
        self.badge.setVisible(False)
        try:
            self.badge.setStyleSheet("QLabel{background:#F44336;color:#fff;border-radius:8px;padding:0 6px;font:11px 'Helvetica Neue';}")
        except Exception:
            pass
        vl.addWidget(self.icon, 0, QtCore.Qt.AlignHCenter)
        vl.addWidget(self.label, 0, QtCore.Qt.AlignHCenter)
        vl.addWidget(self.badge, 0, QtCore.Qt.AlignHCenter)
        self.setLayout(vl)
        self.setSelected(False)

    def mouseReleaseEvent(self, ev: QtGui.QMouseEvent):
        try:
            self.clicked.emit()
        except Exception:
            pass

    def setSelected(self, sel: bool):
        try:
            if sel:
                self.setStyleSheet("QFrame#SidebarItem{background:#e6f0ff;border:1px solid #cfe3ff;border-radius:6px;}")
            else:
                self.setStyleSheet("QFrame#SidebarItem{background:transparent;border:none;}")
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
        return "application/octet-stream"

class ChatSplitter(QtWidgets.QSplitter):
    def resizeEvent(self, event):
        try:
            super().resizeEvent(event)
            h = self.height()
            if h > 0 and self.count() > 1:
                # widget(1) is the input box
                w = self.widget(1)
                if w:
                    # Limit input box to 40% of total height
                    limit = int(h * 0.4)
                    # Respect minimum height of 100
                    limit = max(limit, 100)
                    w.setMaximumHeight(limit)
        except Exception:
            pass

class ChatListView(QtWidgets.QListView):
    def resizeEvent(self, event: QtGui.QResizeEvent):
        try:
            super().resizeEvent(event)
            # Recalculate item geometries when viewport width changes
            self.doItemsLayout()
            self.viewport().update()
        except Exception:
            pass
class UploadDialog(QtWidgets.QDialog):
    progressRequested = QtCore.Signal()
    pausedToggled = QtCore.Signal()
    canceledRequested = QtCore.Signal()
    def __init__(self, total_bytes: int):
        super().__init__()
        self.setWindowTitle("发送文件")
        self.total = int(max(0, total_bytes))
        self._paused = False
        bar = QtWidgets.QProgressBar()
        bar.setRange(0, self.total if self.total > 0 else 0)
        bar.setValue(0)
        self._bar = bar
        btn_pause = QtWidgets.QPushButton("暂停")
        btn_cancel = QtWidgets.QPushButton("取消")
        btn_pause.clicked.connect(self._on_pause)
        btn_cancel.clicked.connect(self._on_cancel)
        h = QtWidgets.QHBoxLayout()
        h.addStretch(1)
        h.addWidget(btn_pause)
        h.addWidget(btn_cancel)
        v = QtWidgets.QVBoxLayout()
        v.addWidget(self._bar)
        v.addLayout(h)
        self.setLayout(v)
        try:
            self.setModal(False)
        except Exception:
            pass
        try:
            self.setFixedWidth(380)
        except Exception:
            pass
    def update_progress(self, sent: int):
        try:
            self._bar.setValue(int(max(0, sent)))
        except Exception:
            pass
    def _on_pause(self):
        try:
            self._paused = not self._paused
            self.pausedToggled.emit()
        except Exception:
            pass
    def _on_cancel(self):
        try:
            self.canceledRequested.emit()
        except Exception:
            pass
class FileUploadWorker(QtCore.QThread):
    progress = QtCore.Signal(int, int)
    finished = QtCore.Signal(bool, str)
    def __init__(self, sock: Optional[socket.socket], header: bytes, path: str, chunk: int = 65536):
        super().__init__()
        self.sock = sock
        self.header = header
        self.path = path
        self.chunk = int(max(1024, chunk))
        self._paused = False
        self._canceled = False
    def pause_toggle(self):
        try:
            self._paused = not self._paused
        except Exception:
            pass
    def cancel(self):
        try:
            self._canceled = True
        except Exception:
            pass
    def run(self):
        try:
            if not self.sock or not self.path or not os.path.isfile(self.path):
                self.finished.emit(False, "socket或路径无效")
                return
            try:
                self.sock.sendall(self.header)
            except Exception as e:
                self.finished.emit(False, str(e))
                return
            total = 0
            try:
                total = os.path.getsize(self.path)
            except Exception:
                total = 0
            sent = 0
            f = None
            try:
                f = open(self.path, "rb")
            except Exception as e:
                self.finished.emit(False, str(e))
                return
            try:
                while True:
                    if self._canceled:
                        break
                    if self._paused:
                        QtCore.QThread.msleep(100)
                        continue
                    buf = f.read(self.chunk)
                    if not buf:
                        break
                    try:
                        b64 = base64.b64encode(buf)
                        self.sock.sendall(b64)
                        QtCore.QThread.msleep(1)
                    except Exception as e:
                        self.finished.emit(False, str(e))
                        try:
                            f.close()
                        except Exception:
                            pass
                        return
                    sent += len(buf)
                    try:
                        self.progress.emit(sent, total)
                    except Exception:
                        pass
                try:
                    f.close()
                except Exception:
                    pass
                if self._canceled:
                    self.finished.emit(False, "已取消")
                    return
                try:
                    self.sock.sendall(b"\n")
                except Exception as e:
                    self.finished.emit(False, str(e))
                    return
                self.finished.emit(True, "")
            except Exception as e:
                try:
                    f and f.close()
                except Exception:
                    pass
                self.finished.emit(False, str(e))
        except Exception:
            try:
                self.finished.emit(False, "异常")
            except Exception:
                pass
class FileChunkSender(QtCore.QThread):
    progress = QtCore.Signal(int)
    finished = QtCore.Signal(bool, str)
    def __init__(self, sock: socket.socket, path: str, chunks: list, prefix: str, paused_ref: list, canceled_ref: list, chunk_size: int, logger: Optional[ChatLogger] = None, username: Optional[str] = None):
        super().__init__()
        self.sock = sock
        self.path = path
        self.chunks = chunks
        self.prefix = prefix
        self._paused_ref = paused_ref
        self._canceled_ref = canceled_ref
        self.sz = int(max(1024, chunk_size))
        self._logger = logger
        self._log_user = username or ""
    def run(self):
        try:
            f = open(self.path, "rb")
        except Exception as e:
            self.finished.emit(False, str(e))
            return
        try:
            try:
                total = os.path.getsize(self.path)
            except Exception:
                total = 0
            for offset in self.chunks:
                if self._canceled_ref[0]:
                    break
                while self._paused_ref[0]:
                    QtCore.QThread.msleep(100)
                    if self._canceled_ref[0]:
                        break
                if self._canceled_ref[0]:
                    break
                try:
                    f.seek(offset)
                    expected = self.sz
                    try:
                        if total > 0:
                            rem = int(max(0, total - int(max(0, offset))))
                            expected = int(max(0, min(self.sz, rem)))
                    except Exception:
                        expected = self.sz
                    if expected <= 0:
                        buf = b""
                    else:
                        parts = []
                        remaining = expected
                        while remaining > 0:
                            b = f.read(remaining)
                            if not b:
                                break
                            parts.append(b)
                            remaining -= len(b)
                        buf = b"".join(parts)
                except Exception as e:
                    self.finished.emit(False, str(e))
                    try:
                        f.close()
                    except Exception:
                        pass
                    return
                try:
                    if expected > 0 and len(buf) < expected:
                        try:
                            if self._logger:
                                self._logger.write("send", self._log_user, f"WARN short_read off={int(max(0,offset))} expected={int(max(0,expected))} got={len(buf)}")
                        except Exception:
                            pass
                except Exception:
                    pass
                if not buf:
                    continue
                try:
                    try:
                        if self._logger:
                            self._logger.write("send", self._log_user, f"FILE_CHUNK off={int(max(0,offset))} size={len(buf)}")
                    except Exception:
                        pass
                    b64 = base64.b64encode(buf).decode("ascii")
                    line = f"{self.prefix} FILE_CHUNK {offset} {b64}\n".encode("utf-8")
                    self.sock.sendall(line)
                    self.progress.emit(len(buf))
                    QtCore.QThread.msleep(1)
                except Exception as e:
                    self.finished.emit(False, str(e))
                    try:
                        f.close()
                    except Exception:
                        pass
                    return
            try:
                f.close()
            except Exception:
                pass
            if self._canceled_ref[0]:
                self.finished.emit(False, "已取消")
            else:
                self.finished.emit(True, "")
        except Exception as e:
            try:
                f and f.close()
            except Exception:
                pass
            self.finished.emit(False, str(e))
class MultiConnFileUploader(QtCore.QObject):
    progress = QtCore.Signal(int, int)
    finished = QtCore.Signal(bool, str)
    def __init__(self, host: str, port: int, username: str, room_or_rid: str, mode: str, target: Optional[str], path: str, conn_count: int = 3, chunk_size: int = 2097152, extra: Optional[str] = "", override_name: Optional[str] = None, logger: Optional[ChatLogger] = None):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.room_or_rid = room_or_rid
        self.mode = mode
        self.target = target
        self.path = path
        self.conn_count = max(1, int(conn_count))
        self.chunk_size = int(max(65536, chunk_size))
        self.extra = extra or ""
        self._override_name = override_name
        self._paused = [False]
        self._canceled = [False]
        self._total = 0
        self._sent = 0
        self._socks = []
        self._md5 = ""
        self._resume_written = 0
        self._threads = []
        self._logger = logger
        self._prefix = ""
        self._offsets = set()
        self._acked = set()
        self._resend_rounds = 0
        self._max_resend_rounds = 2
    def pause_toggle(self):
        try:
            self._paused[0] = not self._paused[0]
        except Exception:
            pass
    def cancel(self):
        try:
            self._canceled[0] = True
            for t in list(self._threads):
                try:
                    t.wait(10000)
                except Exception:
                    pass
            try:
                for s in list(self._socks):
                    try:
                        s.shutdown(socket.SHUT_RDWR)
                    except Exception:
                        pass
                    try:
                        s.close()
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            pass
    def set_resume_written(self, n: int):
        try:
            self._resume_written = int(max(0, n))
        except Exception:
            self._resume_written = 0
    def _open_socket(self) -> Optional[socket.socket]:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.host, self.port))
        except Exception:
            return None
        try:
            hello = f"HELLO {self.username} {self.room_or_rid} {self.extra}\n".encode("utf-8")
            s.sendall(hello)
        except Exception:
            pass
        return s
    def _calc_md5(self) -> str:
        try:
            h = hashlib.md5()
            with open(self.path, "rb") as f:
                while True:
                    b = f.read(1024 * 1024)
                    if not b:
                        break
                    h.update(b)
            return h.hexdigest()
        except Exception:
            return ""
    def start(self):
        try:
            if not os.path.isfile(self.path):
                self.finished.emit(False, "路径无效")
                return
            try:
                self._total = os.path.getsize(self.path)
            except Exception:
                self._total = 0
            for _ in range(self.conn_count):
                s = self._open_socket()
                if s:
                    self._socks.append(s)
            name = self._override_name or os.path.basename(self.path)
            mime = "application/octet-stream"
            try:
                ext = os.path.splitext(name)[1].lower()
                if ext in [".png"]:
                    mime = "image/png"
                elif ext in [".jpg",".jpeg"]:
                    mime = "image/jpeg"
                elif ext in [".gif"]:
                    mime = "image/gif"
                elif ext in [".txt"]:
                    mime = "text/plain"
            except Exception:
                pass
            prefix = ("DM " + self.target) if self.mode == "dm" and self.target else "MSG"
            self._prefix = prefix
            self._md5 = self._calc_md5()
            try:
                meta_line = f"{prefix} FILE_META {name} {mime} {self._total} {self._md5}\n".encode("utf-8")
                if self._socks:
                    self._socks[0].sendall(meta_line)
                try:
                    if self._logger:
                        self._logger.write("send", self.username, f"FILE_META name={name} mime={mime} size={int(max(0,self._total))} md5={self._md5}")
                except Exception:
                    pass
                if (self.mode == "dm" and self.target) or (self.mode == "group"):
                    qline = f"{prefix} FILE_QUERY {self._md5}\n".encode("utf-8")
                    self._socks[0].sendall(qline)
                    try:
                        if self._logger:
                            self._logger.write("send", self.username, f"FILE_QUERY md5={self._md5}")
                    except Exception:
                        pass
            except Exception:
                pass
            begin_line = f"{prefix} FILE_BEGIN {name} {mime} {self._total}\n".encode("utf-8")
            try:
                if self._socks:
                    self._socks[0].sendall(begin_line)
                try:
                    if self._logger:
                        self._logger.write("send", self.username, f"FILE_BEGIN name={name} mime={mime} size={int(max(0,self._total))}")
                except Exception:
                    pass
            except Exception:
                pass
            start_off = int(max(0, self._resume_written))
            offsets = list(range(start_off, self._total, self.chunk_size))
            try:
                self._offsets = set(offsets)
                self._acked = set()
            except Exception:
                pass
            groups = [[] for _ in range(max(1, len(self._socks)))]
            for i, off in enumerate(offsets):
                groups[i % len(groups)].append(off)
            threads = []
            for i, s in enumerate(self._socks):
                t = FileChunkSender(s, self.path, groups[i], prefix, self._paused, self._canceled, self.chunk_size, logger=self._logger, username=self.username)
                t.progress.connect(self._on_piece_sent)
                t.finished.connect(self._on_piece_finished)
                threads.append(t)
            self._pending = len(threads)
            self._threads = threads
            for t in threads:
                t.start()
        except Exception as e:
            self.finished.emit(False, str(e))
    def note_ack(self, off: int, wrote: int):
        try:
            if self._canceled[0]:
                return
            self._acked.add(int(max(0, off)))
            if self._pending <= 0:
                self._maybe_finalize_or_resend()
        except Exception:
            pass
    def _missing_offsets(self) -> list:
        try:
            return [o for o in sorted(list(self._offsets)) if o not in self._acked]
        except Exception:
            return []
    def _start_resend_round(self, offs: list):
        try:
            if not offs:
                self._maybe_finalize_or_resend()
                return
            groups = [[] for _ in range(max(1, len(self._socks)))]
            for i, off in enumerate(offs):
                groups[i % len(groups)].append(off)
            threads = []
            for i, s in enumerate(self._socks):
                t = FileChunkSender(s, self.path, groups[i], self._prefix, self._paused, self._canceled, self.chunk_size, logger=self._logger, username=self.username)
                t.progress.connect(self._on_piece_sent)
                t.finished.connect(self._on_piece_finished)
                threads.append(t)
            self._pending = len(threads)
            self._threads = threads
            for t in threads:
                t.start()
        except Exception:
            try:
                self._maybe_finalize_or_resend()
            except Exception:
                pass
    def _maybe_finalize_or_resend(self):
        try:
            missing = self._missing_offsets()
            if missing and (self._resend_rounds < self._max_resend_rounds):
                self._resend_rounds += 1
                self._start_resend_round(missing)
                return
            try:
                if self._socks:
                    end_line = f"MSG FILE_END\n".encode("utf-8") if self.mode != "dm" else f"DM {self.target} FILE_END\n".encode("utf-8")
                    self._socks[0].sendall(end_line)
                    try:
                        if self._logger:
                            self._logger.write("send", self.username, "FILE_END")
                    except Exception:
                        pass
            except Exception:
                pass
            self.finished.emit(True, "")
        except Exception:
            pass
    def _on_piece_sent(self, n: int):
        try:
            self._sent += int(max(0, n))
            self.progress.emit(self._sent, self._total)
            if self._total > 0:
                pct = int((self._sent / self._total) * 100)
                pass
        except Exception:
            pass
    def _on_piece_finished(self, ok: bool, err: str):
        try:
            self._pending -= 1
            if self._pending <= 0:
                if self._canceled[0]:
                    self.finished.emit(False, "已取消")
                else:
                    self._maybe_finalize_or_resend()
        except Exception:
            pass
class ScreenshotEditDialog(QtWidgets.QDialog):
    TOOL_PEN = 0
    TOOL_RECT = 1
    TOOL_CIRCLE = 2
    TOOL_ARROW = 3

    def __init__(self, pixmap, parent=None):
        super().__init__(parent)
        self.setWindowTitle("截图编辑")
        
        # Scale down if too large (70% of screen) to avoid huge windows
        screen = QtWidgets.QApplication.primaryScreen().availableGeometry()
        max_w = int(screen.width() * 0.7)
        max_h = int(screen.height() * 0.7)
        if pixmap.width() > max_w or pixmap.height() > max_h:
             self.pixmap = pixmap.scaled(max_w, max_h, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
        else:
             self.pixmap = pixmap
             
        # Undo history: stack of committed pixmaps
        self.history = [self.pixmap.copy()]
        
        self.current_tool = self.TOOL_PEN
        
        # Setup UI
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0,0,0,0)
        layout.setSpacing(0)
        
        self.image_label = QtWidgets.QLabel()
        self.image_label.setPixmap(self.history[-1])
        self.image_label.setAlignment(QtCore.Qt.AlignCenter)
        self.image_label.setMouseTracking(True)
        self.image_label.installEventFilter(self)
        
        self.scroll = QtWidgets.QScrollArea()
        self.scroll.setWidget(self.image_label)
        self.scroll.setWidgetResizable(True)
        self.scroll.setAlignment(QtCore.Qt.AlignCenter)
        self.scroll.setStyleSheet("QScrollArea{border:none;background:#333;}")
        
        layout.addWidget(self.scroll, 1)
        
        # Toolbar
        bar = QtWidgets.QWidget()
        bar.setObjectName("ToolBar")
        bar.setStyleSheet("""
            #ToolBar { background: #eee; border-top: 1px solid #ccc; }
            QPushButton { color: black; border: none; padding: 6px; border-radius: 4px; background: transparent; }
            QPushButton:hover { background: #e0e0e0; }
            QPushButton:checked { background: #d0d0d0; }
            QPushButton:pressed { background: #c0c0c0; }
        """)
        hbox = QtWidgets.QHBoxLayout(bar)
        hbox.setContentsMargins(10, 5, 10, 5)
        
        # Tools
        self.btn_pen = QtWidgets.QPushButton("画笔")
        self.btn_pen.setCheckable(True)
        self.btn_pen.setChecked(True)
        self.btn_pen.clicked.connect(lambda: self.set_tool(self.TOOL_PEN))
        
        self.btn_rect = QtWidgets.QPushButton("方框")
        self.btn_rect.setCheckable(True)
        self.btn_rect.clicked.connect(lambda: self.set_tool(self.TOOL_RECT))
        
        self.btn_circle = QtWidgets.QPushButton("圆圈")
        self.btn_circle.setCheckable(True)
        self.btn_circle.clicked.connect(lambda: self.set_tool(self.TOOL_CIRCLE))
        
        self.btn_arrow = QtWidgets.QPushButton("箭头")
        self.btn_arrow.setCheckable(True)
        self.btn_arrow.clicked.connect(lambda: self.set_tool(self.TOOL_ARROW))

        self.btn_undo = QtWidgets.QPushButton("撤销")
        self.btn_undo.clicked.connect(self.undo)
        self.shortcut_undo = QtGui.QShortcut(QtGui.QKeySequence.Undo, self)
        self.shortcut_undo.activated.connect(self.undo)
        
        hbox.addWidget(self.btn_pen)
        hbox.addWidget(self.btn_rect)
        hbox.addWidget(self.btn_circle)
        hbox.addWidget(self.btn_arrow)
        hbox.addWidget(self.btn_undo)
        
        hbox.addStretch(1)
        
        self.btn_cancel = QtWidgets.QPushButton("取消 (Esc)")
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_ok = QtWidgets.QPushButton("确定 (Enter)")
        self.btn_ok.clicked.connect(self.on_accept)
        
        hbox.addWidget(self.btn_cancel)
        hbox.addWidget(self.btn_ok)
        
        layout.addWidget(bar)
        
        # Drawing state
        self.drawing = False
        self.start_pos = QtCore.QPoint()
        self.last_pos = QtCore.QPoint()
        self.active_shape = None # Stores {type, start, end, selected}
        self.dragging_shape = False
        self.resizing_shape = False
        self.drag_start_pos = QtCore.QPoint()
        self.shape_start_pos = QtCore.QPoint()
        self.shape_end_pos = QtCore.QPoint()
        self.shapes = []
        self.active_index = None
        self.pending_shape_start = None
        self.drag_threshold = 5
        self.pending_shape_start = None
        self.drag_threshold = 5
        
        # Initial resize
        screen = QtWidgets.QApplication.primaryScreen().availableGeometry()
        w = min(screen.width() - 100, self.pixmap.width() + 50)
        h = min(screen.height() - 100, self.pixmap.height() + 100)
        self.resize(w, h)

    def set_tool(self, tool):
        if self.active_shape:
            self.commit_shape()
        self.current_tool = tool
        self.btn_pen.setChecked(tool == self.TOOL_PEN)
        self.btn_rect.setChecked(tool == self.TOOL_RECT)
        self.btn_circle.setChecked(tool == self.TOOL_CIRCLE)
        self.btn_arrow.setChecked(tool == self.TOOL_ARROW)

    def undo(self):
        if self.active_shape:
            self.active_shape = None
            self.active_index = None
            self.update_display()
            return
        if len(self.shapes) > 0:
            self.shapes.pop()
            self.update_display()
            return
        if len(self.history) > 1:
            self.history.pop()
            self.update_display()

    def commit_shape(self):
        if not self.active_shape:
            return
        
        if self.active_index is not None:
            self.shapes[self.active_index] = dict(self.active_shape)
        else:
            self.shapes.append(dict(self.active_shape))
        self.active_shape = None
        self.active_index = None
        self.update_display()

    def update_display(self):
        pixmap = self.history[-1].copy()
        if len(self.shapes) > 0:
            painter = QtGui.QPainter(pixmap)
            try:
                painter.setPen(QtGui.QPen(QtCore.Qt.red, 3, QtCore.Qt.SolidLine, QtCore.Qt.RoundCap, QtCore.Qt.RoundJoin))
                for i, shp in enumerate(self.shapes):
                    if self.active_index is not None and i == self.active_index:
                        continue
                    tool = shp['type']
                    start = shp['start']
                    end = shp['end']
                    if tool == self.TOOL_RECT:
                        rect = QtCore.QRect(start, end).normalized()
                        painter.drawRect(rect)
                    elif tool == self.TOOL_CIRCLE:
                        rect = QtCore.QRect(start, end).normalized()
                        painter.drawEllipse(rect)
                    elif tool == self.TOOL_ARROW:
                        self.draw_arrow(painter, start, end)
            finally:
                painter.end()
        if self.active_shape:
            painter = QtGui.QPainter(pixmap)
            try:
                painter.setPen(QtGui.QPen(QtCore.Qt.red, 3, QtCore.Qt.SolidLine, QtCore.Qt.RoundCap, QtCore.Qt.RoundJoin))
                
                tool = self.active_shape['type']
                start = self.active_shape['start']
                end = self.active_shape['end']
                
                if tool == self.TOOL_RECT:
                    rect = QtCore.QRect(start, end).normalized()
                    painter.drawRect(rect)
                    self.draw_handles(painter, rect)
                elif tool == self.TOOL_CIRCLE:
                    rect = QtCore.QRect(start, end).normalized()
                    painter.drawEllipse(rect)
                    self.draw_handles(painter, rect)
                elif tool == self.TOOL_ARROW:
                    self.draw_arrow(painter, start, end)
                    painter.setBrush(QtCore.Qt.white)
                    painter.setPen(QtCore.Qt.blue)
                    painter.drawEllipse(start, 4, 4)
                    painter.drawEllipse(end, 4, 4)
            finally:
                painter.end()
        self.image_label.setPixmap(pixmap)

    def _image_offset(self) -> QtCore.QPoint:
        pm = self.history[-1]
        w = self.image_label.width()
        h = self.image_label.height()
        dx = max(0, (w - pm.width()) // 2)
        dy = max(0, (h - pm.height()) // 2)
        return QtCore.QPoint(dx, dy)
    
    def _to_image_pos(self, pos: QtCore.QPoint) -> QtCore.QPoint:
        off = self._image_offset()
        return pos - off
    
    def _is_in_image(self, pos_img: QtCore.QPoint) -> bool:
        pm = self.history[-1]
        return 0 <= pos_img.x() < pm.width() and 0 <= pos_img.y() < pm.height()
    
    def _event_pos(self, event) -> QtCore.QPoint:
        try:
            return event.position().toPoint()
        except Exception:
            return event.pos()
 
    def draw_handles(self, painter, rect):
        painter.setBrush(QtCore.Qt.white)
        painter.setPen(QtCore.Qt.blue)
        r = 4
        # Corners
        painter.drawEllipse(rect.topLeft(), r, r)
        painter.drawEllipse(rect.topRight(), r, r)
        painter.drawEllipse(rect.bottomLeft(), r, r)
        painter.drawEllipse(rect.bottomRight(), r, r)

    def get_handle_at(self, pos):
        if not self.active_shape:
            return None
        
        tool = self.active_shape['type']
        start = self.active_shape['start']
        end = self.active_shape['end']
        limit = 10
        
        if tool in (self.TOOL_RECT, self.TOOL_CIRCLE):
            rect = QtCore.QRect(start, end).normalized()
            tl = rect.topLeft()
            tr = rect.topRight()
            bl = rect.bottomLeft()
            br = rect.bottomRight()
            
            if (pos - tl).manhattanLength() < limit: return 'tl'
            if (pos - tr).manhattanLength() < limit: return 'tr'
            if (pos - bl).manhattanLength() < limit: return 'bl'
            if (pos - br).manhattanLength() < limit: return 'br'
            if rect.contains(pos): return 'move'
            
        elif tool == self.TOOL_ARROW:
            if (pos - start).manhattanLength() < limit: return 'start'
            if (pos - end).manhattanLength() < limit: return 'end'
            # Check if close to line for move
            # Simplified: just bounding box for move
            rect = QtCore.QRect(start, end).normalized()
            if rect.contains(pos): return 'move'
            
        return None
    
    def get_handle_for(self, shape, pos):
        tool = shape['type']
        start = shape['start']
        end = shape['end']
        limit = 10
        if tool in (self.TOOL_RECT, self.TOOL_CIRCLE):
            rect = QtCore.QRect(start, end).normalized()
            tl = rect.topLeft()
            tr = rect.topRight()
            bl = rect.bottomLeft()
            br = rect.bottomRight()
            if (pos - tl).manhattanLength() < limit: return 'tl'
            if (pos - tr).manhattanLength() < limit: return 'tr'
            if (pos - bl).manhattanLength() < limit: return 'bl'
            if (pos - br).manhattanLength() < limit: return 'br'
            if rect.contains(pos): return 'move'
        elif tool == self.TOOL_ARROW:
            if (pos - start).manhattanLength() < limit: return 'start'
            if (pos - end).manhattanLength() < limit: return 'end'
            rect = QtCore.QRect(start, end).normalized()
            if rect.contains(pos): return 'move'
        return None
    
    def find_shape_at(self, pos):
        for i in range(len(self.shapes) - 1, -1, -1):
            h = self.get_handle_for(self.shapes[i], pos)
            if h:
                return i, h
        return None, None

    def eventFilter(self, obj, event):
        if obj == self.image_label:
            if event.type() == QtCore.QEvent.MouseButtonPress:
                if event.button() == QtCore.Qt.LeftButton:
                    # Check if clicking on active shape handle
                    pos_img = self._to_image_pos(self._event_pos(event))
                    handle = self.get_handle_at(pos_img)
                    
                    if self.active_shape and handle:
                        self.dragging_shape = (handle == 'move')
                        self.resizing_shape = (handle != 'move')
                        self.drag_handle = handle
                        self.drag_start_pos = self._event_pos(event)
                        self.drag_start_img_pos = pos_img
                        self.shape_start_pos = self.active_shape['start']
                        self.shape_end_pos = self.active_shape['end']
                        return True
                    
                    if not self.active_shape:
                        idx, h2 = self.find_shape_at(pos_img)
                        if idx is not None and h2:
                            self.active_index = idx
                            self.active_shape = dict(self.shapes[idx])
                            self.dragging_shape = (h2 == 'move')
                            self.resizing_shape = (h2 != 'move')
                            self.drag_handle = h2
                            self.drag_start_pos = self._event_pos(event)
                            self.drag_start_img_pos = pos_img
                            self.shape_start_pos = self.active_shape['start']
                            self.shape_end_pos = self.active_shape['end']
                            self.update_display()
                            return True
                    
                    # If clicking elsewhere, commit existing shape
                    if self.active_shape:
                        self.commit_shape()
                    
                    self.drawing = True
                    self.start_pos = pos_img
                    self.last_pos = pos_img
                    
                    if self.current_tool == self.TOOL_PEN:
                         self.temp_pixmap = self.history[-1].copy()
                    else:
                        self.pending_shape_start = pos_img
                    return True
            
            elif event.type() == QtCore.QEvent.MouseMove:
                if self.dragging_shape or self.resizing_shape:
                    pos_img = self._to_image_pos(self._event_pos(event))
                    delta = pos_img - self.drag_start_img_pos
                    
                    if self.dragging_shape:
                        self.active_shape['start'] = self.shape_start_pos + delta
                        self.active_shape['end'] = self.shape_end_pos + delta
                    elif self.resizing_shape:
                        if self.active_shape['type'] == self.TOOL_ARROW:
                            if self.drag_handle == 'start':
                                self.active_shape['start'] = self.shape_start_pos + delta
                            elif self.drag_handle == 'end':
                                self.active_shape['end'] = self.shape_end_pos + delta
                        else:
                            r = QtCore.QRect(self.shape_start_pos, self.shape_end_pos).normalized()
                            tl, tr, bl, br = r.topLeft(), r.topRight(), r.bottomLeft(), r.bottomRight()
                            new_pos = self.drag_start_img_pos + delta
                            if self.drag_handle == 'tl':
                                anchor = br
                            elif self.drag_handle == 'tr':
                                anchor = bl
                            elif self.drag_handle == 'bl':
                                anchor = tr
                            elif self.drag_handle == 'br':
                                anchor = tl
                            else:
                                anchor = tl
                            new_rect = QtCore.QRect(anchor, new_pos).normalized()
                            self.active_shape['start'] = new_rect.topLeft()
                            self.active_shape['end'] = new_rect.bottomRight()
                    
                    self.update_display()
                    return True

                if self.drawing:
                    pos_img = self._to_image_pos(self._event_pos(event))
                    if not self._is_in_image(pos_img):
                        return True
                    if self.current_tool == self.TOOL_PEN:
                        if not hasattr(self, 'temp_pixmap'):
                            self.temp_pixmap = self.history[-1].copy()
                        painter = QtGui.QPainter(self.temp_pixmap)
                        try:
                            painter.setPen(QtGui.QPen(QtCore.Qt.red, 3, QtCore.Qt.SolidLine, QtCore.Qt.RoundCap, QtCore.Qt.RoundJoin))
                            painter.drawLine(self.last_pos, pos_img)
                        finally:
                            painter.end()
                        self.last_pos = pos_img
                        self.image_label.setPixmap(self.temp_pixmap)
                    else:
                        if self.active_shape is None:
                            if (pos_img - (self.pending_shape_start or pos_img)).manhattanLength() >= self.drag_threshold:
                                self.active_shape = {
                                    'type': self.current_tool,
                                    'start': self.pending_shape_start,
                                    'end': pos_img
                                }
                                self.update_display()
                        else:
                            self.active_shape['end'] = pos_img
                            self.update_display()
                    return True
                    
                # Hover effect for handles
                pos_img = self._to_image_pos(self._event_pos(event))
                handle = None
                if self._is_in_image(pos_img):
                    if self.active_shape:
                        handle = self.get_handle_at(pos_img)
                    else:
                        _, handle = self.find_shape_at(pos_img)
                if handle:
                    if handle == 'move':
                        self.setCursor(QtCore.Qt.SizeAllCursor)
                    elif handle in ('tl', 'br', 'start', 'end'):
                        self.setCursor(QtCore.Qt.SizeFDiagCursor)
                    elif handle in ('tr', 'bl'):
                        self.setCursor(QtCore.Qt.SizeBDiagCursor)
                else:
                    self.setCursor(QtCore.Qt.ArrowCursor)
                    
            elif event.type() == QtCore.QEvent.MouseButtonRelease:
                if event.button() == QtCore.Qt.LeftButton:
                    if self.dragging_shape or self.resizing_shape:
                        self.dragging_shape = False
                        self.resizing_shape = False
                        return True
                        
                    if self.drawing:
                        self.drawing = False
                        if self.current_tool == self.TOOL_PEN:
                            if hasattr(self, 'temp_pixmap'):
                                self.history.append(self.temp_pixmap)
                        else:
                            if self.active_shape is None:
                                self.pending_shape_start = None
                        return True
        return super().eventFilter(obj, event)


    def draw_arrow(self, painter, start, end):
        painter.drawLine(start, end)
        dx = end.x() - start.x()
        dy = end.y() - start.y()
        angle = math.atan2(dy, dx)
        arrow_len = 15
        arrow_angle = math.pi / 6
        x1 = end.x() - arrow_len * math.cos(angle - arrow_angle)
        y1 = end.y() - arrow_len * math.sin(angle - arrow_angle)
        painter.drawLine(end, QtCore.QPoint(int(x1), int(y1)))
        x2 = end.x() - arrow_len * math.cos(angle + arrow_angle)
        y2 = end.y() - arrow_len * math.sin(angle + arrow_angle)
        painter.drawLine(end, QtCore.QPoint(int(x2), int(y2)))

    def on_accept(self):
        if self.active_shape:
            self.commit_shape()
        pm = self.history[-1].copy()
        if len(self.shapes) > 0:
            painter = QtGui.QPainter(pm)
            try:
                painter.setPen(QtGui.QPen(QtCore.Qt.red, 3, QtCore.Qt.SolidLine, QtCore.Qt.RoundCap, QtCore.Qt.RoundJoin))
                for shp in self.shapes:
                    tool = shp['type']
                    start = shp['start']
                    end = shp['end']
                    if tool == self.TOOL_RECT:
                        rect = QtCore.QRect(start, end).normalized()
                        painter.drawRect(rect)
                    elif tool == self.TOOL_CIRCLE:
                        rect = QtCore.QRect(start, end).normalized()
                        painter.drawEllipse(rect)
                    elif tool == self.TOOL_ARROW:
                        self.draw_arrow(painter, start, end)
            finally:
                painter.end()
        self.result_pixmap = pm
        self.accept()

class EmojiPicker(QtWidgets.QDialog):
    emojiSelected = QtCore.Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(QtCore.Qt.Popup | QtCore.Qt.FramelessWindowHint)
        self.setFocusPolicy(QtCore.Qt.StrongFocus)
        try:
            self.setAttribute(QtCore.Qt.WA_DeleteOnClose, True)
        except Exception:
            pass
        layout = QtWidgets.QVBoxLayout(self)
        try:
            layout.setContentsMargins(0, 0, 0, 0)
        except Exception:
            pass
        
        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        
        container = QtWidgets.QWidget()
        grid = QtWidgets.QGridLayout(container)
        try:
            grid.setSpacing(4)
            grid.setContentsMargins(8, 8, 8, 8)
        except Exception:
            pass
        
        emoji_dir = os.path.join(os.getcwd(), "icons", "emoji")
        files = []
        if os.path.exists(emoji_dir):
            try:
                files = sorted([f for f in os.listdir(emoji_dir) if f.endswith(".png")])
            except Exception:
                files = []
            
        row = 0
        col = 0
        max_cols = 8
        
        for fname in files:
            path = os.path.join(emoji_dir, fname)
            btn = QtWidgets.QPushButton()
            btn.setFixedSize(32, 32)
            btn.setIcon(QtGui.QIcon(path))
            btn.setIconSize(QtCore.QSize(24, 24))
            btn.setFlat(True)
            btn.setCursor(QtCore.Qt.PointingHandCursor)
            
            # Parse unicode
            try:
                base = os.path.splitext(fname)[0]
                parts = base.split("-")
                chars = "".join([chr(int(p, 16)) for p in parts])
                btn.clicked.connect(lambda checked=False, c=chars: self._on_click(c))
                btn.setToolTip(chars)
            except Exception:
                continue
                
            grid.addWidget(btn, row, col)
            col += 1
            if col >= max_cols:
                col = 0
                row += 1
                
        scroll.setWidget(container)
        layout.addWidget(scroll)
        
        # Calculate size
        rows = row + (1 if col > 0 else 0)
        h = min(300, max(100, rows * 36 + 20))
        w = max_cols * 36 + 30
        self.setFixedSize(w, h)
        try:
            app = QtWidgets.QApplication.instance()
            if app:
                app.installEventFilter(self)
        except Exception:
            pass
        
    def _on_click(self, char):
        self.emojiSelected.emit(char)
        self.accept()
    
    def focusOutEvent(self, e: QtGui.QFocusEvent):
        try:
            self.close()
        except Exception:
            pass
        try:
            super().focusOutEvent(e)
        except Exception:
            pass
    
    def eventFilter(self, obj, event):
        try:
            if event.type() == QtCore.QEvent.MouseButtonPress:
                gp = None
                try:
                    gp = event.globalPosition().toPoint()
                except Exception:
                    try:
                        gp = event.globalPos()
                    except Exception:
                        gp = None
                if gp is not None:
                    p = self.mapFromGlobal(gp)
                    if not self.rect().contains(p):
                        self.close()
                        return True
        except Exception:
            pass
        return False
    
    def closeEvent(self, e: QtGui.QCloseEvent):
        try:
            app = QtWidgets.QApplication.instance()
            if app:
                app.removeEventFilter(self)
        except Exception:
            pass
        try:
            super().closeEvent(e)
        except Exception:
            pass

class ChatWindow(QtWidgets.QWidget):
    def __init__(self, host: str, port: int, username: str, log_dir: str, room: str, avatar_path: Optional[str] = None):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.room = room
        self.room_name = room
        self.room_ready = False
        self.sock: Optional[socket.socket] = None
        self.rx: Optional[Receiver] = None
        self.logger = ChatLogger(log_dir, f"{host}_{port}")
        self.dm_target: Optional[str] = None
        self.seq = 1
        self.store = LocalStore(log_dir, username)
        self.avatar_pixmap = None
        self.avatar_filename = None
        if avatar_path and os.path.exists(avatar_path):
            try:
                self.avatar_pixmap = QtGui.QPixmap(avatar_path)
                self.avatar_filename = os.path.basename(avatar_path)
            except Exception:
                self.avatar_pixmap = None
                self.avatar_filename = None
        self.peer_avatars = {}
        self.avatar_file_map = {}
        self.rooms_info = []
        self.room_name_map = {}
        self.socks = {}
        self.receivers = {}
        self.max_upload_bytes = 40 * 1024 * 1024
        self.pending_dm = set()
        self.pending_dm_users = set()
        self.pending_join_users = set()
        self.online_users = set()
        self.conv_avatar_labels = {}
        self.upload_workers = {}
        self._fade_timers = {}
        self._rx_files = {}
        self._finalizing_files = set()
        try:
            app = QtWidgets.QApplication.instance()
            if app:
                app.aboutToQuit.connect(self._on_app_quit)
        except Exception:
            pass
        try:
            d = os.path.join(self.logger.log_dir, "avatars")
            p = os.path.join(d, "avatar_map.json")
            if os.path.isfile(p):
                with open(p, "r", encoding="utf-8") as f:
                    try:
                        self.avatar_file_map = json.load(f) or {}
                    except Exception:
                        self.avatar_file_map = {}
        except Exception:
            pass
        try:
            cfg_path = os.path.expanduser("~/Library/Application Support/XiaoCaiChat/client_config.json")
            if os.path.isfile(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                mb = int(data.get("max_upload_mb") or 40)
                self.max_upload_bytes = int(max(1, mb)) * 1024 * 1024
                self.screenshot_shortcut_seq = data.get("screenshot_shortcut", "Ctrl+Meta+S")
            else:
                self.screenshot_shortcut_seq = "Ctrl+Meta+S"
        except Exception:
            self.screenshot_shortcut_seq = "Ctrl+Meta+S"
        try:
            self.status_icon_online = QtGui.QPixmap(os.path.join(os.getcwd(), "icons", "ui", "online.png"))
        except Exception:
            self.status_icon_online = QtGui.QPixmap()
        try:
            self.status_icon_offline = QtGui.QPixmap(os.path.join(os.getcwd(), "icons", "ui", "offline.png"))
        except Exception:
            self.status_icon_offline = QtGui.QPixmap()

        self.setWindowTitle("XiaoCaiChat")
        self.view = ChatListView()
        self.view.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.view.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.view.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectItems)
        self.view.setUniformItemSizes(False)
        try:
            self.view.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
            self.view.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
            self.view.setStyleSheet(
                "QScrollBar:horizontal{height:0px;}"
                "QScrollBar:vertical{width:8px;background:transparent;margin:0;}"
                "QScrollBar::handle:vertical{background:#cfd8dc;border-radius:4px;min-height:24px;}"
                "QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical{height:0;}"
                "QScrollBar::up-arrow:vertical,QScrollBar::down-arrow:vertical{height:0;width:0;}"
            )
        except Exception:
            pass
        self.conv_models = {}
        self.current_model = None
        self.view.setItemDelegate(BubbleDelegate())
        self.view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.view.customContextMenuRequested.connect(self.on_view_context_menu)
        self.view.viewport().setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.view.viewport().customContextMenuRequested.connect(self.on_view_context_menu)
        self.view.viewport().installEventFilter(self)
        self.view.doubleClicked.connect(self.on_view_double_click)
        try:
            self.view.setMouseTracking(True)
            self.view.viewport().setMouseTracking(True)
            self.view.setProperty("search_highlight_row", -1)
        except Exception:
            pass
        try:
            self.find_shortcut = QtGui.QShortcut(QtGui.QKeySequence.Find, self)
            self.find_shortcut.activated.connect(self._show_find_bar)
        except Exception:
            pass
        self.copy_shortcut = QtGui.QShortcut(QtGui.QKeySequence.Copy, self.view)
        self.copy_shortcut.activated.connect(self.copy_selected)
        try:
            self.screenshot_shortcut = QtGui.QShortcut(QtGui.QKeySequence(self.screenshot_shortcut_seq), self)
            self.screenshot_shortcut.activated.connect(self.do_screenshot)
        except Exception:
            pass
        self.conv_list = QtWidgets.QListWidget()
        self.conv_list.setIconSize(QtCore.QSize(24, 24))
        try:
            self.conv_list.setFrameShape(QtWidgets.QFrame.NoFrame)
            self.conv_list.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
            self.conv_list.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
            self.conv_list.setFocusPolicy(QtCore.Qt.NoFocus)
            self.conv_list.setStyleSheet(
                "QListWidget{border:none;background:transparent;}"
                "QListWidget::item{border:none;padding:6px;}"
                "QListWidget::item:selected{background:transparent;border:1px solid #cfe3ff;border-radius:6px;}"
                "QListWidget::item:hover{background:#f5f7fa;}"
            )
        except Exception:
            pass
        # 左侧功能栏（头像 + 私聊入口）
        self.sidebar = QtWidgets.QFrame()
        self.sidebar.setFixedWidth(64)
        sb = QtWidgets.QVBoxLayout()
        try:
            sb.setContentsMargins(8, 8, 8, 8)
            sb.setSpacing(12)
        except Exception:
            pass
        self.avatar_label = QtWidgets.QLabel()
        self.avatar_label.setFixedSize(40, 40)
        self.avatar_label.setScaledContents(True)
        try:
            pm = self.avatar_pixmap if self.avatar_pixmap else self._letter_pixmap(self.username, 40)
            self.avatar_label.setPixmap(pm.scaled(40, 40, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))
        except Exception:
            pass
        msg_icon_path = os.path.join(os.getcwd(), "icons", "ui", "message.png")
        self.msg_item = SidebarItem(msg_icon_path, "消息")
        self.msg_item.clicked.connect(self.on_sidebar_message)
        self.msg_badge = self.msg_item.badge
        team_icon_path = os.path.join(os.getcwd(), "icons", "ui", "team.png")
        self.group_item = SidebarItem(team_icon_path, "群聊")
        self.group_item.clicked.connect(self.on_sidebar_group)
        self.group_badge = self.group_item.badge
        setting_icon_path = os.path.join(os.getcwd(), "icons", "ui", "setting.png")
        self.setting_item = SidebarItem(setting_icon_path, "设置")
        self.setting_item.clicked.connect(self.on_sidebar_setting)
        sb.addWidget(self.avatar_label, 0, alignment=QtCore.Qt.AlignTop | QtCore.Qt.AlignHCenter)
        self.user_label = QtWidgets.QLabel(self.username)
        try:
            self.user_label.setAlignment(QtCore.Qt.AlignHCenter)
        except Exception:
            pass
        sb.addWidget(self.user_label, 0, alignment=QtCore.Qt.AlignTop | QtCore.Qt.AlignHCenter)
        sb.addWidget(self.msg_item, 0, alignment=QtCore.Qt.AlignTop | QtCore.Qt.AlignHCenter)
        sb.addWidget(self.group_item, 0, alignment=QtCore.Qt.AlignTop | QtCore.Qt.AlignHCenter)
        sb.addWidget(self.setting_item, 0, alignment=QtCore.Qt.AlignTop | QtCore.Qt.AlignHCenter)
        sb.addStretch(1)
        self.sidebar.setLayout(sb)
        self.entry = ChatInput()
        self.entry.imagePasted.connect(self._on_image_pasted)
        try:
            self.entry.setMinimumHeight(100)
        except Exception:
            pass
        try:
            f = self.entry.font()
            ps = int(f.pointSize()) if f.pointSize() > 0 else 14
            f.setPointSize(ps + 1)
            self.entry.setFont(f)
        except Exception:
            pass
        self.pending_image_bytes = None
        self.pending_image_mime = None
        self.pending_image_name = None
        self.pending_image_pixmap = None
        self.pending_file_path = None
        # shortcuts bound to entry
        try:
            self.send_shortcut = QtGui.QShortcut(QtGui.QKeySequence("Ctrl+Return"), self.entry)
            self.send_shortcut.activated.connect(self.on_send)
        except Exception:
            pass
        self.entry.sendRequested.connect(self.on_send)
        try:
            self.entry.fileChipCleared.connect(self._on_file_chip_cleared)
        except Exception:
            pass
        layout = QtWidgets.QHBoxLayout()
        try:
            layout.setContentsMargins(8, 8, 8, 8)
            layout.setSpacing(8)
        except Exception:
            pass
        layout.addWidget(self.sidebar, 0)
        self.side_divider = QtWidgets.QFrame()
        try:
            self.side_divider.setObjectName("SideDivider")
            self.side_divider.setFrameShape(QtWidgets.QFrame.VLine)
            self.side_divider.setFrameShadow(QtWidgets.QFrame.Plain)
            self.side_divider.setFixedWidth(1)
            self.side_divider.setStyleSheet("QFrame#SideDivider{color:#d9d9d9;}")
        except Exception:
            pass
        layout.addWidget(self.side_divider, 0)
        left = QtWidgets.QVBoxLayout()
        try:
            left.setContentsMargins(0, 0, 0, 0)
            left.setSpacing(6)
        except Exception:
            pass
        left.addWidget(self.conv_list, 1)
        
        self.mid_divider = QtWidgets.QFrame()
        self.mid_divider.setFrameShape(QtWidgets.QFrame.VLine)
        self.mid_divider.setFrameShadow(QtWidgets.QFrame.Plain)
        try:
            self.mid_divider.setStyleSheet("color:#e0e0e0;")
            self.mid_divider.setFixedWidth(1)
        except Exception:
            pass
        
        left_layout_h = QtWidgets.QHBoxLayout()
        try:
            left_layout_h.setContentsMargins(0, 0, 0, 0)
            left_layout_h.setSpacing(0)
        except Exception:
            pass
        
        left_inner_container = QtWidgets.QWidget()
        left_inner_container.setLayout(left)
        
        left_layout_h.addWidget(left_inner_container)
        left_layout_h.addWidget(self.mid_divider)
        
        left_container = QtWidgets.QWidget()
        left_container.setLayout(left_layout_h)
        
        splitter_chat = ChatSplitter(QtCore.Qt.Vertical)
        splitter_chat.addWidget(self.view)
        
        # Input container with toolbar
        input_container = QtWidgets.QWidget()
        input_layout = QtWidgets.QVBoxLayout(input_container)
        try:
            input_layout.setContentsMargins(0, 0, 0, 0)
            input_layout.setSpacing(0)
        except Exception:
            pass
            
        input_toolbar = QtWidgets.QWidget()
        try:
            input_toolbar.setStyleSheet("background-color: #f5f5f5; border-top: 1px solid #e0e0e0;")
        except Exception:
            pass
        tb_layout = QtWidgets.QHBoxLayout(input_toolbar)
        try:
            tb_layout.setContentsMargins(4, 2, 4, 2)
            tb_layout.setSpacing(8)
        except Exception:
            pass
            
        self.btn_emoji = QtWidgets.QPushButton()
        try:
            epath = os.path.join(os.getcwd(), "icons", "ui", "emoji.png")
            self.btn_emoji.setIcon(QtGui.QIcon(epath))
            self.btn_emoji.setIconSize(QtCore.QSize(20, 20))
            self.btn_emoji.setFlat(True)
            self.btn_emoji.setFocusPolicy(QtCore.Qt.NoFocus)
            self.btn_emoji.setStyleSheet("QPushButton{border:none;background:transparent;}QPushButton:hover{background:transparent;}QPushButton:pressed{background:transparent;}")
            self.btn_emoji.setCursor(QtCore.Qt.PointingHandCursor)
            self.btn_emoji.setFixedSize(28, 28)
            self.btn_emoji.clicked.connect(self.show_emoji_picker)
        except Exception:
            pass
            
        self.btn_screenshot = QtWidgets.QPushButton()
        try:
            spath = os.path.join(os.getcwd(), "icons", "ui", "screenshot.png")
            self.btn_screenshot.setIcon(QtGui.QIcon(spath))
            self.btn_screenshot.setIconSize(QtCore.QSize(20, 20))
            self.btn_screenshot.setFlat(True)
            self.btn_screenshot.setFocusPolicy(QtCore.Qt.NoFocus)
            self.btn_screenshot.setStyleSheet("QPushButton{border:none;background:transparent;}QPushButton:hover{background:transparent;}QPushButton:pressed{background:transparent;}")
            self.btn_screenshot.setCursor(QtCore.Qt.PointingHandCursor)
            self.btn_screenshot.setFixedSize(28, 28)
            self.btn_screenshot.clicked.connect(self.do_screenshot)
        except Exception:
            pass
            
        tb_layout.addWidget(self.btn_emoji)
        tb_layout.addWidget(self.btn_screenshot)
        tb_layout.addStretch(1)
        
        input_layout.addWidget(input_toolbar)
        input_layout.addWidget(self.entry)
        
        splitter_chat.addWidget(input_container)
        splitter_chat.setStretchFactor(0, 1)
        splitter_chat.setStretchFactor(1, 0)
        splitter_chat.setHandleWidth(1)
        try:
            splitter_chat.setStyleSheet("QSplitter::handle{background:#e0e0e0;}")
        except Exception:
            pass

        self.real_chat_widget = QtWidgets.QWidget()
        real_chat_layout = QtWidgets.QVBoxLayout()
        try:
            real_chat_layout.setContentsMargins(0, 0, 0, 0)
            real_chat_layout.setSpacing(0)
        except Exception:
            pass
        self.find_bar = QtWidgets.QLineEdit()
        self.find_bar.setPlaceholderText("搜索聊天记录，回车定位")
        try:
            self.find_bar.setClearButtonEnabled(True)
            self.find_bar.setVisible(False)
            self.find_bar.setFixedHeight(28)
            self.find_bar.setStyleSheet("QLineEdit{padding:4px 8px;border:none;border-bottom:1px solid #e0e0e0;}")
        except Exception:
            pass
        self.find_bar.returnPressed.connect(self._perform_find)
        try:
            self.find_bar_escape = QtGui.QShortcut(QtGui.QKeySequence(QtCore.Qt.Key_Escape), self.find_bar)
            self.find_bar_escape.activated.connect(self._hide_find_bar)
        except Exception:
            pass
        real_chat_layout.addWidget(self.find_bar, 0)
        real_chat_layout.addWidget(splitter_chat)
        self.real_chat_widget.setLayout(real_chat_layout)

        self.welcome_widget = QtWidgets.QWidget()
        welcome_layout = QtWidgets.QVBoxLayout()
        welcome_label = QtWidgets.QLabel()
        welcome_label.setAlignment(QtCore.Qt.AlignCenter)
        try:
            w_path = os.path.join(os.getcwd(), "icons", "ui", "xiaocaichat.png")
            if os.path.exists(w_path):
                w_pm = QtGui.QPixmap(w_path)
                if not w_pm.isNull():
                    w_pm = w_pm.scaled(128, 128, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
                    img = w_pm.toImage().convertToFormat(QtGui.QImage.Format_ARGB32)
                    w = img.width()
                    h = img.height()
                    for y in range(h):
                        for x in range(w):
                            c = img.pixel(x, y)
                            gray = int(0.299 * ((c >> 16) & 0xFF) + 0.587 * ((c >> 8) & 0xFF) + 0.114 * (c & 0xFF))
                            alpha = (c >> 24) & 0xFF
                            img.setPixel(x, y, (alpha << 24) | (gray << 16) | (gray << 8) | gray)
                    w_pm = QtGui.QPixmap.fromImage(img)
                    welcome_label.setPixmap(w_pm)
        except Exception:
            pass
        welcome_layout.addWidget(welcome_label)
        self.welcome_widget.setLayout(welcome_layout)

        self.chat_stack = QtWidgets.QStackedWidget()
        self.chat_stack.addWidget(self.welcome_widget)
        self.chat_stack.addWidget(self.real_chat_widget)

        self.settings_container = self._make_settings_panel()
        self.right_stack = QtWidgets.QStackedWidget()
        self.right_stack.addWidget(self.chat_stack)
        self.right_stack.addWidget(self.settings_container)
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        splitter.addWidget(left_container)
        splitter.addWidget(self.right_stack)
        try:
            splitter.setStretchFactor(0, 0)
            splitter.setStretchFactor(1, 1)
            splitter.setSizes([240, 720])
        except Exception:
            pass
        layout.addWidget(splitter, 1)
        self.status_bar = QtWidgets.QFrame()
        try:
            self.status_bar.setStyleSheet("QFrame{border-top:1px solid #e0e0e0;}")
        except Exception:
            pass
        sbot = QtWidgets.QHBoxLayout()
        try:
            sbot.setContentsMargins(8, 4, 8, 4)
            sbot.setSpacing(6)
        except Exception:
            pass
        self.status_left = QtWidgets.QLabel(f"未连接 {self.host}:{self.port}")
        self.status_right = QtWidgets.QLabel(f"XiaoCaiChat {APP_VERSION}")
        try:
            self.status_left.setStyleSheet("QLabel{color:#757575;font:12px 'Helvetica Neue';}")
            self.status_right.setStyleSheet("QLabel{color:#9e9e9e;font:12px 'Helvetica Neue';}")
        except Exception:
            pass
        sbot.addWidget(self.status_left, 0, QtCore.Qt.AlignLeft)
        sbot.addStretch(1)
        sbot.addWidget(self.status_right, 0, QtCore.Qt.AlignRight)
        self.status_bar.setLayout(sbot)
        root = QtWidgets.QVBoxLayout()
        try:
            root.setContentsMargins(0, 0, 0, 0)
            root.setSpacing(0)
        except Exception:
            pass
        root.addLayout(layout, 1)
        root.addWidget(self.status_bar, 0)
        self.setLayout(root)
        try:
            self.setMinimumSize(900, 700)
            self.resize(1120, 800)
        except Exception:
            pass

        # QTextEdit 直接回车发送，支持 Ctrl/⌘+Enter 备用快捷键
        self.conv_list.itemDoubleClicked.connect(self.on_pick_conv)
        self.conv_list.itemClicked.connect(self.on_pick_conv)
        self.view_mode = "message"
        try:
            self._sync_room_from_server()
        except Exception:
            pass
        try:
            QtCore.QTimer.singleShot(0, lambda: self._connect_all_rooms())
        except Exception:
            self._connect_all_rooms()
        self.conv_unread = {}
        self.conv_badges = {}
        self._init_conversations()
        try:
            self._update_sidebar_badge()
        except Exception:
            pass
        # 屏蔽群聊入口
        self._bootstrap_local()
        self._last_find_text = ""
        self._last_find_row = -1

    def _show_find_bar(self):
        try:
            self.find_bar.setVisible(True)
            self.find_bar.setFocus()
            if self.find_bar.text():
                self.find_bar.selectAll()
            self.view.setProperty("search_highlight_row", -1)
            self.view.setProperty("search_keyword", "")
            try:
                self.view.viewport().update()
            except Exception:
                pass
        except Exception:
            pass

    def _perform_find(self):
        try:
            kw = (self.find_bar.text() or "").strip()
            if not kw or not self.current_model:
                return
            start = 0
            if self._last_find_text == kw and self._last_find_row is not None and self._last_find_row >= 0:
                start = (self._last_find_row + 1) % len(self.current_model.items)
            found = -1
            n = len(self.current_model.items)
            for off in range(n):
                i = (start + off) % n
                it = self.current_model.items[i]
                t = (it.get("text") or "").lower()
                fn = (it.get("filename") or "").lower()
                if kw.lower() in t or (fn and kw.lower() in fn):
                    found = i
                    break
            if found != -1:
                idx = self.current_model.index(found)
                try:
                    self.view.setCurrentIndex(idx)
                    self.view.scrollTo(idx, QtWidgets.QAbstractItemView.PositionAtCenter)
                except Exception:
                    try:
                        self.view.scrollTo(idx)
                    except Exception:
                        pass
                try:
                    self.view.setProperty("search_highlight_row", found)
                    self.view.setProperty("search_keyword", kw)
                    self.view.viewport().update()
                except Exception:
                    pass
                self._last_find_text = kw
                self._last_find_row = found
            else:
                try:
                    self.view.setProperty("search_highlight_row", -1)
                    self.view.setProperty("search_keyword", "")
                    self.view.viewport().update()
                except Exception:
                    pass
        except Exception:
            pass

    def _hide_find_bar(self):
        try:
            self.find_bar.clear()
            self.find_bar.setVisible(False)
            self._last_find_text = ""
            self._last_find_row = -1
            self.view.setProperty("search_highlight_row", -1)
            self.view.setProperty("search_keyword", "")
            try:
                self.view.viewport().update()
            except Exception:
                pass
        except Exception:
            pass

    def _connect(self) -> bool:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.settimeout(2.0)
        except Exception:
            pass
        try:
            s.connect((self.host, self.port))
        except Exception:
            self.sock = None
            return False
        try:
            s.settimeout(None)
        except Exception:
            pass
        self.sock = s
        try:
            extra = (self.avatar_filename or "").strip() if isinstance(self.avatar_filename, str) else ""
            hello = f"HELLO {self.username} {self.room} {extra}\n".encode("utf-8")
            self.sock.sendall(hello)
        except Exception:
            pass
        self.rx = Receiver(self.sock)
        self.rx.received.connect(self.on_received)
        self.rx.start()
        try:
            if hasattr(self, 'status_left') and self.status_left:
                self.status_left.setText(f"已连接 {self.host}:{self.port}")
        except Exception:
            pass
        # 初始化一个空模型，等待选择私聊
        self.current_conv = None
        self.current_model = ChatModel()
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
        # 不自动请求未读，避免清空本地后服务端推送历史
        if self.avatar_filename:
            try:
                path = os.path.join(os.getcwd(), "icons", "user", self.avatar_filename)
                if os.path.isfile(path):
                    mime = self._guess_mime(path)
                    with open(path, "rb") as f:
                        b64 = base64.b64encode(f.read()).decode("ascii")
                    self._send_seq(f"AVATAR_UPLOAD {self.avatar_filename} {mime} {b64}")
                else:
                    self._send_seq(f"AVATAR {self.avatar_filename}")
            except Exception:
                pass
        try:
            _save_profile(log_dir, self.username, self.avatar_filename)
        except Exception:
            pass
        return True

    def _connect_room(self, rid: str) -> bool:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.settimeout(2.0)
        except Exception:
            pass
        try:
            s.connect((self.host, self.port))
        except Exception:
            return False
        try:
            s.settimeout(None)
        except Exception:
            pass
        try:
            extra = (self.avatar_filename or "").strip() if isinstance(self.avatar_filename, str) else ""
            hello = f"HELLO {self.username} {rid} {extra}\n".encode("utf-8")
            s.sendall(hello)
        except Exception:
            pass
        rx = Receiver(s)
        try:
            rx.received.connect(lambda t, _rid=rid: self.on_received_room(_rid, t))
        except Exception:
            rx.received.connect(self.on_received)
        rx.start()
        self.socks[rid] = s
        self.receivers[rid] = rx
        try:
            if hasattr(self, 'status_left') and self.status_left:
                self.status_left.setText(f"已连接 {self.host}:{self.port}")
        except Exception:
            pass
        try:
            self._send_seq("HIST GROUP 50", rid)
        except Exception:
            pass
        return True

    def _connect_all_rooms(self) -> bool:
        try:
            if not self.rooms_info:
                self._sync_room_from_server()
            rooms = self.rooms_info or []
            if not rooms:
                ok = self._connect()
                if not ok:
                    try:
                        self._show_connect_error(self.host, self.port, "网络或服务器不可用")
                    except Exception:
                        pass
                return ok
            # init view model once
            self.current_conv = None
            self.current_model = ChatModel()
            self.view.setModel(self.current_model)
            # heartbeat timer
            self.hb = QtCore.QTimer(self)
            self.hb.setInterval(30000)
            self.hb.timeout.connect(self._send_ping)
            self.hb.start()
            for r in rooms:
                rid = str(r.get("id"))
                if not self._connect_room(rid):
                    try:
                        self._show_connect_error(self.host, self.port, f"房间 {rid} 连接失败")
                    except Exception:
                        pass
            return True
        except Exception:
            return False
    def _show_connect_error(self, host: str, port: int, detail: str = ""):
        try:
            m = QtWidgets.QMessageBox(self)
            m.setIcon(QtWidgets.QMessageBox.Critical)
            m.setWindowTitle("连接失败")
            m.setText(f"无法连接到服务器 {host}:{port}")
            if detail:
                m.setInformativeText(str(detail))
            m.setStandardButtons(QtWidgets.QMessageBox.Retry | QtWidgets.QMessageBox.Close)
            def _done(code: int):
                try:
                    if code == QtWidgets.QMessageBox.Retry:
                        QtCore.QTimer.singleShot(0, lambda: self._connect_all_rooms())
                except Exception:
                    pass
            m.finished.connect(_done)
            m.setModal(False)
            m.open()
        except Exception:
            pass
        try:
            if hasattr(self, 'status_left') and self.status_left:
                self.status_left.setText(f"未连接 {host}:{port}")
        except Exception:
            pass

    def on_received(self, text: str):
        self.logger.write("recv", self.host, text)
        if text.startswith("PONG "):
            return
        if text.startswith("[ACK] "):
            return
            return
        if text.startswith("[SYS] "):
            parts = text.split()
            if len(parts) >= 4 and parts[1] == "JOIN":
                room = parts[2]
                user = parts[3]
                if room == self.room:
                    if user != self.username:
                        if len(parts) >= 5 and parts[4]:
                            try:
                                self._set_peer_avatar(user, parts[4])
                            except Exception:
                                pass
                            try:
                                if user not in self.peer_avatars:
                                    self._send_seq(f"AVATAR_REQ {user}")
                            except Exception:
                                pass
                        try:
                            if self.view_mode == "message":
                                self._set_online(user, True)
                                self._add_conv_dm(user)
                                try:
                                    self._rebuild_conv_list()
                                except Exception:
                                    pass
                            else:
                                self._set_online(user, True)
                                self.pending_join_users.add(user)
                        except Exception:
                            pass
                    self.view.scrollToBottom()
                return
            if len(parts) >= 7 and parts[1] == "FILE_LINK":
                room = parts[2]
                sender = parts[3]
                url = parts[-1]
                try:
                    size = int(parts[-2])
                except Exception:
                    size = 0
                filename = " ".join(parts[4:-2]) if len(parts) > 6 else ""
                try:
                    if sender == self.username:
                        return
                    key = f"group:{room}"
                    self._ensure_conv(key)
                    av = self.peer_avatars.get(sender)
                    self.conv_models[key].add_link(sender, filename, url, False, av, None, size)
                    try:
                        self.store.add(key, sender, f"[LINK] {filename} {size} {url}", "file", False)
                    except Exception:
                        pass
                    is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
                    if self.current_conv != key or is_inactive:
                        self._inc_unread(key)
                    if self.current_conv == key:
                        try:
                            self.view.scrollToBottom()
                        except Exception:
                            pass
                except Exception:
                    pass
                return
            if len(parts) >= 2 and parts[1] == "DISCONNECT":
                try:
                    m = self.conv_models.get(self.current_conv) if self.current_conv else None
                    if m:
                        m.add("sys", "", "服务器连接已断开", False, None)
                        self.view.scrollToBottom()
                    if hasattr(self, 'status_left') and self.status_left:
                        try:
                            self.status_left.setText(f"已断开 {self.host}:{self.port}")
                        except Exception:
                            pass
                except Exception:
                    pass
                return
            if len(parts) >= 4 and parts[1] == "LEAVE":
                room = parts[2]
                user = parts[3]
                if room == self.room:
                    if user != self.username:
                        self._set_online(user, False)
                    self.view.scrollToBottom()
                return
            if len(parts) >= 4 and parts[1] == "USERS":
                room = parts[2]
                users_csv = " ".join(parts[3:])
                if room == self.room:
                    users = [x for x in users_csv.split(",") if x]
                    current_peers = set()
                    for u in users:
                        uname = u
                        avatar = None
                        if ":" in u:
                            uname, avatar = u.split(":",1)
                        if uname != self.username:
                            if avatar:
                                self._set_peer_avatar(uname, avatar)
                            else:
                                try:
                                    self._try_local_peer_avatar(uname)
                                except Exception:
                                    pass
                            try:
                                if self.view_mode == "message":
                                    self._set_online(uname, True)
                                    self._add_conv_dm(uname)
                                    try:
                                        self._rebuild_conv_list()
                                    except Exception:
                                        pass
                                else:
                                    self._set_online(uname, True)
                                    self.pending_join_users.add(uname)
                            except Exception:
                                pass
                            current_peers.add(uname)
                    for key in list(self.conv_models.keys()):
                        if key.startswith("dm:"):
                            name = key.split(":",1)[1]
                            if name != self.username and name not in current_peers:
                                self._set_online(name, False)
                return
            if len(parts) >= 4 and parts[1] == "ROOM_NAME":
                room = parts[2]
                name = " ".join(parts[3:])
                if name:
                    try:
                        self.room_name_map[room] = name
                    except Exception:
                        pass
                    if room == self.room:
                        try:
                            self.room_name = name
                            self.room_ready = True
                            if self.view_mode == "group":
                                try:
                                    self._ensure_group_items()
                                except Exception:
                                    pass
                            try:
                                self._update_conv_title(f"group:{self.room}")
                            except Exception:
                                pass
                        except Exception:
                            pass
                return
            if len(parts) >= 5 and parts[1] == "AVATAR":
                room = parts[2]
                user = parts[3]
                filename = parts[4]
                if room == self.room and user != self.username:
                    self._set_peer_avatar(user, filename)
                    try:
                        if user not in self.peer_avatars:
                            self._send_seq(f"AVATAR_REQ {user}")
                    except Exception:
                        pass
                return
            if len(parts) >= 6 and parts[1] == "AVATAR_DATA":
                room = parts[2]
                user = parts[3]
                filename = parts[4]
                mime = parts[5]
                b64 = " ".join(parts[6:]) if len(parts) > 6 else ""
                if room == self.room and user != self.username and filename and b64:
                    p = self._save_peer_avatar_file(user, filename, mime, b64)
                    if p:
                        try:
                            self._set_peer_avatar(user, filename)
                        except Exception:
                            pass
                return
            if len(parts) >= 6 and parts[1] == "HISTORY":
                kind = parts[2]
                if kind == "GROUP":
                    return
                if kind == "DM":
                    peer = parts[3]
                    sender = parts[4]
                    ts = parts[5]
                    payload = " ".join(parts[6:]) if len(parts) > 6 else ""
                    self._ensure_conv(f"dm:{peer}")
                    if payload.startswith("[FILE] "):
                        fn, mime, b64 = self._parse_file(payload)
                        if self._is_deleted(f"dm:{peer}", "file", fn, mime):
                            return
                        
                        pix = self._pix_from_b64(mime, b64)
                        self._save_attachment(fn, b64, f"dm:{peer}")
                        av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                        try:
                            sz = len(base64.b64decode(b64))
                        except Exception:
                            sz = None
                        self.conv_models[f"dm:{peer}"].add_file(sender, fn, mime, pix, sender == self.username, av, int(ts) if ts else None, sz)
                    elif payload.startswith("file://"):
                        local_path = QtCore.QUrl(payload).toLocalFile()
                        try:
                            self._add_file_from_path(f"dm:{peer}", sender, local_path, sender == self.username)
                        except Exception:
                            pass
                    else:
                        payload_clean = self._sanitize_text(payload)
                        if self._is_deleted(f"dm:{peer}", "msg", payload_clean, None):
                            return
                        if payload_clean:
                            av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                            self.conv_models[f"dm:{peer}"].add("msg", sender, payload_clean, sender == self.username, av, int(ts) if ts else None)
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
                    try:
                        name = key.split(":",1)[1]
                        if self.view_mode == "message":
                            self._add_conv_dm(name)
                            self._apply_conv_filter()
                        else:
                            self.pending_dm_users.add(name)
                    except Exception:
                        pass
                self._set_unread(key, cnt)
                return
        if text.startswith("[DM] "):
            parts = text.split(" ", 3)
            if len(parts) >= 4 and parts[1] == "FROM":
                name = parts[2]
                msg = parts[3]
                if msg.startswith("[FILE] "):
                    fn, mime, b64 = self._parse_file(msg)
                    if self._is_deleted(f"dm:{name}", "file", fn, mime):
                        return
                    
                    pix = self._pix_from_b64(mime, b64)
                    self._save_attachment(fn, b64, f"dm:{name}")
                    self._ensure_conv(f"dm:{name}")
                    av = self.peer_avatars.get(name)
                    try:
                        sz = len(base64.b64decode(b64))
                    except Exception:
                        sz = None
                    self.conv_models[f"dm:{name}"].add_file(name, fn, mime, pix, False, av, None, sz)
                    self.store.add(f"dm:{name}", name, f"[FILE] {fn} {mime}", "file", False)
                elif msg.startswith("file://"):
                    local_path = QtCore.QUrl(msg).toLocalFile()
                    try:
                        self._add_file_from_path(f"dm:{name}", name, local_path, False)
                    except Exception:
                        pass
                elif msg.startswith("FILE_BEGIN "):
                    toks = msg.split(" ")
                    if len(toks) >= 4:
                         fn = " ".join(toks[1:-2])
                         mime = toks[-2]
                         try:
                             tot = int(toks[-1])
                         except:
                             tot = 0
                         self._rx_file_begin(f"dm:{name}", name, fn, mime, tot, "")
                    return
                elif msg.startswith("FILE_CHUNK "):
                    toks = msg.split(" ")
                    if len(toks) >= 4:
                        try:
                            offset = int(toks[-2])
                            fn = " ".join(toks[1:-2])
                            b64 = toks[-1]
                            self._rx_file_chunk(f"dm:{name}", name, fn, offset, b64)
                        except:
                            pass
                    return
                elif msg.startswith("FILE_END"):
                    toks = msg.split(" ")
                    fn = ""
                    if len(toks) >= 2 and toks[1].startswith("name="):
                        fn = msg[len("FILE_END name="):]
                    self._rx_file_end(f"dm:{name}", name, fn)
                    
                    is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
                    if self.current_conv != f"dm:{name}" or is_inactive:
                        self._send_macos_notification(name, "[文件]")
                    return
                else:
                    msg_clean = self._sanitize_text(msg)
                    try:
                        ks = [k for k in self._rx_files.keys() if k[0] == f"dm:{name}" and k[1] == name]
                        if ks and msg_clean:
                            fn = ks[-1][2]
                            total = int(self._rx_files.get(ks[-1], {}).get("total") or 0)
                            size_str = self._human_readable_size(total) if total > 0 else None
                            if msg_clean.strip() in {fn.strip(), (size_str or "").strip(), (fn + "\n" + (size_str or "")).strip()}:
                                return
                    except Exception:
                        pass
                    if self._is_deleted(f"dm:{name}", "msg", msg_clean, None):
                        return
                    if msg_clean:
                        self._ensure_conv(f"dm:{name}")
                        av = self.peer_avatars.get(name)
                        self.conv_models[f"dm:{name}"].add("msg", name, msg_clean, False, av)
                        self.store.add(f"dm:{name}", name, msg_clean, "msg", False)
                self.view.scrollToBottom()
                try:
                    if self.view_mode == "message":
                        self._add_conv_dm(name)
                        self._apply_conv_filter()
                    else:
                        self.pending_dm_users.add(name)
                except Exception:
                    pass
                
                # Check if app is inactive/minimized, force increment unread even if current_conv matches
                is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
                if self.current_conv != f"dm:{name}" or is_inactive:
                    self._inc_unread(f"dm:{name}")
                
                if self.current_conv != f"dm:{name}" or is_inactive:
                    try:
                        note_text = self._sanitize_text(msg)
                        if msg.startswith("[FILE] "):
                            note_text = "[文件]"
                            try:
                                _, mime, _ = self._parse_file(msg)
                                if mime.lower().startswith("image/"):
                                    note_text = "[图片]"
                            except Exception:
                                pass
                        elif msg.startswith("file://"):
                            note_text = "[文件]"
                        self._send_macos_notification(name, note_text)
                    except Exception:
                        pass
                return
            if len(parts) >= 4 and parts[1] == "TO":
                target = parts[2]
                msg = parts[3]
                if target != self.username:
                    return
                if msg.startswith("[FILE] "):
                    fn, mime, b64 = self._parse_file(msg)
                    if self._is_deleted(f"dm:{target}", "file", fn, mime):
                        return
                    
                    pix = self._pix_from_b64(mime, b64)
                    self._save_attachment(fn, b64, f"dm:{target}")
                    self._ensure_conv(f"dm:{target}")
                    try:
                        sz = len(base64.b64decode(b64))
                    except Exception:
                        sz = None
                    self.conv_models[f"dm:{target}"].add_file(self.username, fn, mime, pix, True, self.avatar_pixmap, None, sz)
                    self.store.add(f"dm:{target}", self.username, f"[FILE] {fn} {mime}", "file", True)
                elif msg.startswith("file://"):
                    local_path = QtCore.QUrl(msg).toLocalFile()
                    try:
                        self._add_file_from_path(f"dm:{target}", self.username, local_path, True)
                    except Exception:
                        pass
                else:
                    msg_clean = self._sanitize_text(msg)
                    if self._is_deleted(f"dm:{target}", "msg", msg_clean, None):
                        return
                    if msg_clean:
                        self._ensure_conv(f"dm:{target}")
                        self.conv_models[f"dm:{target}"].add("msg", self.username, msg_clean, True, self.avatar_pixmap)
                        self.store.add(f"dm:{target}", self.username, msg_clean, "msg", True)
                        self.view.scrollToBottom()
                        try:
                            if self.view_mode == "message":
                                self._add_conv_dm(target)
                                self._apply_conv_filter()
                            else:
                                self.pending_dm_users.add(target)
                        except Exception:
                            pass
                    return
        if ">" in text:
            name, msg = text.split(">", 1)
            name = name.strip()
            msg = msg.strip()
            if msg.startswith("[FILE] "):
                fn, mime, b64 = self._parse_file(msg)
                if self._is_deleted(f"group:{self.room}", "file", fn, mime):
                    return
                if name == self.username:
                    try:
                        m = self.conv_models.get(f"group:{self.room}")
                        if m:
                            for i in reversed(range(len(m.items))):
                                it = m.items[i]
                                if it.get("kind") == "file" and it.get("sender") == self.username and it.get("filename") == fn:
                                    m.remove_row(i)
                                    break
                        try:
                            self.store.delete_message(f"group:{self.room}", self.username, "file", f"[FILE] {fn} {mime}", True, fn, mime)
                        except Exception:
                            pass
                    except Exception:
                        pass
                pix = self._pix_from_b64(mime, b64)
                self._save_attachment(fn, b64, f"group:{self.room}")
                self._ensure_conv(f"group:{self.room}")
                av = self.avatar_pixmap if name == self.username else self.peer_avatars.get(name)
                try:
                    sz = len(base64.b64decode(b64))
                except Exception:
                    sz = None
                self.conv_models[f"group:{self.room}"].add_file(name, fn, mime, pix, name == self.username, av, None, sz)
                self.store.add(f"group:{self.room}", name, f"[FILE] {fn} {mime}", "file", name == self.username)
            elif msg.startswith("file://"):
                local_path = QtCore.QUrl(msg).toLocalFile()
                try:
                    self._add_file_from_path(f"group:{self.room}", name, local_path, name == self.username)
                except Exception:
                    pass
            elif msg.startswith("FILE_BEGIN "):
                toks = msg.split(" ")
                if len(toks) >= 4:
                     fn = " ".join(toks[1:-2])
                     mime = toks[-2]
                     try:
                         tot = int(toks[-1])
                     except:
                         tot = 0
                     self._rx_file_begin(f"group:{self.room}", name, fn, mime, tot, "")
                return
            elif msg.startswith("FILE_CHUNK "):
                toks = msg.split(" ")
                if len(toks) >= 4:
                    try:
                        offset = int(toks[-2])
                        fn = " ".join(toks[1:-2])
                        b64 = toks[-1]
                        self._rx_file_chunk(f"group:{self.room}", name, fn, offset, b64)
                    except:
                        pass
                return
            elif msg.startswith("FILE_END"):
                toks = msg.split(" ")
                fn = ""
                if len(toks) >= 2 and toks[1].startswith("name="):
                    fn = msg[len("FILE_END name="):]
                self._rx_file_end(f"group:{self.room}", name, fn)
                
                is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
                if self.current_conv != f"group:{self.room}" or is_inactive:
                    self._send_macos_notification(f"{name} (群聊)", "[文件]")
                return
            else:
                msg_clean = self._sanitize_text(msg)
                if self._is_deleted(f"group:{self.room}", "msg", msg_clean, None):
                    return
                if msg_clean:
                    self._ensure_conv(f"group:{self.room}")
                    av = self.avatar_pixmap if name == self.username else self.peer_avatars.get(name)
                    self.conv_models[f"group:{self.room}"].add("msg", name, msg_clean, name == self.username, av)
                    self.store.add(f"group:{self.room}", name, msg_clean, "msg", name == self.username)
            self.view.scrollToBottom()
            
            # Check if app is inactive/minimized, force increment unread even if current_conv matches
            is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
            if self.current_conv != f"group:{self.room}" or is_inactive:
                self._inc_unread(f"group:{self.room}")
            
            if self.current_conv != f"group:{self.room}" or is_inactive:
                try:
                    note_text = self._sanitize_text(msg)
                    if msg.startswith("[FILE] "):
                        note_text = "[文件]"
                        try:
                            _, mime, _ = self._parse_file(msg)
                            if mime.lower().startswith("image/"):
                                note_text = "[图片]"
                        except Exception:
                            pass
                    elif msg.startswith("file://"):
                        note_text = "[文件]"
                    self._send_macos_notification(f"{name} (群聊)", note_text)
                except Exception:
                    pass

    def on_received_room(self, rid: str, text: str):
        self.logger.write("recv", self.host, text)
        if text.startswith("PONG "):
            return
        if text.startswith("[ACK] "):
            return
        if text.startswith("[SYS] "):
            parts = text.split()
            if len(parts) >= 4 and parts[1] == "JOIN":
                room = parts[2]
                user = parts[3]
                if room == rid:
                    if user != self.username:
                        if len(parts) >= 5 and parts[4]:
                            try:
                                self._set_peer_avatar(user, parts[4])
                            except Exception:
                                pass
                            try:
                                if user not in self.peer_avatars:
                                    self._send_seq(f"AVATAR_REQ {user}", rid)
                            except Exception:
                                pass
                        try:
                            if self.view_mode == "message":
                                self._set_online(user, True)
                                self._add_conv_dm(user)
                                try:
                                    self._rebuild_conv_list()
                                except Exception:
                                    pass
                            else:
                                self._set_online(user, True)
                                self.pending_join_users.add(user)
                        except Exception:
                            pass
                try:
                    self.view.scrollToBottom()
                except Exception:
                    pass
                return
            if len(parts) >= 7 and parts[1] == "FILE_LINK":
                room = parts[2]
                sender = parts[3]
                url = parts[-1]
                try:
                    size = int(parts[-2])
                except Exception:
                    size = 0
                filename = " ".join(parts[4:-2]) if len(parts) > 6 else ""
                if room == rid:
                    try:
                        if sender == self.username:
                            return
                        key = f"group:{room}"
                        self._ensure_conv(key)
                        av = self.peer_avatars.get(sender)
                        self.conv_models[key].add_link(sender, filename, url, False, av, None, size)
                        try:
                            self.store.add(key, sender, f"[LINK] {filename} {size} {url}", "file", False)
                        except Exception:
                            pass
                        is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
                        if self.current_conv != key or is_inactive:
                            self._inc_unread(key)
                        if self.current_conv == key:
                            try:
                                self.view.scrollToBottom()
                            except Exception:
                                pass
                    except Exception:
                        pass
                return
            if len(parts) >= 2 and parts[1] == "DISCONNECT":
                try:
                    key = f"group:{rid}"
                    self._ensure_conv(key)
                    m = self.conv_models.get(key)
                    if m:
                        m.add("sys", "", "服务器连接已断开", False, None)
                        self.view.scrollToBottom()
                    if hasattr(self, 'status_left') and self.status_left:
                        try:
                            self.status_left.setText(f"已断开 {self.host}:{self.port}")
                        except Exception:
                            pass
                except Exception:
                    pass
                return
            if len(parts) >= 4 and parts[1] == "LEAVE":
                room = parts[2]
                user = parts[3]
                if room == rid:
                    if user != self.username:
                        self._set_online(user, False)
                    try:
                        self.view.scrollToBottom()
                    except Exception:
                        pass
                return
            if len(parts) >= 4 and parts[1] == "USERS":
                room = parts[2]
                users_csv = " ".join(parts[3:])
                if room == rid:
                    users = [x for x in users_csv.split(",") if x]
                    current_peers = set()
                    for u in users:
                        uname = u
                        avatar = None
                        if ":" in u:
                            uname, avatar = u.split(":",1)
                        if uname != self.username:
                            if avatar:
                                self._set_peer_avatar(uname, avatar)
                            else:
                                try:
                                    self._try_local_peer_avatar(uname)
                                except Exception:
                                    pass
                            try:
                                if self.view_mode == "message":
                                    self._set_online(uname, True)
                                    self._add_conv_dm(uname)
                                    try:
                                        self._rebuild_conv_list()
                                    except Exception:
                                        pass
                                else:
                                    self._set_online(uname, True)
                                    self.pending_join_users.add(uname)
                            except Exception:
                                pass
                            current_peers.add(uname)
                    for key in list(self.conv_models.keys()):
                        if key.startswith("dm:"):
                            name = key.split(":",1)[1]
                            if name != self.username and name not in current_peers:
                                self._set_online(name, False)
                return
            if len(parts) >= 4 and parts[1] == "ROOM_NAME":
                room = parts[2]
                name = " ".join(parts[3:])
                if name:
                    try:
                        self.room_name_map[room] = name
                    except Exception:
                        pass
                    try:
                        self._update_conv_title(f"group:{room}")
                    except Exception:
                        pass
                    if room == self.room:
                        try:
                            self.room_name = name
                            self.room_ready = True
                            if self.view_mode == "group":
                                try:
                                    self._ensure_group_items()
                                except Exception:
                                    pass
                            try:
                                self._update_conv_title(f"group:{self.room}")
                            except Exception:
                                pass
                        except Exception:
                            pass
                return
            if len(parts) >= 5 and parts[1] == "AVATAR":
                room = parts[2]
                user = parts[3]
                filename = parts[4]
                if room == rid and user != self.username:
                    self._set_peer_avatar(user, filename)
                    try:
                        if user not in self.peer_avatars:
                            self._send_seq(f"AVATAR_REQ {user}", rid)
                    except Exception:
                        pass
                return
            if len(parts) >= 6 and parts[1] == "AVATAR_DATA":
                room = parts[2]
                user = parts[3]
                filename = parts[4]
                mime = parts[5]
                b64 = " ".join(parts[6:]) if len(parts) > 6 else ""
                if room == rid and user != self.username and filename and b64:
                    p = self._save_peer_avatar_file(user, filename, mime, b64)
                    if p:
                        try:
                            self._set_peer_avatar(user, filename)
                        except Exception:
                            pass
                return
            if len(parts) >= 6 and parts[1] == "HISTORY":
                kind = parts[2]
                if kind == "GROUP":
                    return
                if kind == "DM":
                    peer = parts[3]
                    sender = parts[4]
                    ts = parts[5]
                    payload = " ".join(parts[6:]) if len(parts) > 6 else ""
                    self._ensure_conv(f"dm:{peer}")
                    if payload.startswith("[FILE] "):
                        fn, mime, b64 = self._parse_file(payload)
                        if self._is_deleted(f"dm:{peer}", "file", fn, mime):
                            return
                        pix = self._pix_from_b64(mime, b64)
                        self._save_attachment(fn, b64, f"dm:{peer}")
                        av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                        try:
                            sz = len(base64.b64decode(b64))
                        except Exception:
                            sz = None
                        self.conv_models[f"dm:{peer}"].add_file(sender, fn, mime, pix, sender == self.username, av, int(ts) if ts else None, sz)
                    elif payload.startswith("file://"):
                        local_path = QtCore.QUrl(payload).toLocalFile()
                        try:
                            self._add_file_from_path(f"dm:{peer}", sender, local_path, sender == self.username)
                        except Exception:
                            pass
                    else:
                        payload_clean = self._sanitize_text(payload)
                        if self._is_deleted(f"dm:{peer}", "msg", payload_clean, None):
                            return
                        if payload_clean:
                            av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                            self.conv_models[f"dm:{peer}"].add("msg", sender, payload_clean, sender == self.username, av, int(ts) if ts else None)
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
                    try:
                        name = key.split(":",1)[1]
                        if self.view_mode == "message":
                            self._add_conv_dm(name)
                            self._apply_conv_filter()
                        else:
                            self.pending_dm_users.add(name)
                    except Exception:
                        pass
                self._set_unread(key, cnt)
                return
        if text.startswith("[DM] "):
            parts = text.split(" ", 3)
            if len(parts) >= 4 and parts[1] == "FROM":
                name = parts[2]
                msg = parts[3]
                if msg.startswith("FILE_META "):
                    toks = msg.split(" ")
                    if len(toks) >= 5:
                        try:
                            md5 = toks[-1]
                            tot = int(toks[-2])
                        except Exception:
                            md5 = ""
                            try:
                                tot = int(toks[-2]) if len(toks) >= 2 else 0
                            except Exception:
                                tot = 0
                        mime = toks[-3] if len(toks) >= 3 else "application/octet-stream"
                        fn = " ".join(toks[1:-3]) if len(toks) > 4 else (toks[1] if len(toks) > 1 else "")
                        try:
                            self.logger.write("recv", name, f"FILE_META name={fn} mime={mime} size={tot} md5={md5}")
                        except Exception:
                            pass
                        have_path = self._attachment_path(fn, f"dm:{name}")
                        if os.path.isfile(have_path):
                            try:
                                self._send_seq(f"DM {name} FILE_HAVE {md5} {tot} COMPLETE")
                            except Exception:
                                pass
                        else:
                            att_dir = self._attachment_dir(f"dm:{name}")
                            part = os.path.join(att_dir, fn + ".part")
                            try:
                                k = (f"dm:{name}", name, fn)
                                if os.path.isfile(part) and k not in self._rx_files:
                                    try:
                                        os.remove(part)
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                            try:
                                if k not in self._rx_files:
                                    self._rx_files[k] = {"mime": mime, "total": int(max(0, tot)), "part": part, "chunks": {}, "md5": md5}
                                else:
                                    self._rx_files[k]["mime"] = mime
                                    self._rx_files[k]["total"] = int(max(0, tot))
                                    self._rx_files[k]["part"] = part
                                    self._rx_files[k]["md5"] = md5
                            except Exception:
                                pass
                            try:
                                written = os.path.getsize(part) if os.path.isfile(part) else 0
                            except Exception:
                                written = 0
                            try:
                                if md5:
                                    self._send_seq(f"DM {name} FILE_HAVE {md5} {written} PARTIAL")
                            except Exception:
                                pass
                        return
                if msg.startswith("FILE_QUERY "):
                    toks = msg.split(" ", 2)
                    try:
                        md5 = toks[1]
                    except Exception:
                        md5 = ""
                    try:
                        self.logger.write("recv", name, f"FILE_QUERY md5={md5}")
                    except Exception:
                        pass
                    try:
                        self._send_seq(f"DM {name} FILE_HAVE {md5} 0 PARTIAL")
                    except Exception:
                        pass
                    return
                if msg.startswith("FILE_HAVE "):
                    return
                if msg.startswith("[FILE] "):
                    fn, mime, b64 = self._parse_file(msg)
                    if self._is_deleted(f"dm:{name}", "file", fn, mime):
                        return
                    pix = self._pix_from_b64(mime, b64)
                    try:
                        self._ensure_conv(f"dm:{name}")
                        m = self.conv_models.get(f"dm:{name}")
                        exists = False
                        if m:
                            for it in m.items:
                                if it.get("kind") == "file" and it.get("filename") == fn and it.get("sender") == name:
                                    exists = True
                                    break
                        if not exists:
                            if mime and mime.lower().startswith("image/"):
                                self._save_attachment(fn, b64, f"dm:{name}")
                            else:
                                self._save_attachment_async(fn, b64, f"dm:{name}")
                            av = self.peer_avatars.get(name)
                            try:
                                sz = len(base64.b64decode(b64))
                            except Exception:
                                sz = None
                            self.conv_models[f"dm:{name}"].add_file(name, fn, mime, pix, False, av, None, sz)
                            self.store.add(f"dm:{name}", name, f"[FILE] {fn} {mime}", "file", False)
                    except Exception:
                        pass
                elif msg.startswith("FILE_BEGIN "):
                    toks = msg.split(" ")
                    if len(toks) >= 4:
                        try:
                            tot = int(toks[-1])
                        except Exception:
                            tot = 0
                        mime = toks[-2] if len(toks) >= 2 else "application/octet-stream"
                        fn = " ".join(toks[1:-2]) if len(toks) > 3 else (toks[1] if len(toks) > 1 else "")
                        try:
                            self.logger.write("recv", name, f"FILE_BEGIN name={fn} mime={mime} size={tot}")
                        except Exception:
                            pass
                        self._rx_file_begin(f"dm:{name}", name, fn, mime, tot)
                        return
                elif msg.startswith("FILE_CHUNK "):
                    toks = msg.split(" ", 2)
                    if len(toks) >= 3:
                        try:
                            off = int(toks[1])
                        except Exception:
                            off = 0
                        b64 = toks[2]
                        try:
                            self.logger.write("recv", name, f"FILE_CHUNK off={off} len={len(b64)}")
                        except Exception:
                            pass
                        key = None
                        try:
                            ks = [k for k in self._rx_files.keys() if k[0] == f"dm:{name}" and k[1] == name]
                            if ks:
                                key = ks[-1]
                        except Exception:
                            key = None
                        if key:
                            fn = key[2]
                            try:
                                self.logger.write("recv", name, f"FILE_CHUNK apply name={fn} off={off}")
                            except Exception:
                                pass
                            self._rx_file_chunk(f"dm:{name}", name, fn, off, b64)
                        return
                elif msg.startswith("FILE_END"):
                    key = None
                    try:
                        ks = [k for k in self._rx_files.keys() if k[0] == f"dm:{name}" and k[1] == name]
                        if ks:
                            key = ks[-1]
                    except Exception:
                        key = None
                    if key:
                        fn = key[2]
                        try:
                            self.logger.write("recv", name, f"FILE_END name={fn}")
                        except Exception:
                            pass
                        self._rx_file_end(f"dm:{name}", name, fn)
                        is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
                        if self.current_conv != f"dm:{name}" or is_inactive:
                            self._inc_unread(f"dm:{name}")
                            try:
                                self._send_macos_notification(name, "[文件]")
                            except Exception:
                                pass
                        return
                elif msg.startswith("FILE_CANCEL "):
                    toks = msg.split(" ", 1)
                    fn = toks[1] if len(toks) >= 2 else ""
                    try:
                        att_dir = self._attachment_dir(f"dm:{name}")
                        part = os.path.join(att_dir, fn + ".part")
                        if os.path.isfile(part):
                            try:
                                os.remove(part)
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        k = (f"dm:{name}", name, fn)
                        if k in self._rx_files:
                            del self._rx_files[k]
                    except Exception:
                        pass
                    return
                    try:
                        k = (f"dm:{name}", name, fn)
                        if k in self._rx_files:
                            del self._rx_files[k]
                    except Exception:
                        pass
                    return
                elif msg.startswith("file://"):
                    local_path = QtCore.QUrl(msg).toLocalFile()
                    try:
                        self._add_file_from_path(f"dm:{name}", name, local_path, False)
                    except Exception:
                        pass
                else:
                    msg_clean = self._sanitize_text(msg)
                    if self._is_deleted(f"dm:{name}", "msg", msg_clean, None):
                        return
                    if msg_clean:
                        self._ensure_conv(f"dm:{name}")
                        av = self.peer_avatars.get(name)
                        self.conv_models[f"dm:{name}"].add("msg", name, msg_clean, False, av)
                        self.store.add(f"dm:{name}", name, msg_clean, "msg", False)
                self.view.scrollToBottom()
                try:
                    if self.view_mode == "message":
                        self._add_conv_dm(name)
                        self._apply_conv_filter()
                    else:
                        self.pending_dm_users.add(name)
                except Exception:
                    pass
                is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
                if self.current_conv != f"dm:{name}" or is_inactive:
                    self._inc_unread(f"dm:{name}")
                if self.current_conv != f"dm:{name}" or is_inactive:
                    try:
                        note_text = self._sanitize_text(msg)
                        if msg.startswith("[FILE] "):
                            note_text = "[文件]"
                            try:
                                _, mime, _ = self._parse_file(msg)
                                if mime.lower().startswith("image/"):
                                    note_text = "[图片]"
                            except Exception:
                                pass
                        elif msg.startswith("file://"):
                            note_text = "[文件]"
                        self._send_macos_notification(name, note_text)
                    except Exception:
                        pass
                return
            if len(parts) >= 4 and parts[1] == "TO":
                target = parts[2]
                msg = parts[3]
                if target != self.username:
                    return
                if msg.startswith("FILE_QUERY "):
                    return
                if msg.startswith("FILE_ACK "):
                    toks = msg.split(" ", 4)
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
                        try:
                            for (_, _), w in list(self.upload_workers.items()):
                                if hasattr(w, "_md5") and w._md5 == md5:
                                    try:
                                        if hasattr(w, "note_ack"):
                                            w.note_ack(off, wrote)
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                    return
                if msg.startswith("FILE_HAVE "):
                    toks = msg.split(" ", 4)
                    if len(toks) >= 4:
                        md5 = toks[1]
                        try:
                            written = int(toks[2])
                        except Exception:
                            written = 0
                        status = toks[3] if len(toks) >= 4 else "PARTIAL"
                        try:
                            for (key,row), w in list(self.upload_workers.items()):
                                if hasattr(w, "_md5") and w._md5 == md5:
                                    if status == "COMPLETE":
                                        try:
                                            w.cancel()
                                        except Exception:
                                            pass
                                        m = self.conv_models.get(key)
                                        if m:
                                            try:
                                                it = m.items[row]
                                                tot = int(it.get("filesize") or 0)
                                                m.set_upload_progress(row, tot, tot, None)
                                            except Exception:
                                                pass
                                    else:
                                        try:
                                            w.set_resume_written(written)
                                        except Exception:
                                            pass
                        except Exception:
                            pass
                    return
                # 其余 TO 消息不在此分支入会话，避免与 FROM 分支重复
                return
                return
        if ">" in text:
            name, msg = text.split(">", 1)
            name = name.strip()
            msg = msg.strip()
            rid_key = f"group:{rid}"
            if msg.startswith("[FILE] "):
                fn, mime, b64 = self._parse_file(msg)
                if self._is_deleted(rid_key, "file", fn, mime):
                    return
                pix = self._pix_from_b64(mime, b64)
                try:
                    self._ensure_conv(rid_key)
                    m = self.conv_models.get(rid_key)
                    exists = False
                    if m:
                        for it in m.items:
                            if it.get("kind") == "file" and it.get("filename") == fn and it.get("sender") == name:
                                exists = True
                                break
                    if not exists:
                        if mime and mime.lower().startswith("image/"):
                            self._save_attachment(fn, b64, rid_key)
                        else:
                            self._save_attachment_async(fn, b64, rid_key)
                        av = self.avatar_pixmap if name == self.username else self.peer_avatars.get(name)
                        try:
                            sz = len(base64.b64decode(b64))
                        except Exception:
                            sz = None
                        self.conv_models[rid_key].add_file(name, fn, mime, pix, name == self.username, av, None, sz)
                        self.store.add(rid_key, name, f"[FILE] {fn} {mime}", "file", name == self.username)
                except Exception:
                    pass
            elif msg.startswith("FILE_META "):
                toks = msg.split(" ")
                if len(toks) >= 5:
                    try:
                        md5 = toks[-1]
                        tot = int(toks[-2])
                    except Exception:
                        md5 = ""
                        try:
                            tot = int(toks[-2]) if len(toks) >= 2 else 0
                        except Exception:
                            tot = 0
                    mime = toks[-3] if len(toks) >= 3 else "application/octet-stream"
                    fn = " ".join(toks[1:-3]) if len(toks) > 4 else (toks[1] if len(toks) > 1 else "")
                    try:
                        att_dir = self._attachment_dir(rid_key)
                        part = os.path.join(att_dir, fn + ".part")
                        k = (rid_key, name, fn)
                        if os.path.isfile(part) and k not in self._rx_files:
                            try:
                                os.remove(part)
                            except Exception:
                                pass
                        if k not in self._rx_files:
                            self._rx_files[k] = {"mime": mime, "total": int(max(0, tot)), "part": part, "chunks": {}, "md5": md5}
                        else:
                            self._rx_files[k]["mime"] = mime
                            self._rx_files[k]["total"] = int(max(0, tot))
                            self._rx_files[k]["part"] = part
                            self._rx_files[k]["md5"] = md5
                    except Exception:
                        pass
            elif msg.startswith("FILE_QUERY "):
                toks = msg.split(" ", 2)
                try:
                    md5 = toks[1]
                except Exception:
                    md5 = ""
                try:
                    d_entry = None
                    for k, v in list(self._rx_files.items()):
                        if k[0] == rid_key and v.get("md5") == md5:
                            d_entry = v
                            break
                    written = 0
                    if d_entry:
                        partp = d_entry.get("part")
                        try:
                            written = os.path.getsize(partp) if partp and os.path.isfile(partp) else 0
                        except Exception:
                            written = 0
                    if md5:
                        self._send_seq(f"MSG FILE_HAVE {md5} {int(max(0, written))} PARTIAL", rid)
                except Exception:
                    pass
                return
            elif msg.startswith("FILE_BEGIN "):
                toks = msg.split(" ")
                if len(toks) >= 4:
                    try:
                        tot = int(toks[-1])
                    except Exception:
                        tot = 0
                    mime = toks[-2] if len(toks) >= 2 else "application/octet-stream"
                    fn = " ".join(toks[1:-2]) if len(toks) > 3 else (toks[1] if len(toks) > 1 else "")
                    try:
                        self.logger.write("recv", name, f"GROUP {rid_key} FILE_BEGIN name={fn} mime={mime} size={tot}")
                    except Exception:
                        pass
                    self._rx_file_begin(rid_key, name, fn, mime, tot)
                    return
            elif msg.startswith("FILE_CHUNK "):
                toks = msg.split(" ", 2)
                if len(toks) >= 3:
                    try:
                        off = int(toks[1])
                    except Exception:
                        off = 0
                    b64 = toks[2]
                    try:
                        self.logger.write("recv", name, f"GROUP {rid_key} FILE_CHUNK off={off} len={len(b64)}")
                    except Exception:
                        pass
                    key = None
                    try:
                        ks = [k for k in self._rx_files.keys() if k[0] == rid_key and k[1] == name]
                        if ks:
                            key = ks[-1]
                    except Exception:
                        key = None
                    if key:
                        fn = key[2]
                        try:
                            self.logger.write("recv", name, f"GROUP {rid_key} FILE_CHUNK apply name={fn} off={off}")
                        except Exception:
                            pass
                        self._rx_file_chunk(rid_key, name, fn, off, b64)
                    return
            elif msg.startswith("FILE_END"):
                key = None
                try:
                    ks = [k for k in self._rx_files.keys() if k[0] == rid_key and k[1] == name]
                    if ks:
                        key = ks[-1]
                except Exception:
                    key = None
                if key:
                    fn = key[2]
                    try:
                        self.logger.write("recv", name, f"GROUP {rid_key} FILE_END name={fn}")
                    except Exception:
                        pass
                    self._rx_file_end(rid_key, name, fn)
                    is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
                    if self.current_conv != rid_key or is_inactive:
                        self._inc_unread(rid_key)
                        try:
                            room_title = self.room_name_map.get(rid, rid)
                            self._send_macos_notification(f"{name} ({room_title})", "[文件]")
                        except Exception:
                            pass
                    return
            elif msg.startswith("FILE_HAVE "):
                toks = msg.split(" ", 4)
                if len(toks) >= 4:
                    md5 = toks[1]
                    try:
                        written = int(toks[2])
                    except Exception:
                        written = 0
                    status = toks[3] if len(toks) >= 4 else "PARTIAL"
                    try:
                        for (key,row), w in list(self.upload_workers.items()):
                            if key == rid_key and hasattr(w, "_md5") and w._md5 == md5:
                                if status == "COMPLETE":
                                    try:
                                        w.cancel()
                                    except Exception:
                                        pass
                                    m = self.conv_models.get(key)
                                    if m:
                                        try:
                                            it = m.items[row]
                                            tot = int(it.get("filesize") or 0)
                                            m.set_upload_progress(row, tot, tot, None)
                                        except Exception:
                                            pass
                                else:
                                    try:
                                        w.set_resume_written(written)
                                    except Exception:
                                        pass
                    except Exception:
                        pass
                return
            elif msg.startswith("FILE_ACK "):
                toks = msg.split(" ", 4)
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
                    try:
                        for (key,row), w in list(self.upload_workers.items()):
                            if key == rid_key and hasattr(w, "_md5") and w._md5 == md5:
                                try:
                                    if hasattr(w, "note_ack"):
                                        w.note_ack(off, wrote)
                                except Exception:
                                    pass
                    except Exception:
                        pass
                return
            elif msg.startswith("FILE_CANCEL "):
                toks = msg.split(" ", 1)
                fn = toks[1] if len(toks) >= 2 else ""
                try:
                    att_dir = self._attachment_dir(rid_key)
                    part = os.path.join(att_dir, fn + ".part")
                    if os.path.isfile(part):
                        try:
                            os.remove(part)
                        except Exception:
                            pass
                except Exception:
                    pass
                try:
                    k = (rid_key, name, fn)
                    if k in self._rx_files:
                        del self._rx_files[k]
                except Exception:
                    pass
                return
            elif msg.startswith("file://"):
                local_path = QtCore.QUrl(msg).toLocalFile()
                try:
                    self._add_file_from_path(rid_key, name, local_path, name == self.username)
                except Exception:
                    pass
            else:
                msg_clean = self._sanitize_text(msg)
                try:
                    ks = [k for k in self._rx_files.keys() if k[0] == rid_key and k[1] == name]
                    if ks and msg_clean:
                        fn = ks[-1][2]
                        total = int(self._rx_files.get(ks[-1], {}).get("total") or 0)
                        size_str = self._human_readable_size(total) if total > 0 else None
                        if msg_clean.strip() in {fn.strip(), (size_str or "").strip(), (fn + "\n" + (size_str or "")).strip()}:
                            return
                except Exception:
                    pass
                if self._is_deleted(rid_key, "msg", msg_clean, None):
                    return
                if msg_clean:
                    self._ensure_conv(rid_key)
                    av = self.avatar_pixmap if name == self.username else self.peer_avatars.get(name)
                    self.conv_models[rid_key].add("msg", name, msg_clean, name == self.username, av)
                    self.store.add(rid_key, name, msg_clean, "msg", name == self.username)
            self.view.scrollToBottom()
            is_inactive = not self.isActiveWindow() or self.isMinimized() or QtWidgets.QApplication.instance().applicationState() != QtCore.Qt.ApplicationActive
            if self.current_conv != rid_key or is_inactive:
                self._inc_unread(rid_key)
            if self.current_conv != rid_key or is_inactive:
                try:
                    note_text = self._sanitize_text(msg)
                    if msg.startswith("[FILE] "):
                        note_text = "[文件]"
                        try:
                            _, mime, _ = self._parse_file(msg)
                            if mime.lower().startswith("image/"):
                                note_text = "[图片]"
                        except Exception:
                            pass
                    elif msg.startswith("file://"):
                        note_text = "[文件]"
                    room_title = self.room_name_map.get(rid, rid)
                    self._send_macos_notification(f"{name} ({room_title})", note_text)
                except Exception:
                    pass

    def do_screenshot(self):
        try:
            import tempfile
            fd, path = tempfile.mkstemp(suffix=".png")
            os.close(fd)
            cmd = ["screencapture", "-i", "-x", path]
            subprocess.run(cmd, check=False)
            if os.path.exists(path) and os.path.getsize(path) > 0:
                try:
                    pm = QtGui.QPixmap(path)
                    dlg = ScreenshotEditDialog(pm, self)
                    if dlg.exec() == QtWidgets.QDialog.Accepted:
                        final_pm = dlg.result_pixmap
                        buf = QtCore.QBuffer()
                        buf.open(QtCore.QIODevice.WriteOnly)
                        final_pm.save(buf, "PNG")
                        data = bytes(buf.data())
                        
                        try:
                            if hasattr(self, "entry") and self.entry:
                                self.entry._insert_preview_datauri(data, "image/png")
                        except Exception:
                            pass
                        name = f"screenshot_{int(time.time())}.png"
                        try:
                            self._on_image_pasted(data, None, "image/png", name)
                        except Exception:
                            try:
                                self.entry.imagePasted.emit(data, None, "image/png", name)
                            except Exception:
                                pass
                except Exception:
                    pass
                try:
                    os.remove(path)
                except Exception:
                    pass
            else:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except Exception:
                    pass
        except Exception:
            pass

    def show_emoji_picker(self):
        try:
            picker = EmojiPicker(self)
            picker.emojiSelected.connect(self.insert_emoji)
            
            p = self.btn_emoji.mapToGlobal(QtCore.QPoint(0, 0))
            picker.move(p.x(), p.y() - picker.height() - 5)
            picker.show()
        except Exception:
            pass
            
    def insert_emoji(self, char):
        try:
            self.entry.textCursor().insertText(char)
            self.entry.setFocus()
        except Exception:
            pass

    def on_send(self):
        if not self.current_conv:
            return
        text = self._sanitize_text(self.entry.toPlainText())
        
        # Check if we have a pending pasted file/image
        if self.pending_image_bytes:
            name = self.pending_image_name or ("paste_" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch())) + ".png")
            mime = self.pending_image_mime or "application/octet-stream"
            size_bytes = len(self.pending_image_bytes) if self.pending_image_bytes is not None else 0
            limit_bytes = int(getattr(self, "max_upload_bytes", 40 * 1024 * 1024))
            if int(max(0, size_bytes)) > limit_bytes:
                try:
                    QtWidgets.QMessageBox.warning(self, "发送文件", f"文件大小超过 {self._human_readable_size(limit_bytes)}（{self._human_readable_size(size_bytes)}），无法发送")
                except Exception:
                    pass
                try:
                    placeholder = f"[文件: {name}]"
                    if placeholder in text:
                        text = text.replace(placeholder, "").strip()
                    try:
                        size_str = self._human_readable_size(int(max(0, size_bytes)))
                    except Exception:
                        size_str = ""
                    try:
                        lines = [ln for ln in (text.splitlines()) if ln.strip() not in {name.strip(), size_str.strip()}]
                        lines = [ln for ln in lines if ln.strip() != ""]
                        text = "\n".join(lines).strip()
                    except Exception:
                        pass
                except Exception:
                    pass
                self.pending_image_bytes = None
                self.pending_image_mime = None
                self.pending_image_name = None
                self.pending_image_pixmap = None
                if text:
                    wire_text = text.replace("\n", "\\n")
                    if self.current_conv.startswith("dm:"):
                        target = self.current_conv.split(":",1)[1]
                        self._send_seq(f"DM {target} {wire_text}")
                        self.store.add(f"dm:{target}", self.username, text, "msg", True)
                        self._ensure_conv(self.current_conv)
                        self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                    else:
                        rid = self.current_conv.split(":",1)[1]
                        self._send_seq(f"MSG {wire_text}", rid)
                        self.store.add(f"group:{rid}", self.username, text, "msg", True)
                        self._ensure_conv(self.current_conv)
                        self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                        self.view.scrollToBottom()
                    self.logger.write("sent", self.username, wire_text)
                self.entry.clear()
                return
            is_image = bool(mime and mime.lower().startswith("image/"))
            uniq_name = self._ensure_unique_filename(self.current_conv, self.username, name)
            if (not is_image) and size_bytes >= (2 * 1024 * 1024):
                try:
                    att_dir = self._attachment_dir(self.current_conv)
                    os.makedirs(att_dir, exist_ok=True)
                    temp_path = os.path.join(att_dir, uniq_name)
                    with open(temp_path, "wb") as f:
                        f.write(self.pending_image_bytes)
                    self._ensure_conv(self.current_conv)
                    pix = QtGui.QPixmap(temp_path)
                    try:
                        sz = os.path.getsize(temp_path)
                    except Exception:
                        sz = size_bytes
                    if int(max(0, sz)) > int(limit_bytes):
                        try:
                            QtWidgets.QMessageBox.warning(self, "发送文件", f"文件大小超过 {self._human_readable_size(limit_bytes)}（{self._human_readable_size(sz)}），无法发送")
                        except Exception:
                            pass
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass
                        self.pending_image_bytes = None
                        self.pending_image_mime = None
                        self.pending_image_name = None
                        self.pending_image_pixmap = None
                        self.view.scrollToBottom()
                        self.entry.clear()
                        return
                    if self.current_conv.startswith("group:"):
                        rid = self.current_conv.split(":",1)[1]
                        self._ensure_conv(self.current_conv)
                        self.conv_models[self.current_conv].add_file(self.username, uniq_name, mime, pix if not pix.isNull() else None, True, self.avatar_pixmap, None, sz)
                        try:
                            # 图片且小于2MB则直接走内嵌发送，否则上传服务器
                            if mime.lower().startswith("image/") and int(max(0, sz or 0)) < (2 * 1024 * 1024):
                                with open(temp_path, "rb") as f:
                                    b64 = base64.b64encode(f.read()).decode("ascii")
                                payload_text = f"[FILE] {uniq_name} {mime} {b64}"
                                self._send_seq(f"MSG {payload_text}", rid)
                            else:
                                self._http_upload_group_file(temp_path, rid, uniq_name)
                        except Exception:
                            pass
                    else:
                        self.conv_models[self.current_conv].add_file(self.username, uniq_name, mime, pix if not pix.isNull() else None, True, self.avatar_pixmap, None, sz)
                        self.store.add(self.current_conv, self.username, f"[FILE] {uniq_name} {mime}", "file", True)
                    try:
                        placeholder = f"[文件: {uniq_name}]"
                        if placeholder in text:
                            text = text.replace(placeholder, "").strip()
                        size_str = self._human_readable_size(size_bytes)
                        lines = [ln for ln in (text.splitlines()) if ln.strip() not in {name.strip(), size_str.strip()}]
                        lines = [ln for ln in lines if ln.strip() != ""]
                        text = "\n".join(lines).strip()
                    except Exception:
                        pass
                    if self.current_conv.startswith("dm:"):
                        self._start_async_upload(temp_path, uniq_name)
                except Exception:
                    pass
            else:
                b64 = base64.b64encode(self.pending_image_bytes).decode("ascii")
                # If text contains the placeholder, remove it so we don't send duplicate text
                placeholder = f"[文件: {uniq_name}]"
                if placeholder in text:
                    text = text.replace(placeholder, "").strip()
                # Also strip file-chip plaintext (name + size) that comes from HTML chip
                try:
                    size_str = self._human_readable_size(len(self.pending_image_bytes))
                    lines = [ln for ln in (text.splitlines()) if ln.strip() not in {name.strip(), size_str.strip()}]
                    lines = [ln for ln in lines if ln.strip() != ""]
                    text = "\n".join(lines).strip()
                except Exception:
                    pass
                payload_text = f"[FILE] {uniq_name} {mime} {b64}"
                if self.current_conv.startswith("dm:"):
                    target = self.current_conv.split(":",1)[1]
                    self._send_seq(f"DM {target} {payload_text}")
                    self.store.add(f"dm:{target}", self.username, payload_text, "file", True)
                    self.logger.write("sent", self.username, payload_text)
                    self._ensure_conv(self.current_conv)
                    _pix = self.pending_image_pixmap or self._pix_from_b64(mime, b64)
                    self.conv_models[self.current_conv].add_file(self.username, uniq_name, mime, _pix, True, self.avatar_pixmap, None, len(self.pending_image_bytes) if self.pending_image_bytes is not None else None)
                    try:
                        self._save_attachment(uniq_name, b64, self.current_conv)
                    except Exception:
                        pass
                else:
                    rid = self.current_conv.split(":",1)[1]
                    try:
                        limit_inline = 2 * 1024 * 1024
                        size_inline = len(self.pending_image_bytes) if self.pending_image_bytes is not None else 0
                        if mime.lower().startswith("image/") and size_inline < limit_inline:
                            self._send_seq(f"MSG {payload_text}", rid)
                            self.logger.write("sent", self.username, payload_text)
                            self._ensure_conv(self.current_conv)
                            _pix = self.pending_image_pixmap or self._pix_from_b64(mime, b64)
                            self.conv_models[self.current_conv].add_file(self.username, uniq_name, mime, _pix, True, self.avatar_pixmap, None, size_inline if size_inline else None)
                            try:
                                self._save_attachment(uniq_name, b64, self.current_conv)
                            except Exception:
                                pass
                        else:
                            att_dir = self._attachment_dir(self.current_conv)
                            os.makedirs(att_dir, exist_ok=True)
                            temp_path = os.path.join(att_dir, uniq_name)
                            with open(temp_path, "wb") as f:
                                f.write(self.pending_image_bytes)
                            pix = QtGui.QPixmap(temp_path)
                            try:
                                sz = os.path.getsize(temp_path)
                            except Exception:
                                sz = len(self.pending_image_bytes) if self.pending_image_bytes is not None else None
                            self._ensure_conv(self.current_conv)
                            self.conv_models[self.current_conv].add_file(self.username, uniq_name, mime, pix if not pix.isNull() else None, True, self.avatar_pixmap, None, sz)
                            self.store.add(self.current_conv, self.username, f"[FILE] {uniq_name} {mime}", "file", True)
                            self._http_upload_group_file(temp_path, rid, uniq_name)
                    except Exception:
                        pass
            
            self.pending_image_bytes = None
            self.pending_image_mime = None
            self.pending_image_name = None
            self.pending_image_pixmap = None
            self.view.scrollToBottom()
            
            # If there was remaining text, send it too
            if text:
                wire_text = text.replace("\n", "\\n")
                if self.current_conv.startswith("dm:"):
                    target = self.current_conv.split(":",1)[1]
                    self._send_seq(f"DM {target} {wire_text}")
                    self.store.add(f"dm:{target}", self.username, text, "msg", True)
                    self._ensure_conv(self.current_conv)
                    self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                else:
                    rid = self.current_conv.split(":",1)[1]
                    self._send_seq(f"MSG {wire_text}", rid)
                    self.store.add(f"group:{rid}", self.username, text, "msg", True)
                    self._ensure_conv(self.current_conv)
                    self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                    self.view.scrollToBottom()
                self.logger.write("sent", self.username, wire_text)
            
            self.entry.clear()
            return

        if not self.pending_image_bytes:
            img_bytes = self._extract_first_image_from_editor()
            if img_bytes:
                self.pending_image_bytes = img_bytes
                self.pending_image_mime = "image/png"
                self.pending_image_name = "paste_" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch())) + ".png"
                self.pending_image_pixmap = None
            
        try:
            if self.pending_image_bytes:
                name = self.pending_image_name or ("paste_" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch())) + ".png")
                mime = self.pending_image_mime or "image/png"
                uniq_name = self._ensure_unique_filename(self.current_conv, self.username, name)
                if self.current_conv.startswith("dm:"):
                    b64 = base64.b64encode(self.pending_image_bytes).decode("ascii")
                    payload_text = f"[FILE] {uniq_name} {mime} {b64}"
                    target = self.current_conv.split(":",1)[1]
                    self._send_seq(f"DM {target} {payload_text}")
                    self.store.add(f"dm:{target}", self.username, payload_text, "file", True)
                    self.logger.write("sent", self.username, payload_text)
                    self._ensure_conv(self.current_conv)
                    _pix = self.pending_image_pixmap or self._pix_from_b64(mime, b64)
                    self.conv_models[self.current_conv].add_file(self.username, uniq_name, mime, _pix, True, self.avatar_pixmap, None, len(self.pending_image_bytes) if self.pending_image_bytes is not None else None)
                    try:
                        self._save_attachment(uniq_name, b64, self.current_conv)
                    except Exception:
                        pass
                else:
                    rid = self.current_conv.split(":",1)[1]
                    try:
                        limit_inline = 2 * 1024 * 1024
                        size_inline = len(self.pending_image_bytes) if self.pending_image_bytes is not None else 0
                        b64 = base64.b64encode(self.pending_image_bytes).decode("ascii")
                        payload_text = f"[FILE] {uniq_name} {mime} {b64}"
                        if mime.lower().startswith("image/") and size_inline < limit_inline:
                            self._send_seq(f"MSG {payload_text}", rid)
                            self.store.add(f"group:{rid}", self.username, payload_text, "file", True)
                            self.logger.write("sent", self.username, payload_text)
                            self._ensure_conv(self.current_conv)
                            _pix = self.pending_image_pixmap or self._pix_from_b64(mime, b64)
                            self.conv_models[self.current_conv].add_file(self.username, uniq_name, mime, _pix, True, self.avatar_pixmap, None, size_inline if size_inline else None)
                            try:
                                self._save_attachment(uniq_name, b64, self.current_conv)
                            except Exception:
                                pass
                        else:
                            att_dir = self._attachment_dir(self.current_conv)
                            os.makedirs(att_dir, exist_ok=True)
                            temp_path = os.path.join(att_dir, uniq_name)
                            with open(temp_path, "wb") as f:
                                f.write(self.pending_image_bytes)
                            self._http_upload_group_file(temp_path, rid, uniq_name)
                    except Exception:
                        pass
                self.pending_image_bytes = None
                self.pending_image_mime = None
                self.pending_image_name = None
                self.pending_image_pixmap = None
                self.view.scrollToBottom()
            else:
                url_path = self._extract_first_file_url_from_text(text)
                if url_path and os.path.isfile(url_path):
                    try:
                        name = os.path.basename(url_path)
                        mime = self._guess_mime(url_path)
                        self._ensure_conv(self.current_conv)
                        try:
                            sz = os.path.getsize(url_path)
                        except Exception:
                            sz = None
                        limit_bytes = int(getattr(self, "max_upload_bytes", 40 * 1024 * 1024))
                        if int(max(0, sz or 0)) > int(limit_bytes):
                            try:
                                QtWidgets.QMessageBox.warning(self, "发送文件", f"文件大小超过 {self._human_readable_size(limit_bytes)}（{self._human_readable_size(sz or 0)}），无法发送")
                            except Exception:
                                pass
                            try:
                                text = self._sanitize_text(text.replace(f"file://{url_path}", ""))
                            except Exception:
                                pass
                            if text:
                                try:
                                    size_str = self._human_readable_size(int(max(0, sz or 0)))
                                except Exception:
                                    size_str = ""
                                try:
                                    base = os.path.basename(url_path)
                                    lines = [ln for ln in (text.splitlines()) if ln.strip() not in {base.strip(), size_str.strip()}]
                                    lines = [ln for ln in lines if ln.strip() != ""]
                                    text = "\n".join(lines).strip()
                                except Exception:
                                    pass
                                wire_text = text.replace("\n", "\\n")
                                if self.current_conv.startswith("dm:"):
                                    target = self.current_conv.split(":",1)[1]
                                    self._send_seq(f"DM {target} {wire_text}")
                                    self.store.add(f"dm:{target}", self.username, text, "msg", True)
                                    self._ensure_conv(self.current_conv)
                                    self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                                else:
                                    rid = self.current_conv.split(":",1)[1]
                                    self._send_seq(f"MSG {wire_text}", rid)
                                    self.store.add(f"group:{rid}", self.username, text, "msg", True)
                                    self._ensure_conv(self.current_conv)
                                    self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                                    self.view.scrollToBottom()
                                self.logger.write("sent", self.username, wire_text)
                            self.entry.clear()
                            return
                        if self.current_conv.startswith("group:"):
                            rid = self.current_conv.split(":",1)[1]
                            try:
                                att_dir = self._attachment_dir(self.current_conv)
                                os.makedirs(att_dir, exist_ok=True)
                                uniq_name = self._ensure_unique_filename(self.current_conv, self.username, name)
                                dst = os.path.join(att_dir, uniq_name)
                                try:
                                    shutil.copy2(url_path, dst)
                                except Exception:
                                    with open(url_path, "rb") as sf, open(dst, "wb") as df:
                                        df.write(sf.read())
                                pix = QtGui.QPixmap(dst)
                                try:
                                    sz = os.path.getsize(dst)
                                except Exception:
                                    sz = os.path.getsize(url_path) if os.path.exists(url_path) else None
                                self._ensure_conv(self.current_conv)
                                self.conv_models[self.current_conv].add_file(self.username, uniq_name, mime, pix if not pix.isNull() else None, True, self.avatar_pixmap, None, sz)
                                # 图片且小于2MB则直接走内嵌发送，否则上传服务器
                                if mime.lower().startswith("image/") and int(max(0, sz or 0)) < (2 * 1024 * 1024):
                                    try:
                                        with open(dst, "rb") as f:
                                            b64 = base64.b64encode(f.read()).decode("ascii")
                                        payload_text = f"[FILE] {uniq_name} {mime} {b64}"
                                        self._send_seq(f"MSG {payload_text}", rid)
                                        self.store.add(f"group:{rid}", self.username, payload_text, "file", True)
                                    except Exception:
                                        pass
                                else:
                                    self._http_upload_group_file(dst, rid, uniq_name)
                            except Exception:
                                pass
                            try:
                                text = self._sanitize_text(text.replace(f"file://{url_path}", ""))
                            except Exception:
                                pass
                            if text:
                                wire_text = text.replace("\n", "\\n")
                                try:
                                    self._send_seq(f"MSG {wire_text}", rid)
                                    self.store.add(f"group:{rid}", self.username, text, "msg", True)
                                    self._ensure_conv(self.current_conv)
                                    self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                                    self.view.scrollToBottom()
                                    self.logger.write("sent", self.username, wire_text)
                                except Exception:
                                    pass
                            self.entry.clear()
                            return
                        else:
                            pix = QtGui.QPixmap(url_path)
                            self.conv_models[self.current_conv].add_file(self.username, name, mime, pix if not pix.isNull() else None, True, self.avatar_pixmap, None, sz)
                            self.store.add(self.current_conv, self.username, f"[FILE] {name} {mime}", "file", True)
                            try:
                                text = self._sanitize_text(text.replace(f"file://{url_path}", ""))
                            except Exception:
                                pass
                            try:
                                self._start_async_upload(url_path)
                            except Exception:
                                pass
                            self.view.scrollToBottom()
                    except Exception:
                        pass
            if text:
                wire_text = text.replace("\n", "\\n")
                if self.current_conv.startswith("dm:"):
                    target = self.current_conv.split(":",1)[1]
                    self._send_seq(f"DM {target} {wire_text}")
                    self.store.add(f"dm:{target}", self.username, text, "msg", True)
                    self._ensure_conv(self.current_conv)
                    self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                    self.view.scrollToBottom()
                else:
                    rid = self.current_conv.split(":",1)[1]
                    self._send_seq(f"MSG {wire_text}", rid)
                    self.store.add(f"group:{rid}", self.username, text, "msg", True)
                    self._ensure_conv(self.current_conv)
                    self.conv_models[self.current_conv].add("msg", self.username, text, True, self.avatar_pixmap)
                    self.view.scrollToBottom()
                self.logger.write("sent", self.username, wire_text)
            self.entry.clear()
        except Exception:
            pass

    def on_clear_dm(self):
        pass

    def on_pick_conv(self, item: QtWidgets.QListWidgetItem):
        text = item.text()
        data = None
        try:
            data = item.data(QtCore.Qt.UserRole)
        except Exception:
            data = None
        if isinstance(data, str) and data.startswith("group:"):
            self.switch_conv(data)
        else:
            name = text.split(" (",1)[0]
            self.switch_conv(f"dm:{name}")
        try:
            self.view.scrollToBottom()
            QtCore.QTimer.singleShot(0, lambda: self.view.scrollToBottom())
        except Exception:
            pass

    def _switch_room(self, rid: str, name: str):
        try:
            if hasattr(self, 'hb') and self.hb:
                try:
                    self.hb.stop()
                except Exception:
                    pass
            if hasattr(self, 'rx') and self.rx:
                try:
                    self.rx.stop()
                    self.rx.wait(2000)
                except Exception:
                    pass
                try:
                    self.rx.deleteLater()
                except Exception:
                    pass
                self.rx = None
            if self.sock:
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    self.sock.close()
                except Exception:
                    pass
                self.sock = None
            self.room = rid
            self.room_name = name
            self.room_ready = True
            try:
                self.room_name_map[rid] = name
            except Exception:
                pass
            self._connect()
            self.view_mode = "group"
            try:
                self._rebuild_conv_list()
            except Exception:
                pass
        except Exception:
            pass

    def _add_conv_dm(self, name: str):
        exists = False
        for i in range(self.conv_list.count()):
            if self.conv_list.item(i).text().split(" (",1)[0] == name:
                exists = True
                break
        if not exists:
            it = QtWidgets.QListWidgetItem(name)
            it.setSizeHint(QtCore.QSize(200, 56))
            try:
                it.setIcon(QtGui.QIcon())
            except Exception:
                pass
            self.conv_list.addItem(it)
            row = self.conv_list.count() - 1
            w = QtWidgets.QWidget()
            hl = QtWidgets.QHBoxLayout()
            try:
                hl.setContentsMargins(12, 6, 12, 6)
                hl.setSpacing(8)
            except Exception:
                pass
            avatar_lbl = QtWidgets.QLabel()
            avatar_lbl.setFixedSize(28, 28)
            avatar_lbl.setPixmap(self._status_pixmap_for_name(name, 28))
            name_lbl = QtWidgets.QLabel(name)
            try:
                name_lbl.setStyleSheet("QLabel{font:14px 'Helvetica Neue';}")
            except Exception:
                pass
            badge_lbl = QtWidgets.QLabel()
            badge_lbl.setVisible(False)
            try:
                badge_lbl.setStyleSheet("QLabel{background:#F44336;color:#fff;border-radius:8px;padding:0 6px;font:11px 'Helvetica Neue';}")
            except Exception:
                pass
            hl.addWidget(avatar_lbl)
            hl.addWidget(name_lbl, 1)
            hl.addWidget(badge_lbl)
            w.setLayout(hl)
            self.conv_list.setItemWidget(it, w)
        self.conv_badges[f"dm:{name}"] = badge_lbl
        self.conv_avatar_labels[f"dm:{name}"] = avatar_lbl
        self._ensure_unread_key(f"dm:{name}")
        self._ensure_conv(f"dm:{name}")
        try:
            self._apply_conv_filter()
        except Exception:
            pass
        try:
            self._update_conv_title(f"dm:{name}")
            self._update_sidebar_badge()
        except Exception:
            pass

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
        if key in self.conv_avatar_labels:
            del self.conv_avatar_labels[key]

    def on_send_file(self):
        dlg = QtWidgets.QFileDialog(self)
        dlg.setFileMode(QtWidgets.QFileDialog.ExistingFile)
        if dlg.exec():
            files = dlg.selectedFiles()
            if files:
                path = files[0]
                name = os.path.basename(path)
                mime = self._guess_mime(path)
                try:
                    if self.current_conv and self.current_conv.startswith("group:"):
                        rid = self.current_conv.split(":",1)[1]
                        self._http_upload_group_file(path, rid, name)
                        self.view.scrollToBottom()
                    else:
                        pix = QtGui.QPixmap(path)
                        self._ensure_conv(self.current_conv)
                        try:
                            sz = os.path.getsize(path)
                        except Exception:
                            sz = None
                        self.conv_models[self.current_conv].add_file(self.username, name, mime, pix if not pix.isNull() else None, True, self.avatar_pixmap, None, sz)
                        self.store.add(self.current_conv, self.username, f"[FILE] {name} {mime}", "file", True)
                        self._start_async_upload(path)
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
        self._update_sidebar_badge()

    def _reset_unread(self, key: str):
        self.conv_unread[key] = 0
        self._update_conv_title(key)
        self._update_sidebar_badge()

    def _update_conv_title(self, key: str):
        if key.startswith("group:"):
            rid = key.split(":",1)[1]
            title = self.room_name if rid == self.room else self.room_name_map.get(rid, rid)
        else:
            title = key.split(":",1)[1]
        count = self.conv_unread.get(key, 0)
        text = f"{title} ({count})" if count > 0 else title
        for i in range(self.conv_list.count()):
            item = self.conv_list.item(i)
            base = item.text().split(" (",1)[0]
            if base == title:
                item.setText(text)
                # update per-item badge
                b = self.conv_badges.get(key)
                if b is not None:
                    if count > 0:
                        b.setText(str(count))
                        b.setVisible(True)
                    else:
                        b.setVisible(False)
                break
        try:
            self._apply_conv_filter()
        except Exception:
            pass
    def _update_sidebar_badge(self):
        try:
            total_dm = sum(v for k,v in self.conv_unread.items() if k.startswith("dm:"))
            group_cnt = sum(v for k,v in self.conv_unread.items() if k.startswith("group:"))
            if total_dm > 0:
                self.msg_badge.setText(str(total_dm))
                self.msg_badge.setVisible(True)
            else:
                self.msg_badge.setVisible(False)
            if group_cnt > 0:
                self.group_badge.setText(str(group_cnt))
                self.group_badge.setVisible(True)
            else:
                self.group_badge.setVisible(False)
            self._update_dock_badge(total_dm + group_cnt)
        except Exception:
            pass
    def _apply_conv_filter(self):
        try:
            items_log = []
            for i in range(self.conv_list.count()):
                base = self.conv_list.item(i).text().split(" (",1)[0]
                item = self.conv_list.item(i)
                data = None
                try:
                    data = item.data(QtCore.Qt.UserRole)
                except Exception:
                    data = None
                if self.view_mode == "group":
                    hide = not (isinstance(data, str) and data.startswith("group:"))
                    self.conv_list.setItemHidden(self.conv_list.item(i), hide)
                    items_log.append(f"{i}:{base}:{'hide' if hide else 'show'}")
                else:
                    hide = (isinstance(data, str) and data.startswith("group:"))
                    self.conv_list.setItemHidden(self.conv_list.item(i), hide)
                    items_log.append(f"{i}:{base}:{'hide' if hide else 'show'}")
            try:
                pass
            except Exception:
                pass
        except Exception:
            pass

    def _rebuild_conv_list(self):
        try:
            self.conv_list.clear()
            self.conv_badges = {}
            try:
                self.conv_avatar_labels = {}
            except Exception:
                pass
            if self.view_mode == "group":
                self._ensure_group_items()
                want = f"group:{self.room}"
                for i in range(self.conv_list.count()):
                    it = self.conv_list.item(i)
                    data = it.data(QtCore.Qt.UserRole) if it else None
                    if data == want:
                        self.conv_list.setCurrentRow(i)
                        break
            else:
                names_set = set([k.split(":",1)[1] for k in self.conv_unread.keys() if k.startswith("dm:")])
                def _sort_key(n: str):
                    return (0 if n in self.online_users else 1, n.lower())
                names = sorted(list(names_set), key=_sort_key)
                for name in names:
                    self._add_conv_dm(name)
                if self.current_conv and self.current_conv.startswith("dm:"):
                    sel = self.current_conv.split(":",1)[1]
                    for i in range(self.conv_list.count()):
                        base = self.conv_list.item(i).text().split(" (",1)[0]
                        if base == sel:
                            self.conv_list.setCurrentRow(i)
                            break
            self._apply_conv_filter()
        except Exception:
            pass

    def _sync_room_from_server(self):
        try:
            url = f"http://{self.host}:34568/api/status"
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            rooms = data.get("rooms") or []
            self.rooms_info = rooms
            try:
                self.room_name_map = {str(r.get("id")): str(r.get("name") or str(r.get("id"))) for r in rooms}
            except Exception:
                self.room_name_map = {}
            if rooms:
                prefer = None
                for r in rooms:
                    if str(r.get("id")) == str(self.room):
                        prefer = r
                        break
                info = prefer or rooms[0]
                rid = str(info.get("id") or self.room)
                rname = str(info.get("name") or rid)
                self.room = rid
                self.room_name = rname
                self.room_ready = True
                try:
                    self.room_name_map[self.room] = self.room_name
                except Exception:
                    pass
        except Exception:
            pass

    def _update_dock_badge(self, count: int):
        if not Cocoa:
            return
        try:
            dock_tile = Cocoa.NSApplication.sharedApplication().dockTile()
            if count > 0:
                dock_tile.setBadgeLabel_(str(count))
            else:
                dock_tile.setBadgeLabel_(None)
        except Exception:
            pass
    def _try_local_peer_avatar(self, name: str) -> bool:
        try:
            icon_dir = os.path.join(os.getcwd(), "icons", "user")
            avatars_dir = os.path.join(self.logger.log_dir, "avatars")
            for ext in (".png", ".jpg", ".jpeg"):
                p = os.path.join(icon_dir, f"{name}{ext}")
                if os.path.isfile(p):
                    pm = QtGui.QPixmap(p)
                    if not pm.isNull():
                        self.peer_avatars[name] = pm
                        self._refresh_conv_icon(name)
                        try:
                            for m in self.conv_models.values():
                                m.set_sender_avatar(name, pm)
                        except Exception:
                            pass
                        return True
            try:
                per_user_dir = os.path.join(self.logger.log_dir, name)
                for ext in (".png", ".jpg", ".jpeg"):
                    cand = os.path.join(per_user_dir, f"avatar{ext}")
                    if os.path.isfile(cand):
                        pm = QtGui.QPixmap(cand)
                        if not pm.isNull():
                            self.peer_avatars[name] = pm
                            self._refresh_conv_icon(name)
                            try:
                                for m in self.conv_models.values():
                                    m.set_sender_avatar(name, pm)
                            except Exception:
                                pass
                            return True
                for ext in (".png", ".jpg", ".jpeg"):
                    cand2 = os.path.join(per_user_dir, f"{name}{ext}")
                    if os.path.isfile(cand2):
                        pm = QtGui.QPixmap(cand2)
                        if not pm.isNull():
                            self.peer_avatars[name] = pm
                            self._refresh_conv_icon(name)
                            try:
                                for m in self.conv_models.values():
                                    m.set_sender_avatar(name, pm)
                            except Exception:
                                pass
                            return True
            except Exception:
                pass
            try:
                profiles = _load_profiles(self.logger.log_dir)
                afn = profiles.get(name)
                if afn:
                    p1 = os.path.join(icon_dir, afn)
                    p2 = os.path.join(avatars_dir, afn)
                    p3 = os.path.join(self.logger.log_dir, name, afn)
                    cand = p1 if os.path.isfile(p1) else (p2 if os.path.isfile(p2) else (p3 if os.path.isfile(p3) else None))
                    if cand:
                        pm = QtGui.QPixmap(cand)
                        if not pm.isNull():
                            self.peer_avatars[name] = pm
                            self._refresh_conv_icon(name)
                            try:
                                for m in self.conv_models.values():
                                    m.set_sender_avatar(name, pm)
                            except Exception:
                                pass
                            return True
            except Exception:
                pass
        except Exception:
            pass
        return False

    def _send_macos_notification(self, title: str, text: str):
        if not Cocoa:
            return
        try:
            notification = Cocoa.NSUserNotification.alloc().init()
            notification.setTitle_(str(title))
            notification.setInformativeText_(str(text))
            notification.setSoundName_("NSUserNotificationDefaultSoundName")
            Cocoa.NSUserNotificationCenter.defaultUserNotificationCenter().deliverNotification_(notification)
        except Exception:
            pass
    def _set_unread(self, key: str, cnt: int):
        self.conv_unread[key] = max(0, cnt)
        self._update_conv_title(key)
        self._update_sidebar_badge()

    def _ensure_conv(self, key: str):
        if key not in self.conv_models:
            self.conv_models[key] = ChatModel()

    def _letter_pixmap(self, name: str, size: int = 24) -> QtGui.QPixmap:
        pm = QtGui.QPixmap(size, size)
        pm.fill(QtCore.Qt.transparent)
        p = QtGui.QPainter(pm)
        p.setRenderHint(QtGui.QPainter.Antialiasing, True)
        hue = (sum(ord(c) for c in name) % 360)
        color = QtGui.QColor.fromHsl(hue, 160, 160)
        p.setBrush(color)
        p.setPen(QtCore.Qt.NoPen)
        p.drawEllipse(0, 0, size, size)
        p.setPen(QtGui.QColor(255, 255, 255))
        f = p.font()
        f.setBold(True)
        p.setFont(f)
        p.drawText(QtCore.QRect(0, 0, size, size), QtCore.Qt.AlignCenter, name[:1] if name else "?")
        p.end()
        return pm

    def _icon_for_name(self, name: str) -> QtGui.QIcon:
        if name == self.username and self.avatar_pixmap:
            return QtGui.QIcon(self.avatar_pixmap)
        pm = self.peer_avatars.get(name)
        if pm:
            return QtGui.QIcon(pm)
        return QtGui.QIcon(self._letter_pixmap(name))

    def _refresh_conv_icon(self, name: str):
        for i in range(self.conv_list.count()):
            item = self.conv_list.item(i)
            base = item.text().split(" (",1)[0]
            if base == name:
                try:
                    item.setIcon(QtGui.QIcon())
                except Exception:
                    pass
                lbl = self.conv_avatar_labels.get(f"dm:{name}")
                if lbl:
                    lbl.setPixmap(self._status_pixmap_for_name(name, 24))
                break

    def _set_peer_avatar(self, name: str, filename: str):
        try:
            pm = None
            per_user_dir = os.path.join(self.logger.log_dir, name)
            for ext in (".png", ".jpg", ".jpeg"):
                cand = os.path.join(per_user_dir, f"avatar{ext}")
                if os.path.exists(cand):
                    pm = QtGui.QPixmap(cand)
                    if not pm.isNull():
                        break
            if pm is None or pm.isNull():
                per_user = os.path.join(per_user_dir, filename)
                if os.path.exists(per_user):
                    pm = QtGui.QPixmap(per_user)
            if pm is None or pm.isNull():
                for ext in (".png", ".jpg", ".jpeg"):
                    cand = os.path.join(self.logger.log_dir, "avatars", f"avatar{ext}")
                    if os.path.exists(cand):
                        pm = QtGui.QPixmap(cand)
                        if not pm.isNull():
                            break
            if pm is None or pm.isNull():
                alt = os.path.join(self.logger.log_dir, "avatars", filename)
                if os.path.exists(alt):
                    pm = QtGui.QPixmap(alt)
            if pm is None or pm.isNull():
                path = os.path.join(os.getcwd(), "icons", "user", filename)
                if os.path.exists(path):
                    pm = QtGui.QPixmap(path)
            if pm and not pm.isNull():
                self.peer_avatars[name] = pm
                self._refresh_conv_icon(name)
                try:
                    for m in self.conv_models.values():
                        m.set_sender_avatar(name, pm)
                except Exception:
                    pass
                try:
                    d = os.path.join(self.logger.log_dir, "avatars")
                    os.makedirs(d, exist_ok=True)
                    self.avatar_file_map[name] = filename
                    with open(os.path.join(d, "avatar_map.json"), "w", encoding="utf-8") as f:
                        json.dump(self.avatar_file_map, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
        except Exception:
            pass

    def _save_peer_avatar_file(self, user: str, filename: str, mime: str, b64: str) -> Optional[str]:
        try:
            d = os.path.join(self.logger.log_dir, "avatars")
            os.makedirs(d, exist_ok=True)
            p = os.path.join(d, filename)
            data = base64.b64decode(b64)
            with open(p, "wb") as f:
                f.write(data)
            try:
                per_user_dir = os.path.join(self.logger.log_dir, user)
                os.makedirs(per_user_dir, exist_ok=True)
                pu = os.path.join(per_user_dir, filename)
                with open(pu, "wb") as f2:
                    f2.write(data)
            except Exception:
                pass
            try:
                ext = ".png" if mime.lower() == "image/png" else (".jpg" if mime.lower() in ("image/jpg", "image/jpeg") else os.path.splitext(filename)[1] or ".png")
                p_norm = os.path.join(d, f"avatar{ext}")
                with open(p_norm, "wb") as f3:
                    f3.write(data)
                pu_norm = os.path.join(os.path.join(self.logger.log_dir, user), f"avatar{ext}")
                with open(pu_norm, "wb") as f4:
                    f4.write(data)
                try:
                    _save_profile(self.logger.log_dir, user, f"avatar{ext}")
                except Exception:
                    pass
                return pu_norm
            except Exception:
                return p
        except Exception:
            return None

    def _set_online(self, name: str, online: bool):
        if online:
            self.online_users.add(name)
        else:
            self.online_users.discard(name)
        self._refresh_conv_icon(name)

    def _base_avatar_pixmap(self, name: str, size: int = 24) -> QtGui.QPixmap:
        if name == self.username and self.avatar_pixmap:
            return self.avatar_pixmap.scaled(size, size, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
        pm = self.peer_avatars.get(name)
        if not pm:
            try:
                self._try_local_peer_avatar(name)
            except Exception:
                pass
            pm = self.peer_avatars.get(name)
        if pm:
            return pm.scaled(size, size, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
        return self._letter_pixmap(name, size)

    def _status_pixmap_for_name(self, name: str, size: int = 24) -> QtGui.QPixmap:
        base = self._base_avatar_pixmap(name, size)
        if name in self.online_users:
            return base
        return self._desaturate_pixmap(base, size)

    def _desaturate_pixmap(self, pm: QtGui.QPixmap, size: int) -> QtGui.QPixmap:
        img = pm.toImage().convertToFormat(QtGui.QImage.Format_ARGB32)
        w = img.width()
        h = img.height()
        for y in range(h):
            for x in range(w):
                c = img.pixel(x, y)
                a = (c >> 24) & 0xFF
                r = (c >> 16) & 0xFF
                g = (c >> 8) & 0xFF
                b = c & 0xFF
                gray = int(0.299 * r + 0.587 * g + 0.114 * b)
                img.setPixel(x, y, (a << 24) | (gray << 16) | (gray << 8) | gray)
        out = QtGui.QPixmap.fromImage(img)
        return out.scaled(size, size, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)

    def switch_conv(self, key: str):
        self._ensure_conv(key)
        self.current_conv = key
        self.chat_stack.setCurrentIndex(1)
        if key.startswith("group:"):
            self.dm_target = None
        else:
            name = key.split(":",1)[1]
            self.dm_target = name
        self.current_model = self.conv_models[key]
        self.view.setModel(self.current_model)
        self._reset_unread(key)
        try:
            self.view.scrollToBottom()
        except Exception:
            pass
        # 不自动发送 READ，避免服务端推送未读/历史
        if len(self.current_model.items) == 0:
            conv = key
            missing = 0
            for sender, ts, kind, text, selfflag in self.store.recent(conv, 100):
                if text.startswith("[LINK] "):
                    try:
                        toks = text.split(" ")
                        url = toks[-1] if len(toks) >= 2 else ""
                        size = int(toks[-2]) if len(toks) >= 3 else 0
                        filename = " ".join(toks[1:-2]) if len(toks) >= 3 else (toks[1] if len(toks) > 1 else "")
                    except Exception:
                        filename = ""
                        url = ""
                        size = 0
                    av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                    self.current_model.add_link(sender, filename, url, bool(selfflag), av, int(ts) if ts else None, size)
                    continue
                if kind == "file" and text.startswith("[FILE] "):
                    fn, mime, _ = self._parse_file(text)
                    p = self._attachment_path(fn, conv)
                    pix = QtGui.QPixmap(p) if os.path.isfile(p) else None
                    av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                    try:
                        sz = os.path.getsize(p) if os.path.isfile(p) else None
                    except Exception:
                        sz = None
                    self.current_model.add_file(sender, fn, mime, pix if pix and not pix.isNull() else None, bool(selfflag), av, int(ts) if ts else None, sz)
                    if not (pix and not pix.isNull()):
                        missing += 1
                elif kind == "sys":
                    self.current_model.add("sys", "", text, False, None, int(ts) if ts else None)
                else:
                    av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                    self.current_model.add("msg", sender, text, bool(selfflag), av, int(ts) if ts else None)
            # 不自动拉取历史，保留空界面
        try:
            QtCore.QTimer.singleShot(0, lambda: self.view.scrollToBottom())
        except Exception:
            pass

    def on_view_context_menu(self, pos):
        sender = self.sender()
        global_pos = sender.mapToGlobal(pos) if hasattr(sender, 'mapToGlobal') else QtGui.QCursor.pos()
        vp_pos = self.view.viewport().mapFromGlobal(global_pos)
        index = self.view.indexAt(vp_pos)
        if not index.isValid():
            index = self.view.currentIndex()
        menu = QtWidgets.QMenu(self)
        kind = index.data(ChatModel.KindRole) if index.isValid() else None
        act_copy = menu.addAction("复制文本")
        act_copy.setEnabled(index.isValid() and kind in ("msg", "file"))
        def do_copy():
            self._copy_index(index)
        act_copy.triggered.connect(do_copy)
        if kind == "msg":
            act_view = menu.addAction("查看/复制...")
            def do_view():
                txt = index.data(ChatModel.TextRole) or ""
                self._open_text_viewer(txt)
            act_view.triggered.connect(do_view)
        act_del = menu.addAction("删除此消息")
        can_del = index.isValid() and kind in ("msg", "file")
        act_del.setEnabled(can_del)
        if can_del:
            def do_del():
                sender_name = index.data(ChatModel.SenderRole)
                text = index.data(ChatModel.TextRole)
                is_self = bool(index.data(ChatModel.SelfRole))
                filename = index.data(ChatModel.FileNameRole)
                mime = index.data(ChatModel.MimeRole)
                store_text = text if kind == "msg" else (f"[FILE] {filename} {mime}" if filename and mime else text)
                self.store.delete_message(self.current_conv, sender_name, kind, store_text, is_self, filename, mime)
                try:
                    prefix = store_text if kind == "msg" else f"[FILE] {filename} {mime}"
                    self.store.mark_deleted(self.current_conv, sender_name, kind, prefix)
                except Exception:
                    pass
                if kind == "file" and filename:
                    try:
                        p = self._attachment_path(filename, self.current_conv)
                        if os.path.isfile(p):
                            os.remove(p)
                    except Exception:
                        pass
                self.current_model.remove_row(index.row())
            act_del.triggered.connect(do_del)
        if kind == "file":
            filename = index.data(ChatModel.FileNameRole)
            mime = index.data(ChatModel.MimeRole) or ""
            link_url = index.data(ChatModel.LinkUrlRole) or ""
            act_open = menu.addAction("打开文件")
            def do_open_file():
                if link_url:
                    QtGui.QDesktopServices.openUrl(QtCore.QUrl(str(link_url)))
                else:
                    path = self._attachment_path(filename, self.current_conv)
                    QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(path))
            act_open.triggered.connect(do_open_file)
            
            act_save_as = menu.addAction("另存为...")
            def do_save_as():
                try:
                    if link_url:
                        dst, _ = QtWidgets.QFileDialog.getSaveFileName(self, "另存为", filename or "")
                        if dst:
                            try:
                                with urllib.request.urlopen(link_url, timeout=10.0) as resp:
                                    data = resp.read()
                                with open(dst, "wb") as f:
                                    f.write(data)
                            except Exception:
                                QtWidgets.QMessageBox.warning(self, "下载失败", "无法下载该文件")
                    else:
                        src = self._attachment_path(filename, self.current_conv)
                        if not os.path.isfile(src):
                            return
                        dst, _ = QtWidgets.QFileDialog.getSaveFileName(self, "另存为", filename)
                        if dst:
                            shutil.copy2(src, dst)
                except Exception:
                    pass
            act_save_as.triggered.connect(do_save_as)

            act_open_dir = menu.addAction("打开所在文件夹")
            def do_open_dir():
                QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(self._attachment_dir(self.current_conv)))
            act_open_dir.triggered.connect(do_open_dir)
            act_preview = menu.addAction("预览")
            pix = index.data(ChatModel.PixmapRole)
            act_preview.setEnabled(bool(pix) and mime.startswith("image/"))
            def do_preview():
                if not pix:
                    return
                dlg = QtWidgets.QDialog(self)
                dlg.setWindowTitle(filename or "预览")
                lbl = QtWidgets.QLabel()
                lbl.setPixmap(pix.scaled(600, 600, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))
                lay = QtWidgets.QVBoxLayout()
                lay.addWidget(lbl)
                dlg.setLayout(lay)
                dlg.exec()
            act_preview.triggered.connect(do_preview)
            row = index.row()
            key = self.current_conv or ""
            w = self.upload_workers.get((key, row))
            if w:
                state = self.current_model.data(index, ChatModel.UploadStateRole) or ""
                if state == "paused":
                    act_resume = menu.addAction("继续发送")
                    def _resume():
                        try:
                            w.pause_toggle()
                            self.current_model.set_upload_progress(row, None, None, "uploading")
                        except Exception:
                            pass
                    act_resume.triggered.connect(_resume)
                else:
                    act_pause = menu.addAction("暂停发送")
                    def _pause():
                        try:
                            w.pause_toggle()
                            self.current_model.set_upload_progress(row, None, None, "paused")
                        except Exception:
                            pass
                    act_pause.triggered.connect(_pause)
                act_cancel = menu.addAction("取消发送")
                def _cancel():
                    try:
                        # cancel worker
                        w.cancel()
                        # hide pie instantly
                        try:
                            self.current_model.set_upload_progress(row, None, None, "canceled")
                            self.current_model.set_upload_alpha(row, 0)
                            key2 = (key, row)
                            t2 = self._fade_timers.get(key2)
                            if t2:
                                try:
                                    t2.stop()
                                    t2.deleteLater()
                                except Exception:
                                    pass
                                try:
                                    del self._fade_timers[key2]
                                except Exception:
                                    pass
                            if hasattr(self, "view") and self.view:
                                self.view.viewport().update()
                        except Exception:
                            pass
                        # remove worker mapping to avoid further UI updates
                        try:
                            if (key, row) in self.upload_workers:
                                del self.upload_workers[(key, row)]
                        except Exception:
                            pass
                        # notify peer to cleanup .part
                        try:
                            fname = index.data(ChatModel.FileNameRole) or ""
                            if self.current_conv and self.current_conv.startswith("dm:"):
                                target = self.current_conv.split(":",1)[1]
                                if fname:
                                    self._send_seq(f"DM {target} FILE_CANCEL {fname}")
                            else:
                                rid = (self.current_conv.split(":",1)[1] if self.current_conv and self.current_conv.startswith("group:") else self.room)
                                if rid and fname:
                                    self._send_seq(f"MSG FILE_CANCEL {fname}", rid)
                        except Exception:
                            pass
                    except Exception:
                        pass
                act_cancel.triggered.connect(_cancel)
        act_clear = menu.addAction("清空当前会话")
        def do_clear():
            res = QtWidgets.QMessageBox.question(self, "确认", "确定清空当前会话的聊天记录？", QtWidgets.QMessageBox.Yes|QtWidgets.QMessageBox.No)
            if res == QtWidgets.QMessageBox.Yes:
                self.store.delete_conv(self.current_conv)
                self.current_model.clear()
                try:
                    d = self._attachment_dir(self.current_conv)
                    if os.path.isdir(d):
                        shutil.rmtree(d, ignore_errors=True)
                    self.store.mark_cleared(self.current_conv)
                except Exception:
                    pass
        act_clear.triggered.connect(do_clear)
        menu.exec(global_pos)

    def _check_remote_file_exists(self, url: str) -> bool:
        if not url.startswith("http"):
            return True
        try:
            req = urllib.request.Request(url, method='HEAD')
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status == 200
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            return True
        except Exception:
            return True

    def on_view_double_click(self, index: QtCore.QModelIndex):
        if not index.isValid():
            return
        kind = index.data(ChatModel.KindRole)
        if kind == "file":
            filename = index.data(ChatModel.FileNameRole)
            link_url = index.data(ChatModel.LinkUrlRole) or ""
            if filename:
                if link_url:
                    if not self._check_remote_file_exists(str(link_url)):
                        QtWidgets.QMessageBox.warning(self, "提示", "该文件已过期（超过服务器保留时间）被删除，无法下载")
                        return
                    QtGui.QDesktopServices.openUrl(QtCore.QUrl(str(link_url)))
                else:
                    QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(self._attachment_path(filename, self.current_conv)))
        elif kind == "msg":
            txt = index.data(ChatModel.TextRole) or ""
            self._open_text_viewer(txt)

    def eventFilter(self, obj, ev):
        if ev.type() == QtCore.QEvent.ApplicationActivate:
            try:
                if not self.isVisible():
                    self.show()
                    try:
                        self.raise_()
                        self.activateWindow()
                    except Exception:
                        pass
                # Clear unread count for current conversation when app becomes active
                if self.current_conv:
                    self._reset_unread(self.current_conv)
            except Exception:
                pass
            return False
        if ev.type() == QtCore.QEvent.WindowActivate:
             # Clear unread count for current conversation when window becomes active
             try:
                 if self.current_conv:
                     self._reset_unread(self.current_conv)
             except Exception:
                 pass
        if obj is self.view.viewport():
            if ev.type() == QtCore.QEvent.ContextMenu:
                self.on_view_context_menu(ev.pos())
                return True
            if ev.type() == QtCore.QEvent.MouseMove:
                idx = self.view.indexAt(ev.position().toPoint())
                want_ibeam = False
                if idx.isValid() and idx.data(ChatModel.KindRole) == "msg":
                    if self._bubble_contains(idx, ev.position().toPoint()):
                        want_ibeam = True
                if want_ibeam:
                    self.view.viewport().setCursor(QtGui.QCursor(QtCore.Qt.IBeamCursor))
                else:
                    self.view.viewport().unsetCursor()
                return False
            if ev.type() in (QtCore.QEvent.Leave, QtCore.QEvent.MouseButtonRelease):
                self.view.viewport().unsetCursor()
                return False
        return super().eventFilter(obj, ev)

    def copy_selected(self):
        idx = self.view.currentIndex()
        if idx.isValid():
            self._copy_index(idx)

    def _copy_index(self, index: QtCore.QModelIndex):
        if not index.isValid():
            return
        kind = index.data(ChatModel.KindRole)
        if kind == "msg":
            text = index.data(ChatModel.TextRole) or ""
            QtWidgets.QApplication.clipboard().setText(text)
        elif kind == "file":
            fn = index.data(ChatModel.FileNameRole) or ""
            url = index.data(ChatModel.LinkUrlRole) or ""
            if url:
                QtWidgets.QApplication.clipboard().setText(str(url))
            else:
                path = self._attachment_path(fn, self.current_conv)
                QtWidgets.QApplication.clipboard().setText(path if os.path.exists(path) else fn)

    def _bootstrap_local(self):
        peers = self.store.peers()
        for p in peers:
            self._add_conv_dm(p)
            self._set_online(p, False)
            try:
                if p != self.username:
                    self._try_local_peer_avatar(p)
            except Exception:
                pass
        try:
            profiles = _load_profiles(self.logger.log_dir)
            for name in profiles.keys():
                if name and name != self.username and name not in self.peer_avatars:
                    try:
                        self._try_local_peer_avatar(name)
                    except Exception:
                        pass
        except Exception:
            pass
        # preload group conv
        self._ensure_conv(f"group:{self.room}")
        missing = 0
        for sender, ts, kind, text, selfflag in self.store.recent(f"group:{self.room}", 100):
            if text.startswith("[LINK] "):
                try:
                    toks = text.split(" ")
                    url = toks[-1] if len(toks) >= 2 else ""
                    size = int(toks[-2]) if len(toks) >= 3 else 0
                    filename = " ".join(toks[1:-2]) if len(toks) >= 3 else (toks[1] if len(toks) > 1 else "")
                except Exception:
                    filename = ""
                    url = ""
                    size = 0
                av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                self.conv_models[f"group:{self.room}"].add_link(sender, filename, url, bool(selfflag), av, int(ts) if ts else None, size)
                continue
            if kind == "file" and text.startswith("[FILE] "):
                fn, mime, _ = self._parse_file(text)
                p = self._attachment_path(fn, f"group:{self.room}")
                pix = QtGui.QPixmap(p) if os.path.isfile(p) else None
                av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                try:
                    sz = os.path.getsize(p) if os.path.isfile(p) else None
                except Exception:
                    sz = None
                self.conv_models[f"group:{self.room}"].add_file(sender, fn, mime, pix if pix and not pix.isNull() else None, bool(selfflag), av, None, sz)
                if not (pix and not pix.isNull()):
                    missing += 1
            elif kind == "sys":
                self.conv_models[f"group:{self.room}"].add("sys", "", text, False, None)
            else:
                av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                self.conv_models[f"group:{self.room}"].add("msg", sender, text, bool(selfflag), av)
        # if group model empty or some images missing locally, request history to hydrate attachments
        try:
            m = self.conv_models.get(f"group:{self.room}")
            if (m and len(m.items) == 0) or missing > 0:
                self._send_seq("HIST GROUP 50")
        except Exception:
            pass

    def _send_seq(self, body: str, rid: Optional[str] = None):
        try:
            target_room = rid
            if not target_room:
                try:
                    if self.current_conv and self.current_conv.startswith("group:"):
                        target_room = self.current_conv.split(":",1)[1]
                except Exception:
                    target_room = None
            if not target_room:
                target_room = self.room
            payload = f"SEQ {self.seq} {body}\n".encode("utf-8")
            s = self.socks.get(target_room) if hasattr(self, 'socks') else None
            if s:
                s.sendall(payload)
            elif self.sock:
                self.sock.sendall(payload)
            self.seq += 1
        except Exception:
            pass

    def _send_ping(self):
        try:
            ts = str(QtCore.QDateTime.currentMSecsSinceEpoch())
            sent = False
            if hasattr(self, 'socks'):
                for rid, s in list(self.socks.items()):
                    try:
                        s.sendall((f"PING {ts}\n").encode("utf-8"))
                        sent = True
                    except Exception:
                        pass
            if not sent and self.sock:
                self.sock.sendall((f"PING {ts}\n").encode("utf-8"))
        except Exception:
            pass
    def _restart_room_socket(self, rid: Optional[str] = None):
        try:
            if rid:
                rx = self.receivers.get(rid)
                s = self.socks.get(rid)
                try:
                    if rx:
                        rx.stop()
                except Exception:
                    pass
                try:
                    if s:
                        s.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    if s:
                        s.close()
                except Exception:
                    pass
                try:
                    if rid in self.receivers:
                        del self.receivers[rid]
                except Exception:
                    pass
                try:
                    if rid in self.socks:
                        del self.socks[rid]
                except Exception:
                    pass
                try:
                    self._connect_room(rid)
                except Exception:
                    pass
            else:
                try:
                    if self.rx:
                        self.rx.stop()
                except Exception:
                    pass
                try:
                    if self.sock:
                        self.sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    if self.sock:
                        self.sock.close()
                except Exception:
                    pass
                try:
                    self._connect()
                except Exception:
                    pass
        except Exception:
            pass
    def _start_async_upload(self, path: str, override_name: Optional[str] = None):
        try:
            if not path or not os.path.isfile(path):
                return
            try:
                limit_bytes = int(getattr(self, "max_upload_bytes", 40 * 1024 * 1024))
                sz0 = os.path.getsize(path)
                if int(max(0, sz0)) > int(limit_bytes):
                    try:
                        QtWidgets.QMessageBox.warning(self, "发送文件", f"文件大小超过 {self._human_readable_size(limit_bytes)}（{self._human_readable_size(sz0)}），无法发送")
                    except Exception:
                        pass
                    return
            except Exception:
                pass
            name = override_name or os.path.basename(path)
            mime = self._guess_mime(path)
            rid = None
            uploader = None
            if self.current_conv and self.current_conv.startswith("dm:"):
                target = self.current_conv.split(":",1)[1]
                uploader = MultiConnFileUploader(self.host, self.port, self.username, self.room, "dm", target, path, 2, 1048576, (self.avatar_filename or ""), name, logger=self.logger)
            elif self.current_conv and self.current_conv.startswith("group:"):
                rid = self.current_conv.split(":",1)[1]
                uploader = MultiConnFileUploader(self.host, self.port, self.username, rid, "group", None, path, 2, 1048576, (self.avatar_filename or ""), name, logger=self.logger)
            else:
                rid = self.room
                uploader = MultiConnFileUploader(self.host, self.port, self.username, rid, "group", None, path, 2, 1048576, (self.avatar_filename or ""), name, logger=self.logger)
            total = 0
            try:
                total = os.path.getsize(path)
            except Exception:
                total = 0
            conv_key = self.current_conv or (f"group:{self.room}" if self.room else "")
            self._ensure_conv(conv_key)
            m = self.conv_models.get(conv_key)
            row = (len(m.items) - 1) if m else -1
            worker = uploader
            if m and row >= 0:
                try:
                    m.set_upload_progress(row, 0, total, "uploading")
                except Exception:
                    pass
                try:
                    if hasattr(self, "view") and self.view:
                        self.view.viewport().update()
                except Exception:
                    pass
            self.upload_workers[(conv_key, row)] = worker
            def _on_progress(sent, tot):
                try:
                    if m and row >= 0:
                        cur_state = m.data(m.index(row), ChatModel.UploadStateRole)
                        cur_alpha = m.data(m.index(row), ChatModel.UploadAlphaRole) or 0
                        if str(cur_state) == "canceled":
                            return
                        if int(max(0, sent)) < int(max(0, tot)):
                            m.set_upload_progress(row, sent, tot, "uploading")
                        else:
                            if (cur_state not in ("fading", "done")) and int(cur_alpha) <= 0:
                                m.set_upload_progress(row, sent, tot, "fading")
                                if not hasattr(self, "_fade_timers"):
                                    self._fade_timers = {}
                                key = (conv_key, row)
                                if key not in self._fade_timers:
                                    m.set_upload_alpha(row, 255)
                                    t = QtCore.QTimer(self)
                                    t.setInterval(30)
                                    def _tick():
                                        alpha = m.data(m.index(row), ChatModel.UploadAlphaRole) or 0
                                        na = max(0, int(alpha) - 25)
                                        m.set_upload_alpha(row, na)
                                        try:
                                            if hasattr(self, "view") and self.view:
                                                self.view.viewport().update()
                                        except Exception:
                                            pass
                                        if na <= 0:
                                            try:
                                                m.set_upload_progress(row, None, None, "done")
                                            except Exception:
                                                pass
                                            try:
                                                t.stop()
                                                t.deleteLater()
                                            except Exception:
                                                pass
                                            try:
                                                del self._fade_timers[key]
                                            except Exception:
                                                pass
                                    t.timeout.connect(_tick)
                                    self._fade_timers[key] = t
                                    t.start()
                        try:
                            if hasattr(self, "view") and self.view:
                                self.view.viewport().update()
                        except Exception:
                            pass
                except Exception:
                    pass
            def _on_finished(ok, err):
                try:
                    if not ok:
                        m2 = self.conv_models.get(conv_key)
                        if m2:
                            try:
                                msg = "已取消文件发送" if err == "已取消" else "文件发送失败"
                                m2.add("sys", "", msg, False, None)
                            except Exception:
                                pass
                        if (conv_key, row) in self.upload_workers:
                            try:
                                del self.upload_workers[(conv_key, row)]
                            except Exception:
                                pass
                    else:
                        self.logger.write("sent", self.username, f"[FILE] {name} {mime}")
                        if m and row >= 0:
                            try:
                                m.set_upload_progress(row, total, total, None)
                            except Exception:
                                pass
                        try:
                            self._copy_attachment_from_path(name, path, conv_key)
                        except Exception:
                            pass
                        if (conv_key, row) in self.upload_workers:
                            try:
                                del self.upload_workers[(conv_key, row)]
                            except Exception:
                                pass
                except Exception:
                    pass
            def _on_pause():
                try:
                    worker.pause_toggle()
                    if m and row >= 0:
                        m.set_upload_progress(row, None, None, "paused")
                except Exception:
                    pass
            def _on_cancel():
                try:
                    worker.cancel()
                    if m and row >= 0:
                        m.set_upload_progress(row, None, None, "canceled")
                        try:
                            m.set_upload_alpha(row, 0)
                        except Exception:
                            pass
                        try:
                            key2 = (conv_key, row)
                            t2 = self._fade_timers.get(key2)
                            if t2:
                                try:
                                    t2.stop()
                                    t2.deleteLater()
                                except Exception:
                                    pass
                                try:
                                    del self._fade_timers[key2]
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        try:
                            if hasattr(self, "view") and self.view:
                                self.view.viewport().update()
                        except Exception:
                            pass
                    try:
                        if self.current_conv and self.current_conv.startswith("dm:"):
                            target = self.current_conv.split(":",1)[1]
                            self._send_seq(f"DM {target} FILE_CANCEL {name}")
                        else:
                            rid = (self.current_conv.split(":",1)[1] if self.current_conv and self.current_conv.startswith("group:") else self.room)
                            if rid:
                                self._send_seq(f"MSG FILE_CANCEL {name}", rid)
                    except Exception:
                        pass
                except Exception:
                    pass
            worker.progress.connect(_on_progress)
            worker.finished.connect(_on_finished)
            # controls via context menu actions
            worker.start()
        except Exception:
            pass
    def _http_upload_group_file(self, path: str, rid: str, name: Optional[str] = None):
        try:
            url = f"http://{self.host}:34568/api/upload_file"
            boundary = "----XiaoCaiBoundary" + str(int(QtCore.QDateTime.currentMSecsSinceEpoch()))
            parts = []
            def _p(s: str):
                return s.encode("utf-8")
            parts.append(_p(f"--{boundary}\r\nContent-Disposition: form-data; name=\"room\"\r\n\r\n{rid}\r\n"))
            parts.append(_p(f"--{boundary}\r\nContent-Disposition: form-data; name=\"sender\"\r\n\r\n{self.username}\r\n"))
            fname = name or os.path.basename(path)
            parts.append(_p(f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{fname}\"\r\nContent-Type: application/octet-stream\r\n\r\n"))
            data = b""
            try:
                with open(path, "rb") as f:
                    data = f.read()
            except Exception:
                data = b""
            parts.append(data)
            parts.append(_p("\r\n"))
            parts.append(_p(f"--{boundary}--\r\n"))
            body = b"".join(parts)
            req = urllib.request.Request(url, data=body, headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(len(body))}, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    raw = resp.read()
                    try:
                        info = json.loads(raw.decode("utf-8"))
                    except Exception:
                        info = {}
                    try:
                        link_url = info.get("url") or ""
                        fname2 = info.get("file_name") or fname
                        fsize = int(info.get("size") or (len(data) if isinstance(data, (bytes, bytearray)) else 0))
                    except Exception:
                        link_url = ""
                        fname2 = fname
                        try:
                            fsize = len(data) if isinstance(data, (bytes, bytearray)) else 0
                        except Exception:
                            fsize = 0
                    if link_url:
                        try:
                            key = f"group:{rid}"
                            self._ensure_conv(key)
                            av = self.avatar_pixmap
                            m = self.conv_models.get(key)
                            if m:
                                try:
                                    for idx in range(len(m.items) - 1, -1, -1):
                                        it = m.items[idx]
                                        if it.get("kind") == "file" and it.get("sender") == self.username and it.get("filename") == fname2 and not it.get("link_url"):
                                            m.remove_row(idx)
                                            break
                                except Exception:
                                    pass
                                m.add_link(self.username, fname2, link_url, True, av, None, fsize)
                            try:
                                self.store.add(key, self.username, f"[LINK] {fname2} {fsize} {link_url}", "file", True)
                            except Exception:
                                pass
                            try:
                                if self.current_conv == key:
                                    self.view.scrollToBottom()
                            except Exception:
                                pass
                        except Exception:
                            pass
            except Exception:
                pass
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
    def _ensure_unique_filename(self, conv_key: Optional[str], sender: str, name: str) -> str:
        try:
            base, ext = os.path.splitext(name)
            att_dir = self._attachment_dir(conv_key)
            m = self.conv_models.get(conv_key) if conv_key else None
            i = 2
            candidate = name
            while True:
                exists_in_model = False
                if m:
                    for it in m.items:
                        if it.get("kind") == "file" and it.get("sender") == sender and it.get("filename") == candidate:
                            exists_in_model = True
                            break
                exists_on_disk = os.path.isfile(os.path.join(att_dir, candidate))
                if not exists_in_model and not exists_on_disk:
                    return candidate
                candidate = f"{base} ({i}){ext}"
                i += 1
        except Exception:
            return name

    def on_sidebar_message(self):
        try:
            self.msg_item.setSelected(True)
            try:
                self.group_item.setSelected(False)
            except Exception:
                pass
            self.setting_item.setSelected(False)
            self.conv_list.setVisible(True)
            self.right_stack.setCurrentIndex(0)
            self.view_mode = "message"
            try:
                self._rebuild_conv_list()
            except Exception:
                pass
            self.current_conv = None
            self.current_model = None
            self.chat_stack.setCurrentIndex(0)
            self.conv_list.setFocus()
            try:
                for name in list(self.pending_join_users | self.pending_dm_users):
                    if name and name != self.username:
                        self._add_conv_dm(name)
                self.pending_join_users.clear()
                self.pending_dm_users.clear()
                self._rebuild_conv_list()
            except Exception:
                pass
        except Exception:
            pass

    def on_sidebar_setting(self):
        try:
            self.setting_item.setSelected(True)
            self.msg_item.setSelected(False)
            try:
                self.group_item.setSelected(False)
            except Exception:
                pass
            self.conv_list.setVisible(False)
            self.right_stack.setCurrentIndex(1)
        except Exception:
            pass

    def _ensure_group_item(self):
        self._ensure_group_items()

    def _ensure_group_items(self):
        if not self.room_ready:
            try:
                return
            except Exception:
                return
        rooms = self.rooms_info or []
        # build items for all rooms
        for r in rooms:
            rid = str(r.get("id"))
            title = str(r.get("name") or rid)
            exists = False
            for i in range(self.conv_list.count()):
                it0 = self.conv_list.item(i)
                data0 = it0.data(QtCore.Qt.UserRole) if it0 else None
                if data0 == f"group:{rid}":
                    exists = True
                    break
            if not exists:
                it = QtWidgets.QListWidgetItem(title)
                it.setSizeHint(QtCore.QSize(200, 56))
                try:
                    it.setIcon(QtGui.QIcon())
                except Exception:
                    pass
                try:
                    it.setData(QtCore.Qt.UserRole, f"group:{rid}")
                except Exception:
                    pass
                self.conv_list.addItem(it)
                w = QtWidgets.QWidget()
                hl = QtWidgets.QHBoxLayout()
                try:
                    hl.setContentsMargins(12, 6, 12, 6)
                    hl.setSpacing(8)
                except Exception:
                    pass
                icon_lbl = QtWidgets.QLabel()
                icon_lbl.setFixedSize(28, 28)
                try:
                    pm = QtGui.QIcon(os.path.join(os.getcwd(), "icons", "user", "group.png")).pixmap(28, 28)
                    icon_lbl.setPixmap(pm)
                except Exception:
                    pass
                name_lbl = QtWidgets.QLabel(title)
                try:
                    name_lbl.setStyleSheet("QLabel{font:14px 'Helvetica Neue';}")
                except Exception:
                    pass
                badge_lbl = QtWidgets.QLabel()
                badge_lbl.setVisible(False)
                try:
                    badge_lbl.setStyleSheet("QLabel{background:#F44336;color:#fff;border-radius:8px;padding:0 6px;font:11px 'Helvetica Neue';}")
                except Exception:
                    pass
                hl.addWidget(icon_lbl)
                hl.addWidget(name_lbl, 1)
                hl.addWidget(badge_lbl)
                w.setLayout(hl)
                self.conv_list.setItemWidget(it, w)
                self.conv_badges[f"group:{rid}"] = badge_lbl
            try:
                self._ensure_unread_key(f"group:{rid}")
            except Exception:
                pass
        try:
            self._apply_conv_filter()
        except Exception:
            pass
        try:
            for r in rooms:
                rid = str(r.get("id"))
                self._update_conv_title(f"group:{rid}")
        except Exception:
            pass

    def on_sidebar_group(self):
        try:
            self.group_item.setSelected(True)
            self.msg_item.setSelected(False)
            self.setting_item.setSelected(False)
            self.conv_list.setVisible(True)
            self.right_stack.setCurrentIndex(0)
            self._ensure_group_items()
            self.view_mode = "group"
            try:
                self._rebuild_conv_list()
            except Exception:
                pass
            self.current_conv = None
            self.current_model = None
            self.chat_stack.setCurrentIndex(0)
            self.conv_list.setFocus()
        except Exception:
            pass

    def _make_settings_panel(self) -> QtWidgets.QWidget:
        w = QtWidgets.QWidget()
        v = QtWidgets.QVBoxLayout()
        try:
            v.setContentsMargins(12, 12, 12, 12)
            v.setSpacing(12)
        except Exception:
            pass
        form = QtWidgets.QFormLayout()
        self.host_edit = QtWidgets.QLineEdit(self.host)
        self.port_edit = QtWidgets.QSpinBox()
        try:
            self.port_edit.setRange(1, 65535)
            self.port_edit.setValue(int(self.port))
        except Exception:
            pass
        self.max_upload_mb_spin = QtWidgets.QSpinBox()
        try:
            self.max_upload_mb_spin.setRange(1, 2048)
            cur_mb = int(getattr(self, "max_upload_bytes", 40 * 1024 * 1024) // (1024 * 1024))
            self.max_upload_mb_spin.setValue(int(max(1, cur_mb)))
        except Exception:
            pass
        self.shortcut_edit = QtWidgets.QLineEdit()
        try:
            self.shortcut_edit.setPlaceholderText("Ctrl+Meta+S")
            self.shortcut_edit.setText(getattr(self, "screenshot_shortcut_seq", "Ctrl+Meta+S"))
        except Exception:
            pass
        apply_btn = QtWidgets.QPushButton("保存")
        clear_btn = QtWidgets.QPushButton("清除缓存")
        form.addRow("服务器 IP", self.host_edit)
        form.addRow("端口", self.port_edit)
        form.addRow("最大发送大小 (MB)", self.max_upload_mb_spin)
        form.addRow("截图快捷键", self.shortcut_edit)
        v.addLayout(form)
        # 移除状态未连接提示
        h = QtWidgets.QHBoxLayout()
        h.addStretch(1)
        h.addWidget(apply_btn)
        h.addWidget(clear_btn)
        v.addLayout(h)
        w.setLayout(v)
        def do_apply():
            try:
                new_host = self.host_edit.text().strip() or self.host
                new_port = int(self.port_edit.value())
                _save_client_config(os.path.expanduser("~/Library/Application Support/XiaoCaiChat/client_config.json"), new_host, new_port, self.room)
                self.host = new_host
                self.port = new_port
                try:
                    mb = int(self.max_upload_mb_spin.value())
                    self.max_upload_bytes = int(max(1, mb)) * 1024 * 1024
                except Exception:
                    pass
                try:
                    sc = self.shortcut_edit.text().strip()
                    if sc:
                        self.screenshot_shortcut_seq = sc
                        if hasattr(self, "screenshot_shortcut"):
                            self.screenshot_shortcut.setKey(QtGui.QKeySequence(sc))
                except Exception:
                    pass
                try:
                    cfg_path = os.path.expanduser("~/Library/Application Support/XiaoCaiChat/client_config.json")
                    data = {}
                    if os.path.isfile(cfg_path):
                        with open(cfg_path, "r", encoding="utf-8") as f:
                            try:
                                data = json.load(f) or {}
                            except Exception:
                                data = {}
                    data["max_upload_mb"] = int(max(1, self.max_upload_bytes // (1024 * 1024)))
                    data["screenshot_shortcut"] = getattr(self, "screenshot_shortcut_seq", "Ctrl+Meta+S")
                    with open(cfg_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
                try:
                    m = QtWidgets.QMessageBox(self)
                    m.setIcon(QtWidgets.QMessageBox.Information)
                    m.setWindowTitle("已保存")
                    m.setText("配置已保存")
                    m.exec()
                except Exception:
                    pass
            except Exception:
                pass
        def do_clear_all():
            try:
                res = QtWidgets.QMessageBox.question(self, "确认", "确定清除缓存日志？", QtWidgets.QMessageBox.Yes|QtWidgets.QMessageBox.No)
                if res == QtWidgets.QMessageBox.Yes:
                    # 仅清除日志文件
                    try:
                        log_dir = self.logger.log_dir if hasattr(self, 'logger') and self.logger else os.path.expanduser("~/Library/Application Support/XiaoCaiChat")
                        for fn in os.listdir(log_dir):
                            if fn.endswith('.log'):
                                try:
                                    os.remove(os.path.join(log_dir, fn))
                                except Exception:
                                    pass
                        # 同时尝试清理备用 logs 子目录
                        logs_sub = os.path.join(os.path.expanduser("~/Library/Application Support/XiaoCaiChat"), "logs")
                        if os.path.isdir(logs_sub):
                            for fn in os.listdir(logs_sub):
                                if fn.endswith('.log'):
                                    try:
                                        os.remove(os.path.join(logs_sub, fn))
                                    except Exception:
                                        pass
                    except Exception:
                        pass
                    try:
                        QtWidgets.QMessageBox.information(self, "完成", "缓存日志已清除")
                    except Exception:
                        pass
            except Exception:
                pass
        apply_btn.clicked.connect(do_apply)
        clear_btn.clicked.connect(do_clear_all)
        return w

    def _on_image_pasted(self, data: object, pm: object, mime: str, name: str):
        try:
            pixmap = pm if isinstance(pm, QtGui.QPixmap) else None
            self.pending_image_bytes = data if isinstance(data, (bytes, bytearray)) else None
            self.pending_image_mime = mime
            self.pending_image_name = name
            self.pending_image_pixmap = pixmap
            
        except Exception:
            pass

    def _on_file_chip_cleared(self):
        try:
            self.pending_image_bytes = None
            self.pending_image_mime = None
            self.pending_image_name = None
            self.pending_image_pixmap = None
            
        except Exception:
            pass

    def _parse_file(self, msg: str):
        s = msg.strip()
        if not s.startswith("[FILE] "):
            return "file", "application/octet-stream", ""
        tokens = s.split(" ")
        if len(tokens) < 3:
            return "file", "application/octet-stream", ""
        
        # Check if second to last token looks like a mime type (contains '/')
        # Case A: [FILE] name... mime b64 (Network msg or Sent msg in store)
        # Case B: [FILE] name... mime (Received msg in store)
        if len(tokens) >= 4 and "/" in tokens[-2]:
             mime = tokens[-2]
             b64 = tokens[-1]
             name = " ".join(tokens[1:-2])
        else:
             mime = tokens[-1]
             b64 = ""
             name = " ".join(tokens[1:-1])
        return name, mime, b64

    def _is_deleted(self, conv_key: str, kind: str, name_or_text: str, mime: Optional[str]) -> bool:
        try:
            if kind == "file":
                prefix = f"[FILE] {name_or_text} {mime}" if mime else f"[FILE] {name_or_text}"
            else:
                prefix = self._sanitize_text(name_or_text or "")
            return self.store.is_deleted(conv_key, kind, prefix)
        except Exception:
            return False

    def _extract_first_image_from_editor(self) -> Optional[bytes]:
        try:
            doc: QtGui.QTextDocument = self.entry.document()
            it = QtGui.QTextDocument.Iterator(doc)
        except Exception:
            pass
        try:
            cursor = QtGui.QTextCursor(doc)
            cursor.movePosition(QtGui.QTextCursor.Start)
            while not cursor.atEnd():
                fmt = cursor.charFormat()
                if fmt.isImageFormat():
                    imgfmt = QtGui.QTextImageFormat(fmt)
                    url = QtCore.QUrl(imgfmt.name())
                    res = doc.resource(QtGui.QTextDocument.ImageResource, url)
                    if isinstance(res, QtGui.QImage) and not res.isNull():
                        buf = QtCore.QBuffer()
                        buf.open(QtCore.QIODevice.WriteOnly)
                        res.save(buf, "PNG")
                        return bytes(buf.data())
                cursor.movePosition(QtGui.QTextCursor.NextCharacter)
            # fallback: parse html data uri
            html = self.entry.toHtml()
            if "data:image" in html and "base64," in html:
                start = html.find("base64,") + len("base64,")
                end = html.find("'", start)
                b64 = html[start:end] if end != -1 else html[start:]
                return base64.b64decode(b64)
        except Exception:
            pass
        return None

    def _extract_first_file_url_from_text(self, s: str) -> Optional[str]:
        try:
            t = s or ""
            if "file://" in t:
                i = t.find("file://")
                end = len(t)
                for sep in [" \n", "\n", " "]:
                    j = t.find(sep, i)
                    if j != -1:
                        end = min(end, j)
                url = t[i:end]
                qurl = QtCore.QUrl(url)
                p = qurl.toLocalFile()
                return p if p else None
        except Exception:
            return None
        return None

    def _sanitize_text(self, s: str) -> str:
        try:
            t = (s or "")
            # remove object replacement chars used for embedded images
            t = t.replace("\uFFFC", "")
            # remove zero-width spaces
            t = t.replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
            t = t.replace("\\n", "\n")
            t = t.strip()
            return t
        except Exception:
            return s or ""

    def _human_readable_size(self, n: int) -> str:
        try:
            units = ["B", "KB", "MB", "GB", "TB"]
            i = 0
            f = float(max(0, int(n)))
            while f >= 1024.0 and i < len(units) - 1:
                f /= 1024.0
                i += 1
            return ("{:.1f}{}".format(f, units[i]) if i > 0 else "{}{}".format(int(f), units[i]))
        except Exception:
            return "0B"

    def _add_file_from_path(self, conv_key: str, sender: str, path: str, is_self: bool):
        if not path or not os.path.isfile(path):
            return
        try:
            with open(path, "rb") as f:
                data = f.read()
            name = os.path.basename(path)
            mime = self._guess_mime(path)
            b64 = base64.b64encode(data).decode("ascii")
            pix = QtGui.QPixmap(path)
            self._save_attachment(name, b64, conv_key)
            self._ensure_conv(conv_key)
            av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
            self.conv_models[conv_key].add_file(sender, name, mime, pix if not pix.isNull() else None, is_self, av, None, len(data))
            self.store.add(conv_key, sender, f"[FILE] {name} {mime}", "file", is_self)
        except Exception:
            pass

    def _save_attachment(self, filename: str, b64: str, conv_key: Optional[str] = None):
        att_dir = self._attachment_dir(conv_key)
        os.makedirs(att_dir, exist_ok=True)
        try:
            data = base64.b64decode(b64)
            with open(os.path.join(att_dir, filename), "wb") as f:
                f.write(data)
        except Exception:
            pass
    def _save_attachment_async(self, filename: str, b64: str, conv_key: Optional[str] = None):
        att_dir = self._attachment_dir(conv_key)
        os.makedirs(att_dir, exist_ok=True)
        class _Task(QtCore.QRunnable):
            def __init__(self, owner, dirp: str, fname: str, payload: str):
                super().__init__()
                self.owner = owner
                self.dirp = dirp
                self.fname = fname
                self.payload = payload
            def run(self):
                try:
                    data = base64.b64decode(self.payload)
                    with open(os.path.join(self.dirp, self.fname), "wb") as f:
                        f.write(data)
                    try:
                        if hasattr(self.owner, "logger") and self.owner.logger:
                            self.owner.logger.write("recv", self.owner.username, f"FILE_SAVE path={os.path.join(self.dirp, self.fname)} size={len(data)}")
                    except Exception:
                        pass
                except Exception:
                    pass
        try:
            QtCore.QThreadPool.globalInstance().start(_Task(self, att_dir, filename, b64))
        except Exception:
            pass
    def _copy_attachment_from_path(self, filename: str, src_path: str, conv_key: Optional[str] = None):
        att_dir = self._attachment_dir(conv_key)
        os.makedirs(att_dir, exist_ok=True)
        try:
            dst = os.path.join(att_dir, filename)
            shutil.copyfile(src_path, dst)
        except Exception:
            pass
    def _delete_part_globally(self, filename: str):
        try:
            base = self._attachment_dir(None)
            for entry in os.listdir(base):
                partp = os.path.join(base, entry, filename + ".part")
                if os.path.isfile(partp):
                    try:
                        os.remove(partp)
                    except Exception:
                        pass
            try:
                for k in list(self._rx_files.keys()):
                    if k and len(k) >= 3 and k[2] == filename:
                        try:
                            del self._rx_files[k]
                        except Exception:
                            pass
            except Exception:
                pass
        except Exception:
            pass
    def _rx_write_chunk_async(self, conv_key: str, sender: str, filename: str, part_path: str, total: int, offset: int, b64: str):
        class _Task(QtCore.QRunnable):
            def __init__(self, owner, conv_key: str, sender: str, filename: str, path: str, total: int, off: int, payload: str):
                super().__init__()
                self.owner = owner
                self.conv_key = conv_key
                self.sender = sender
                self.filename = filename
                self.path = path
                self.total = int(max(0, total))
                self.off = int(max(0, off))
                self.payload = payload
            def run(self):
                try:
                    key = (self.conv_key, self.sender, self.filename)
                    try:
                        if (hasattr(self.owner, "_rx_files") and key not in self.owner._rx_files):
                            return
                    except Exception:
                        pass
                    data = None
                    try:
                        data = base64.b64decode(self.payload)
                    except Exception:
                        try:
                            data = base64.b64decode(self.payload.encode("ascii"), validate=False)
                        except Exception:
                            data = b""
                    if not isinstance(data, (bytes, bytearray)):
                        data = b""
                    try:
                        if not os.path.isfile(self.path):
                            with open(self.path, "wb") as _f:
                                _f.write(b"")
                    except Exception:
                        pass
                    mode = "r+b"
                    try:
                        cur_sz = os.path.getsize(self.path) if os.path.isfile(self.path) else 0
                    except Exception:
                        cur_sz = 0
                    try:
                        if self.off == cur_sz:
                            mode = "ab"
                    except Exception:
                        mode = "r+b"
                    with open(self.path, mode) as f:
                        if mode != "ab":
                            try:
                                f.seek(self.off)
                            except Exception:
                                f.seek(0, os.SEEK_END)
                        f.write(data)
                        try:
                            f.flush()
                        except Exception:
                            pass
                        try:
                            os.fsync(f.fileno())
                        except Exception:
                            pass
                    try:
                        d = self.owner._rx_files.get(key) or {}
                        chunks = d.get("chunks")
                        if isinstance(chunks, dict):
                            chunks[self.off] = len(data)
                    except Exception:
                        pass
                    try:
                        d = self.owner._rx_files.get(key) or {}
                        md5 = d.get("md5")
                        wrote = len(data)
                        if md5 and wrote > 0:
                            if self.conv_key.startswith("dm:"):
                                peer = self.sender
                                try:
                                    self.owner._send_seq(f"DM {peer} FILE_ACK {md5} {int(max(0,self.off))} {int(max(0,wrote))}")
                                except Exception:
                                    pass
                            elif self.conv_key.startswith("group:"):
                                try:
                                    rid = self.conv_key.split(":",1)[1]
                                    self.owner._send_seq(f"MSG FILE_ACK {md5} {int(max(0,self.off))} {int(max(0,wrote))}", rid)
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    try:
                        if hasattr(self.owner, "logger") and self.owner.logger:
                            self.owner.logger.write("recv", self.sender, f"FILE_CHUNK_WRITE part={self.path} off={int(max(0,self.off))} wrote={len(data)}")
                    except Exception:
                        pass
                    try:
                        if self.total > 0:
                            try:
                                sz = os.path.getsize(self.path)
                            except Exception:
                                sz = 0
                            if sz >= self.total:
                                self.owner._rx_file_end(self.conv_key, self.sender, self.filename)
                    except Exception:
                        pass
                except FileNotFoundError:
                    return
                except Exception:
                    pass
        try:
            QtCore.QThreadPool.globalInstance().start(_Task(self, conv_key, sender, filename, part_path, int(max(0,total)), offset, b64))
        except Exception:
            pass

    def _attachment_dir(self, conv_key: Optional[str] = None) -> str:
        base = os.path.join(self.store.root, "attachments")
        if not conv_key:
            return base
        safe = (
            conv_key.replace(":", "_")
            .replace("/", "_")
            .replace("&", "_")
        )
        return os.path.join(base, safe)

    def _attachment_path(self, filename: str, conv_hint: Optional[str] = None) -> str:
        # try conv-specific path first
        if conv_hint:
            p = os.path.join(self._attachment_dir(conv_hint), filename)
            if os.path.isfile(p):
                return p
        # fallback to current conv
        if hasattr(self, "current_conv") and self.current_conv:
            p = os.path.join(self._attachment_dir(self.current_conv), filename)
            if os.path.isfile(p):
                return p
        # flat attachments dir
        p = os.path.join(self._attachment_dir(None), filename)
        if os.path.isfile(p):
            return p
        # search subfolders
        try:
            base = self._attachment_dir(None)
            for entry in os.listdir(base):
                cand = os.path.join(base, entry, filename)
                if os.path.isfile(cand):
                    return cand
        except Exception:
            pass
        return p
    def _rx_file_begin(self, conv_key: str, sender: str, filename: str, mime: str, total: int):
        att_dir = self._attachment_dir(conv_key)
        os.makedirs(att_dir, exist_ok=True)
        part = os.path.join(att_dir, filename + ".part")
        try:
            with open(part, "wb") as f:
                pass
        except Exception:
            pass
        self._rx_files[(conv_key, sender, filename)] = {"mime": mime, "total": int(max(0, total)), "part": part, "chunks": {}, "md5": None}
        try:
            if hasattr(self, "logger") and self.logger:
                self.logger.write("recv", sender, f"RX_BEGIN conv={conv_key} name={filename} mime={mime} total={int(max(0,total))} part={part}")
        except Exception:
            pass
    def _rx_file_chunk(self, conv_key: str, sender: str, filename: str, offset: int, b64: str):
        key = (conv_key, sender, filename)
        d = self._rx_files.get(key)
        if not d:
            return
        try:
            if hasattr(self, "logger") and self.logger:
                self.logger.write("recv", sender, f"RX_CHUNK conv={conv_key} name={filename} off={int(max(0,offset))} len={len(b64)}")
        except Exception:
            pass
        self._rx_write_chunk_async(conv_key, sender, filename, d["part"], int(d.get("total") or 0), offset, b64)
    def _rx_file_end(self, conv_key: str, sender: str, filename: str):
        key = (conv_key, sender, filename)
        d = self._rx_files.get(key)
        if not d:
            return
        if key in getattr(self, "_finalizing_files", set()):
            return
        try:
            self._finalizing_files.add(key)
        except Exception:
            pass
        att_dir = self._attachment_dir(conv_key)
        dst = os.path.join(att_dir, filename)
        def _contiguous_prefix_size() -> int:
            try:
                chunks = d.get("chunks") or {}
                file_sz = 0
                try:
                    file_sz = os.path.getsize(d.get("part") or "")
                except Exception:
                    file_sz = 0
                if not isinstance(chunks, dict) or not chunks:
                    return int(max(0, file_sz))
                off = 0
                while True:
                    ln = chunks.get(off)
                    if not ln or ln <= 0:
                        break
                    off += int(max(0, ln))
                return int(max(0, max(off, file_sz)))
            except Exception:
                try:
                    return os.path.getsize(d.get("part") or "")
                except Exception:
                    return 0
        def _do_update_and_cleanup():
            try:
                if hasattr(self, "logger") and self.logger:
                    sz = 0
                    try:
                        sz = os.path.getsize(dst)
                    except Exception:
                        sz = 0
                    self.logger.write("recv", sender, f"RX_END conv={conv_key} name={filename} dst={dst} size={sz}")
            except Exception:
                pass
            # Update existing bubble if present; else add new bubble
            try:
                mime = d.get("mime") or ""
                pix = QtGui.QPixmap(dst) if mime.startswith("image/") else None
                self._ensure_conv(conv_key)
                m = self.conv_models.get(conv_key)
                av = self.avatar_pixmap if sender == self.username else self.peer_avatars.get(sender)
                is_self = (sender == self.username)
                final_size = None
                try:
                    final_size = os.path.getsize(dst)
                except Exception:
                    final_size = None
                found_row = -1
                if m:
                    for i, it in enumerate(m.items):
                        if it.get("kind") == "file" and it.get("filename") == filename and it.get("sender") == sender:
                            found_row = i
                            break
                if m and found_row >= 0:
                    try:
                        it = m.items[found_row]
                        it["filesize"] = final_size
                        if pix and not pix.isNull():
                            it["pixmap"] = pix
                        top = m.index(found_row)
                        bottom = m.index(found_row)
                        m.dataChanged.emit(top, bottom)
                    except Exception:
                        pass
                else:
                    try:
                        m.add_file(sender, filename, mime, pix if pix and not pix.isNull() else None, is_self, av, None, final_size)
                        self.store.add(conv_key, sender, f"[FILE] {filename} {mime}", "file", is_self)
                    except Exception:
                        pass
                try:
                    if hasattr(self, "view") and self.view:
                        self.view.scrollToBottom()
                except Exception:
                    pass
            except Exception:
                pass
            try:
                if key in self._finalizing_files:
                    self._finalizing_files.remove(key)
            except Exception:
                pass
            try:
                if key in self._rx_files:
                    del self._rx_files[key]
            except Exception:
                pass
        def _attempt_finalize():
            partp = d.get("part")
            total = int(d.get("total") or 0)
            cur = _contiguous_prefix_size()
            part_sz = 0
            try:
                part_sz = os.path.getsize(partp) if partp and os.path.isfile(partp) else 0
            except Exception:
                part_sz = 0
            if partp and os.path.isfile(partp) and (total <= 0 or cur >= total or part_sz >= total):
                try:
                    if os.path.isfile(dst):
                        try:
                            os.remove(dst)
                        except Exception:
                            pass
                    try:
                        shutil.move(partp, dst)
                    except Exception:
                        try:
                            shutil.copyfile(partp, dst)
                            try:
                                os.remove(partp)
                            except Exception:
                                pass
                        except Exception:
                            return False
                except Exception:
                    return False
                _do_update_and_cleanup()
                return True
            return False
        if not _attempt_finalize():
            try:
                max_tries = 200  # ~30s with 150ms interval
                tries = [0]
                def _tick():
                    if _attempt_finalize():
                        return
                    tries[0] += 1
                    try:
                        cur = _contiguous_prefix_size()
                        md5 = d.get("md5")
                        if md5 and cur > 0:
                            if conv_key.startswith("dm:"):
                                peer = sender
                                try:
                                    self._send_seq(f"DM {peer} FILE_HAVE {md5} {cur} PARTIAL")
                                except Exception:
                                    pass
                            elif conv_key.startswith("group:"):
                                try:
                                    rid = conv_key.split(":",1)[1]
                                    self._send_seq(f"MSG FILE_HAVE {md5} {cur} PARTIAL", rid)
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    if tries[0] < max_tries:
                        QtCore.QTimer.singleShot(150, _tick)
                    else:
                        pass
                QtCore.QTimer.singleShot(150, _tick)
            except Exception:
                try:
                    _do_update_and_cleanup()
                except Exception:
                    pass

    def _open_text_viewer(self, text: str):
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("查看/复制")
        edit = QtWidgets.QPlainTextEdit()
        edit.setPlainText(text)
        edit.setReadOnly(True)
        edit.setLineWrapMode(QtWidgets.QPlainTextEdit.WidgetWidth)
        try:
            edit.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
            edit.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
            edit.setStyleSheet(
                "QScrollBar:horizontal{height:0px;}"
                "QScrollBar:vertical{width:8px;background:transparent;margin:0;}"
                "QScrollBar::handle:vertical{background:#cfd8dc;border-radius:4px;min-height:24px;}"
                "QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical{height:0;}"
                "QScrollBar::up-arrow:vertical,QScrollBar::down-arrow:vertical{height:0;width:0;}"
            )
            edit.moveCursor(QtGui.QTextCursor.End)
        except Exception:
            pass
        btn_copy_sel = QtWidgets.QPushButton("复制选中")
        btn_copy_all = QtWidgets.QPushButton("复制全部")
        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(edit)
        h = QtWidgets.QHBoxLayout()
        h.addStretch(1)
        h.addWidget(btn_copy_sel)
        h.addWidget(btn_copy_all)
        layout.addLayout(h)
        dlg.setLayout(layout)
        def do_copy_sel():
            QtWidgets.QApplication.clipboard().setText(edit.textCursor().selectedText())
        def do_copy_all():
            QtWidgets.QApplication.clipboard().setText(text)
        btn_copy_sel.clicked.connect(do_copy_sel)
        btn_copy_all.clicked.connect(do_copy_all)
        dlg.resize(600, 400)
        dlg.exec()

    def _text_pos_from_event(self, index: QtCore.QModelIndex, vp_pos: QtCore.QPoint):
        r = self.view.visualRect(index)
        kind = index.data(ChatModel.KindRole)
        if kind != "msg":
            return None
        text = index.data(ChatModel.TextRole) or ""
        is_self = bool(index.data(ChatModel.SelfRole))
        fm = self.view.fontMetrics()
        maxw = int(r.width() * 0.65)
        doc = QtGui.QTextDocument()
        opt = QtGui.QTextOption()
        opt.setWrapMode(QtGui.QTextOption.WrapAtWordBoundaryOrAnywhere)
        doc.setDefaultFont(option.font)
        doc.setDefaultTextOption(opt)
        w0 = fm.horizontalAdvance(text)
        text_w = min(w0 + 6, maxw)
        doc.setTextWidth(text_w)
        doc.setPlainText(text)
        pad = 12
        bubble_w = int(text_w) + pad * 2
        bubble_h = int(doc.size().height()) + pad * 2
        margin = 10
        avatar_size = 22
        avatar_pad = 8
        bubble_x = r.right() - bubble_w - margin - avatar_size - avatar_pad if is_self else r.left() + margin + avatar_size + avatar_pad
        bubble_y = r.top() + 26
        text_rect = QtCore.QRect(bubble_x + pad, bubble_y + pad, bubble_w - 2*pad, bubble_h - 2*pad)
        if not text_rect.contains(vp_pos):
            return None
        local = QtCore.QPointF(vp_pos.x() - text_rect.x(), vp_pos.y() - text_rect.y())
        # clamp to content area to ensure hitTest returns a position
        local.setX(max(0.0, min(local.x(), float(text_rect.width() - 1))))
        local.setY(max(0.0, min(local.y(), float(text_rect.height() - 1))))
        layout = doc.documentLayout()
        try:
            pos = layout.hitTest(local, QtCore.Qt.FuzzyHit)
            if pos < 0:
                # fallback to nearest edge
                if local.x() <= 0.0 and local.y() <= 0.0:
                    return 0
                return len(text)
            return pos
        except Exception:
            return None

    def _bubble_contains(self, index: QtCore.QModelIndex, vp_pos: QtCore.QPoint) -> bool:
        r = self.view.visualRect(index)
        kind = index.data(ChatModel.KindRole)
        if kind != "msg":
            return False
        text = index.data(ChatModel.TextRole) or ""
        is_self = bool(index.data(ChatModel.SelfRole))
        fm = self.view.fontMetrics()
        maxw = int(r.width() * 0.65)
        doc = QtGui.QTextDocument()
        opt = QtGui.QTextOption()
        opt.setWrapMode(QtGui.QTextOption.WrapAtWordBoundaryOrAnywhere)
        doc.setDefaultFont(self.view.font())
        doc.setDefaultTextOption(opt)
        w0 = fm.horizontalAdvance(text)
        text_w = min(w0, maxw)
        doc.setTextWidth(text_w)
        doc.setPlainText(text)
        pad = 12
        bubble_w = int(text_w) + pad * 2
        bubble_h = int(doc.size().height()) + pad * 2
        margin = 10
        avatar_size = 22
        avatar_pad = 8
        bubble_x = r.right() - bubble_w - margin - avatar_size - avatar_pad if is_self else r.left() + margin + avatar_size + avatar_pad
        bubble_y = r.top() + 26
        bubble_rect = QtCore.QRect(bubble_x, bubble_y, bubble_w, bubble_h)
        return bubble_rect.contains(vp_pos)

    def closeEvent(self, e: QtGui.QCloseEvent):
        try:
            e.ignore()
            self.hide()
        except Exception:
            pass

    def _on_app_quit(self):
        try:
            for key, w in list(self.upload_workers.items()):
                try:
                    w.cancel()
                except Exception:
                    pass
            try:
                self.upload_workers.clear()
            except Exception:
                pass
        except Exception:
            pass


def parse_args():
    p = argparse.ArgumentParser(prog="qt_chat_client", add_help=True)
    p.add_argument("--username", type=str, default=getpass.getuser())
    p.add_argument("--log-dir", type=str, default=os.path.expanduser("~/Library/Application Support/XiaoCaiChat"))
    p.add_argument("--theme", type=str, default="flat")
    p.add_argument("--config", type=str, default=os.path.expanduser("~/Library/Application Support/XiaoCaiChat/client_config.json"))
    return p.parse_args()

def _load_client_config(path: str):
    try:
        defaults = {"host": "127.0.0.1", "port": 34567, "room": "世界", "theme": "flat"}
        candidates = []
        if path:
            candidates.append(path)
        # user-level config
        candidates.append(os.path.expanduser("~/Library/Application Support/XiaoCaiChat/client_config.json"))
        # app bundle dir
        try:
            app_dir = os.path.dirname(sys.argv[0])
            candidates.append(os.path.join(app_dir, "client_config.json"))
        except Exception:
            pass
        # bundled resources (current working dir may be sys._MEIPASS)
        candidates.append(os.path.join(os.getcwd(), "client_config.json"))
        for pth in candidates:
            if pth and os.path.isfile(pth):
                with open(pth, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                for k in defaults:
                    if k in data:
                        defaults[k] = data[k]
                break
        return defaults["host"], int(defaults["port"]), defaults["room"], defaults.get("theme")
    except Exception:
        return "127.0.0.1", 34567, "世界", "flat"

def _save_client_config(path: str, host: str, port: int, room: str, theme: Optional[str] = None):
    try:
        p = path or os.path.expanduser("~/Library/Application Support/XiaoCaiChat/client_config.json")
        ddir = os.path.dirname(p)
        os.makedirs(ddir, exist_ok=True)
        data = {"host": host, "port": int(port), "room": room}
        if theme:
            data["theme"] = theme
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _ensure_user_config(path: str):
    try:
        p = path or os.path.expanduser("~/Library/Application Support/XiaoCaiChat/client_config.json")
        ddir = os.path.dirname(p)
        os.makedirs(ddir, exist_ok=True)
        defaults = {"host": "127.0.0.1", "port": 34567, "room": "世界", "theme": "flat", "max_upload_mb": 40}
        if os.path.isfile(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
            except Exception:
                data = {}
            changed = False
            for k, v in defaults.items():
                if k not in data:
                    data[k] = v
                    changed = True
            if changed:
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(defaults, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def main():
    args = parse_args()
    base_dir = getattr(sys, "_MEIPASS", os.getcwd())
    try:
        os.chdir(base_dir)
    except Exception:
        pass
    try:
        import signal
        def _sig_handler(*_):
            try:
                QtWidgets.QApplication.quit()
            except Exception:
                pass
        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)
    except Exception:
        pass
    try:
        _ensure_user_config(args.config)
    except Exception:
        pass
    app = QtWidgets.QApplication([])
    try:
        app.setQuitOnLastWindowClosed(False)
    except Exception:
        pass
    try:
        app.setWindowIcon(QtGui.QIcon(os.path.join(os.getcwd(), "icons", "ui", "xiaocaichat.png")))
    except Exception:
        pass
    icon_dir = os.path.join(os.getcwd(), "icons", "user")
    dlg = QtWidgets.QDialog()
    dlg.setWindowTitle("登录")
    name_edit = QtWidgets.QLineEdit()
    name_edit.setPlaceholderText("用户名")
    try:
        name_edit.setText(args.username)
    except Exception:
        pass
    prof_list = QtWidgets.QListWidget()
    prof_list.setIconSize(QtCore.QSize(48, 48))
    prof_list.setViewMode(QtWidgets.QListView.IconMode)
    prof_list.setResizeMode(QtWidgets.QListView.Adjust)
    prof_list.setMovement(QtWidgets.QListView.Static)
    prof_list.setSpacing(6)
    prof_list.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
    listw = QtWidgets.QListWidget()
    listw.setIconSize(QtCore.QSize(64, 64))
    listw.setViewMode(QtWidgets.QListView.IconMode)
    listw.setResizeMode(QtWidgets.QListView.Adjust)
    listw.setMovement(QtWidgets.QListView.Static)
    listw.setSpacing(8)
    btn_ok = QtWidgets.QPushButton("进入")
    btn_cancel = QtWidgets.QPushButton("取消")
    v = QtWidgets.QVBoxLayout()
    form = QtWidgets.QFormLayout()
    form.addRow("用户名", name_edit)
    hl_recent = QtWidgets.QHBoxLayout()
    lbl_recent = QtWidgets.QLabel("最近登录用户")
    btn_new = QtWidgets.QPushButton("新用户")
    try:
        hl_recent.addWidget(lbl_recent)
        hl_recent.addStretch(1)
        hl_recent.addWidget(btn_new)
    except Exception:
        pass
    v.addLayout(hl_recent)
    login_stack = QtWidgets.QStackedWidget()
    recent_page = QtWidgets.QWidget()
    vr = QtWidgets.QVBoxLayout()
    vr.addWidget(prof_list)
    recent_page.setLayout(vr)
    new_page = QtWidgets.QWidget()
    vn = QtWidgets.QVBoxLayout()
    vn.addLayout(form)
    vn.addWidget(QtWidgets.QLabel("选择头像"))
    vn.addWidget(listw, 1)
    new_page.setLayout(vn)
    login_stack.addWidget(recent_page)
    login_stack.addWidget(new_page)
    v.addWidget(login_stack, 1)
    h = QtWidgets.QHBoxLayout()
    h.addStretch(1)
    h.addWidget(btn_ok)
    h.addWidget(btn_cancel)
    v.addLayout(h)
    dlg.setLayout(v)
    # load profiles
    profiles = _load_profiles(args.log_dir)
    try:
        for uname, afn in profiles.items():
            path = os.path.join(icon_dir, afn) if afn else None
            if path and os.path.exists(path):
                it = QtWidgets.QListWidgetItem(QtGui.QIcon(path), uname)
            else:
                # fallback letter icon
                pm = QtGui.QPixmap(48, 48)
                pm.fill(QtCore.Qt.transparent)
                p = QtGui.QPainter(pm)
                p.setRenderHint(QtGui.QPainter.Antialiasing, True)
                hue = (sum(ord(c) for c in uname) % 360)
                color = QtGui.QColor.fromHsl(hue, 160, 160)
                p.setBrush(color)
                p.setPen(QtCore.Qt.NoPen)
                p.drawEllipse(0, 0, 48, 48)
                p.setPen(QtGui.QColor(255, 255, 255))
                f = p.font(); f.setBold(True); p.setFont(f)
                p.drawText(QtCore.QRect(0, 0, 48, 48), QtCore.Qt.AlignCenter, uname[:1])
                p.end()
                it = QtWidgets.QListWidgetItem(QtGui.QIcon(pm), uname)
            it.setData(QtCore.Qt.UserRole, os.path.join(icon_dir, afn) if afn else None)
            prof_list.addItem(it)
    except Exception:
        pass
    try:
        prof_list.clearSelection()
        prof_list.setCurrentRow(-1)
    except Exception:
        pass
    try:
        for fn in sorted(os.listdir(icon_dir)):
            path = os.path.join(icon_dir, fn)
            if os.path.isfile(path) and fn.lower().endswith((".png", ".jpg", ".jpeg")):
                it = QtWidgets.QListWidgetItem(QtGui.QIcon(path), "")
                it.setData(QtCore.Qt.UserRole, path)
                listw.addItem(it)
    except Exception:
        pass
    btn_ok.clicked.connect(dlg.accept)
    btn_cancel.clicked.connect(dlg.reject)
    listw.itemDoubleClicked.connect(lambda *_: dlg.accept())
    prof_list.itemDoubleClicked.connect(lambda *_: dlg.accept())
    prof_list.itemClicked.connect(lambda it: name_edit.setText(it.text()))
    def do_new_user():
        try:
            prof_list.clearSelection()
            prof_list.setCurrentRow(-1)
        except Exception:
            pass
        try:
            name_edit.clear()
            name_edit.setFocus()
        except Exception:
            pass
        try:
            login_stack.setCurrentIndex(1)
        except Exception:
            pass
    btn_new.clicked.connect(do_new_user)
    if dlg.exec() != QtWidgets.QDialog.Accepted:
        return
    # prefer profile selection only if user explicitly selected
    sel = prof_list.selectedItems()
    if sel:
        pitem = sel[0]
        name = pitem.text().strip() or args.username
        avatar = pitem.data(QtCore.Qt.UserRole)
    else:
        name = name_edit.text().strip()
        if not name:
            QtWidgets.QMessageBox.warning(None, "提示", "请输入用户名")
            return
        it = listw.currentItem()
        avatar = it.data(QtCore.Qt.UserRole) if it else None
    try:
        _save_profile(args.log_dir, name, os.path.basename(avatar) if avatar else None)
    except Exception:
        pass
    host, port, room, theme_cfg = _load_client_config(args.config)
    _apply_theme(app, theme_cfg or args.theme)
    win = ChatWindow(host, port, name, args.log_dir, room, avatar)
    try:
        app.installEventFilter(win)
        app.aboutToQuit.connect(win.cleanup)
        import atexit
        atexit.register(win.cleanup)
    except Exception:
        pass
    win.resize(700, 500)
    win.show()
    app.exec()

def _load_profiles(base_dir: str):
    try:
        os.makedirs(base_dir, exist_ok=True)
        p = os.path.join(base_dir, "profiles.json")
        data = {}
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f) or {}
                except Exception:
                    data = {}
        # fallback: scan subdirectories as usernames
        try:
            for entry in os.listdir(base_dir):
                d = os.path.join(base_dir, entry)
                if os.path.isdir(d) and entry not in data:
                    if entry == "attachments":
                        continue
                    if os.path.isfile(os.path.join(d, "local.db")):
                        data[entry] = ""
        except Exception:
            pass
        return data
    except Exception:
        return {}

def _save_profile(base_dir: str, username: str, avatar_filename: Optional[str]):
    try:
        os.makedirs(base_dir, exist_ok=True)
        p = os.path.join(base_dir, "profiles.json")
        d = {}
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                try:
                    d = json.load(f)
                except Exception:
                    d = {}
        d[username] = avatar_filename or ""
        with open(p, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _apply_theme(app: QtWidgets.QApplication, name: str):
    base = os.path.join(os.getcwd(), "themes", f"{name}.qss")
    if os.path.exists(base):
        try:
            with open(base, "r", encoding="utf-8") as f:
                app.setStyleSheet(f.read())
        except Exception:
            pass


if __name__ == "__main__":
    main()
    def cleanup(self):
        try:
            if hasattr(self, 'rx') and self.rx:
                try:
                    self.rx.stop()
                    self.rx.wait(2000)
                    try:
                        self.rx.deleteLater()
                    except Exception:
                        pass
                    self.rx = None
                except Exception:
                    pass
            try:
                if hasattr(self, 'hb') and self.hb:
                    self.hb.stop()
            except Exception:
                pass
            if self.sock:
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
