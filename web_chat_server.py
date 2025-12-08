import json
import threading
import time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import argparse
import os
from datetime import datetime


class ChatStore:
    def __init__(self, log_dir: str):
        self.messages = []
        self.lock = threading.Lock()
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)
        date = datetime.now().strftime("%Y%m%d")
        self.log_path = os.path.join(self.log_dir, f"webchat_{date}.log")

    def append(self, username: str, text: str):
        ts = time.time()
        with self.lock:
            self.messages.append({"ts": ts, "username": username, "text": text})
            line = f"[{datetime.now().strftime('%H:%M:%S')}] {username}: {text}\n"
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line)

    def since(self, idx: int):
        with self.lock:
            return list(self.messages[idx:])


INDEX_HTML = """
<!doctype html>
<html lang=zh>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>内网群聊</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial; margin: 0; }
    header { background: #1f2937; color: #fff; padding: 12px 16px; }
    main { padding: 12px 16px; }
    #log { border: 1px solid #ddd; height: 60vh; overflow-y: auto; padding: 8px; border-radius: 6px; }
    #bar { display: flex; gap: 8px; margin-top: 8px; }
    input, button { padding: 8px; font-size: 14px; }
    #username { width: 140px; }
    #msg { flex: 1; }
    .line { margin: 4px 0; }
    .me { color: #2563eb }
    .other { color: #374151 }
  </style>
</head>
<body>
  <header>内网群聊</header>
  <main>
    <div id="log"></div>
    <div id="bar">
      <input id="username" placeholder="用户名" />
      <input id="msg" placeholder="输入消息，按 Enter 发送" />
      <button id="send">发送</button>
    </div>
  </main>
  <script>
    const log = document.getElementById('log');
    const username = document.getElementById('username');
    const msg = document.getElementById('msg');
    const sendBtn = document.getElementById('send');
    let idx = 0;
    username.value = (new URLSearchParams(location.search).get('u')) || '';
    async function pull(){
      try{
        const r = await fetch('/pull?idx=' + idx);
        const data = await r.json();
        for(const m of data){
          const div = document.createElement('div');
          div.className = 'line ' + (m.username === username.value ? 'me' : 'other');
          const t = new Date(m.ts * 1000).toLocaleTimeString();
          div.textContent = `[${t}] ${m.username}: ${m.text}`;
          log.appendChild(div);
          log.scrollTop = log.scrollHeight;
          idx++;
        }
      }catch(e){ /* ignore */ }
    }
    async function send(){
      const u = username.value.trim();
      const text = msg.value.trim();
      if(!u || !text) return;
      msg.value = '';
      await fetch('/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({username: u, text})
      });
    }
    setInterval(pull, 1000);
    msg.addEventListener('keydown', (e)=>{ if(e.key==='Enter'){ send(); } });
    sendBtn.addEventListener('click', send);
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    store: ChatStore = None  # type: ignore

    def _send(self, code: int, body: bytes, content_type: str = "text/html"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            self._send(200, INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if u.path == "/pull":
            qs = parse_qs(u.query)
            idx = int(qs.get("idx", [0])[0])
            data = self.store.since(idx)
            body = json.dumps(data).encode("utf-8")
            self._send(200, body, "application/json")
            return
        self._send(404, b"not found")

    def do_POST(self):
        u = urlparse(self.path)
        if u.path == "/send":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                payload = {}
            username = str(payload.get("username") or "")[:64]
            text = str(payload.get("text") or "")[:2000]
            if username and text:
                self.store.append(username, text)
            self._send(200, b"ok", "text/plain")
            return
        self._send(404, b"not found")


def run(host: str, port: int, log_dir: str):
    store = ChatStore(log_dir)
    Handler.store = store
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"Web chat listening on http://{host}:{port}/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def parse_args():
    p = argparse.ArgumentParser(prog="web_chat_server", add_help=True)
    p.add_argument("--host", type=str, default="127.0.0.1")
    p.add_argument("--port", type=int, default=8002)
    p.add_argument("--log-dir", type=str, default=os.path.join(os.getcwd(), "chat_logs"))
    return p.parse_args()


def main():
    args = parse_args()
    run(args.host, args.port, args.log_dir)


if __name__ == "__main__":
    main()
