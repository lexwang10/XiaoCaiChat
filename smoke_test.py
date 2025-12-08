import socket
import time

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 5001))
    s.sendall(b"HELLO Tester general\n")
    f = s.makefile("r", encoding="utf-8", newline="\n")
    s.sendall(b"SEQ 1 MSG hello from smoke test\n")
    deadline = time.time() + 2
    while time.time() < deadline:
        s.sendall(b"PING 123\n")
        time.sleep(0.2)
        try:
            line = f.readline()
            if not line:
                break
            print(line.strip())
        except Exception:
            break
    s.close()

if __name__ == "__main__":
    main()
