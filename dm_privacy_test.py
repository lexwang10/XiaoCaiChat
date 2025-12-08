import socket
import time

def make_client(name):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.2)
    s.connect(("127.0.0.1", 5001))
    s.sendall(f"HELLO {name} general\n".encode("utf-8"))
    return s

def read_all(s, deadline):
    buf = b""
    while time.time() < deadline:
        try:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
        except Exception:
            time.sleep(0.05)
    return buf.decode("utf-8", errors="ignore").splitlines()

def main():
    a = make_client("C01")
    b = make_client("C03")
    c = make_client("C02")
    time.sleep(0.5)
    a.sendall(b"SEQ 1 DM C03 hello-c01-to-c03\n")
    deadline = time.time() + 3.0
    out_a = read_all(a, deadline)
    out_b = read_all(b, deadline)
    out_c = read_all(c, deadline)
    a.close(); b.close(); c.close()
    seen_by_b = any(x.startswith("[DM] FROM C01 ") for x in out_b)
    seen_by_c = any("hello-c01-to-c03" in x for x in out_c)
    print("A lines:", out_a)
    print("B lines:", out_b)
    print("C lines:", out_c)
    print("DM seen by target:", seen_by_b)
    print("DM leaked to third:", seen_by_c)
    if seen_by_b and not seen_by_c:
        print("OK: DM privacy holds")
    else:
        print("FAIL: DM privacy violation or missing delivery")

if __name__ == "__main__":
    main()
