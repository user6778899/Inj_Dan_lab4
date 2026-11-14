import socket

for i, port in [(1, 2181), (2, 2182), (3, 2183)]:
    print(f"--- zk{i} (127.0.0.1:{port}) ---")
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2.0) as s:
            s.sendall(b"srvr")
            chunks = []
            while True:
                data = s.recv(4096)
                if not data:
                    break
                chunks.append(data)
            resp = b"".join(chunks).decode("utf-8", errors="replace").strip()
            print(resp if resp else "Нет ответа")
    except Exception as e:
        print(f"Error contacting service: {e}")
    print()
