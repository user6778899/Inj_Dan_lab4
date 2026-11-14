import json
import os
import queue
import re
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import boto3
import psutil
from botocore.config import Config as BotoConfig
from kazoo.client import KazooClient

from config import BASE_DIR, get_config

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def is_port_open(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex((host, port)) == 0


def ensure_zookeeper(cfg: dict):
    print("=== 1. ПРОВЕРКА И ЗАПУСК ZOOKEEPER ===")
    ports = [(1, 2181), (2, 2182), (3, 2183)]
    missing = [(idx, p) for idx, p in ports if not is_port_open(p)]

    if missing:
        zk_base = Path(r"C:\Zookeeper")
        if all((zk_base / f"zk{i}" / "bin" / "zkServer.cmd").exists() for i, _ in missing):
            print("[INFO] Не все серверы ZooKeeper запущены — поднимаю автоматически из C:\\Zookeeper...")
            for idx, _ in missing:
                cmd = f'cd /d C:\\Zookeeper\\zk{idx}\\bin && zkServer.cmd'
                subprocess.Popen(["cmd", "/c", "start", f"ZK{idx}", "cmd", "/k", cmd])
            for _ in range(25):
                if all(is_port_open(p) for _, p in ports):
                    break
                time.sleep(1.0)

    zk_lines = ["=== Проверка ансамбля Apache ZooKeeper ==="]
    for idx, port in ports:
        ok = is_port_open(port)
        line = f"Узел zk{idx} (127.0.0.1:{port}) -> {'ДОСТУПЕН' if ok else 'НЕ ОТВЕЧАЕТ'}"
        print(f"[{'OK' if ok else 'ERR'}] {line}")
        zk_lines.append(line)
        if not ok:
            print("\nОШИБКА: Серверы ZooKeeper не поднялись! Запусти zk1..zk3 вручную через zkServer.cmd.")
            sys.exit(1)

    zk = KazooClient(hosts=cfg["zk_hosts"], timeout=cfg["zk_timeout"])
    zk.start(timeout=10)
    try:
        zk_lines.append(f"Корневые znodes (/): {zk.get_children('/')}")
        if zk.exists("/zookeeper/config"):
            raw_cfg, _ = zk.get("/zookeeper/config")
            zk_lines.append("Конфигурация кворума (/zookeeper/config):")
            zk_lines.append(raw_cfg.decode("utf-8", errors="replace").strip())
    finally:
        zk.stop()
        zk.close()

    logs_dir = BASE_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (logs_dir / "zk_ensemble_check.txt").write_text("\n".join(zk_lines), encoding="utf-8")


def probe_s3(endpoint: str, cfg: dict):
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=cfg["s3_access_key"],
            aws_secret_access_key=cfg["s3_secret_key"],
            config=BotoConfig(connect_timeout=2, read_timeout=5, retries={"max_attempts": 1}),
        )
        head = s3.head_object(Bucket=cfg["s3_bucket"], Key=cfg["s3_key"])
        return True, head["ContentLength"] / (1024 * 1024), ""
    except Exception as exc:
        return False, 0.0, str(exc)


def find_first_working_s3(cfg: dict):
    candidates = []
    for ep in (cfg["s3_endpoint"], "http://127.0.0.1:9001", "http://127.0.0.1:9000"):
        if ep not in candidates:
            candidates.append(ep)
    last_err = ""
    for ep in candidates:
        ok, size_mb, err = probe_s3(ep, cfg)
        if ok:
            return ep, size_mb, ""
        last_err = err
    return None, 0.0, last_err


def try_autostart_rustfs(cfg: dict):
    # Способ А: существующий Docker-контейнер
    try:
        res = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}|{{.Image}}|{{.Status}}"],
            capture_output=True, text=True, timeout=6
        )
        if res.returncode == 0:
            for line in res.stdout.splitlines():
                if "rustfs" in line.lower():
                    cname = line.split("|")[0].strip()
                    print(f"[INFO] Найден Docker-контейнер '{cname}', запускаю (docker start)...")
                    subprocess.run(["docker", "start", cname], timeout=15)
                    return True
        elif "error during connect" in (res.stderr or "").lower() or "daemon" in (res.stderr or "").lower():
            print("[WARN] Docker установлен, но Docker Desktop сейчас выключен.")
            dd = Path(r"C:\Program Files\Docker\Docker\Docker Desktop.exe")
            if dd.exists():
                print("[INFO] Подсказка: если RustFS был в Docker — просто открой Docker Desktop.")
    except FileNotFoundError:
        pass
    except Exception:
        pass

    # Способ Б: поиск на диске docker-compose.yml или rustfs.exe + disk1..disk4
    skip_dirs = {"Windows", "Program Files", "Program Files (x86)", "ProgramData", "AppData",
                 "$Recycle.Bin", "System Volume Information", ".git", "node_modules", "__pycache__", "venv"}
    roots = [BASE_DIR.parent, Path.home() / "Desktop", Path.home(), Path("C:/rustfs"), Path("C:/")]
    visited = set()

    def iter_dirs(root: Path, depth: int = 0):
        if depth > 3 or not root.exists():
            return
        try:
            r_res = str(root.resolve())
        except Exception:
            return
        if r_res in visited:
            return
        visited.add(r_res)
        yield root
        try:
            for entry in root.iterdir():
                if entry.is_dir() and entry.name not in skip_dirs and not entry.name.startswith("."):
                    yield from iter_dirs(entry, depth + 1)
        except PermissionError:
            pass

    for root in roots:
        for folder in iter_dirs(root):
            # 1) docker-compose с rustfs
            for cname in ("docker-compose.yml", "docker-compose.yaml", "compose.yaml", "compose.yml"):
                cfile = folder / cname
                if cfile.exists():
                    txt = cfile.read_text(encoding="utf-8", errors="ignore")
                    if "rustfs" in txt.lower():
                        print(f"[INFO] Найден {cfile}, запускаю docker compose up -d...")
                        r = subprocess.run(["docker", "compose", "up", "-d"], cwd=str(folder), timeout=30)
                        if r.returncode == 0:
                            return True

            # 2) rustfs.exe + папки disk1..disk4
            exe = folder / "rustfs.exe"
            if exe.exists():
                print(f"[INFO] Найден {exe}, запускаю локальный сервер RustFS...")
                bats = [b for b in folder.glob("*.bat") if "lab4" not in b.name.lower()]
                if bats:
                    print(f"[INFO] Запускаю найденный батник: {bats[0].name}")
                    subprocess.Popen(["cmd", "/c", "start", "RustFS", "cmd", "/k", str(bats[0])], cwd=str(folder))
                    return True

                disks = [str(folder / f"disk{i}") for i in range(1, 5) if (folder / f"disk{i}").is_dir()]
                if not disks:
                    disks = [str(folder / "data")] if (folder / "data").is_dir() else [str(folder)]

                help_out = ""
                try:
                    h = subprocess.run([str(exe), "--help"], capture_output=True, text=True, timeout=5)
                    help_out = (h.stdout or "") + (h.stderr or "")
                except Exception:
                    pass

                env = os.environ.copy()
                env["RUSTFS_ACCESS_KEY"] = cfg["s3_access_key"]
                env["RUSTFS_SECRET_KEY"] = cfg["s3_secret_key"]

                subcmd = ["server"] if "server" in help_out.lower() else []
                port = "9001" if ":9001" in cfg["s3_endpoint"] else "9000"
                console_port = "9002" if port == "9001" else "9001"
                cmd = [
                    str(exe), *subcmd,
                    "--address", f"127.0.0.1:{port}",
                    "--console-enable", "true",
                    "--console-address", f"127.0.0.1:{console_port}",
                    *disks,
                ]
                subprocess.Popen(cmd, cwd=str(folder), env=env, creationflags=subprocess.CREATE_NEW_CONSOLE)
                return True
    return False


def ensure_rustfs(cfg: dict):
    print("\n=== 2. ПРОВЕРКА И ЗАПУСК RUSTFS ===")
    ep, size_mb, err = find_first_working_s3(cfg)
    if ep:
        cfg["s3_endpoint"] = ep
        print(f"[OK] RustFS уже работает ({ep})! Найден s3://{cfg['s3_bucket']}/{cfg['s3_key']} ({size_mb:.1f} MiB)")
        return

    print("[INFO] RustFS не отвечает — пробую найти и запустить автоматически...")
    started = try_autostart_rustfs(cfg)
    if started:
        for _ in range(15):
            ep, size_mb, err = find_first_working_s3(cfg)
            if ep:
                cfg["s3_endpoint"] = ep
                print(f"[OK] RustFS успешно запущен ({ep})! Файл {size_mb:.1f} MiB доступен.")
                return
            time.sleep(1.0)

    print("\n[ERR] Не удалось автоматически подключиться к RustFS.")
    print(f"Последняя ошибка: {err}")
    print("Что сделать вручную (30 секунд):")
    print("  Вариант 1 (если в ЛР №2 был Docker): открой Docker Desktop -> вкладка Containers -> нажми Play у rustfs.")
    print("  Вариант 2 (если был rustfs.exe): открой папку с disk1..disk4 и запусти свой скрипт/rustfs.exe.")
    print("После этого просто снова нажми run_lab4.bat!")
    sys.exit(1)


def reset_zk_state(cfg: dict):
    zk = KazooClient(hosts=cfg["zk_hosts"], timeout=cfg["zk_timeout"])
    zk.start(timeout=10)
    try:
        if zk.exists("/rustfs_lab4"):
            zk.delete("/rustfs_lab4", recursive=True)
        zk.ensure_path("/rustfs_lab4")
        zk.create("/rustfs_lab4/pointer", b"init")
    finally:
        zk.stop()
        zk.close()


def read_pointer_state(cfg: dict):
    zk = KazooClient(hosts=cfg["zk_hosts"], timeout=cfg["zk_timeout"])
    zk.start(timeout=10)
    try:
        data, stat = zk.get("/rustfs_lab4/pointer")
        return data.decode("utf-8", errors="replace"), stat.version
    finally:
        zk.stop()
        zk.close()


def pump_stream(proc, label: str, out_q: queue.Queue):
    for line in proc.stdout:
        out_q.put((label, line.rstrip("\r\n")))
    out_q.put((label, None))


def run_stage(cfg: dict, run_name: str, fencing: int) -> dict:
    title = f"ЭТАП {'4' if fencing else '3'}: FENCING={fencing} ({'С ЗАЩИТОЙ' if fencing else 'БЕЗ ЗАЩИТЫ'})"
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)

    reset_zk_state(cfg)
    for f in (BASE_DIR / "logs").glob(f"{run_name}_*.leader_ts"):
        f.unlink(missing_ok=True)

    env = os.environ.copy()
    env["FENCING"] = str(fencing)
    env["S3_ENDPOINT"] = cfg["s3_endpoint"]
    env["S3_ACCESS_KEY"] = cfg["s3_access_key"]
    env["S3_SECRET_KEY"] = cfg["s3_secret_key"]
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

    coord_script = str(BASE_DIR / "coordinator.py")
    p1 = subprocess.Popen(
        [sys.executable, coord_script, "--id", "coord1", "--run-name", run_name],
        cwd=str(BASE_DIR), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    )
    p2 = None
    out_q = queue.Queue()
    threading.Thread(target=pump_stream, args=(p1, "coord1", out_q), daemon=True).start()

    suspended = False
    suspend_ts = None
    failover_sec = None
    p2_started = False
    p2_finished = False
    resumed = False
    active_streams = 1

    pct_re = re.compile(r"Прогресс:\s*([\d.]+)%")
    all_lines = []

    try:
        while active_streams > 0:
            try:
                label, line = out_q.get(timeout=0.5)
            except queue.Empty:
                line = ""
                label = None

            if label is not None and line is None:
                active_streams -= 1
            elif label is not None and line:
                print(line, flush=True)
                all_lines.append(line)

                if label == "coord1" and "СТАЛ ЛИДЕРОМ" in line and not p2_started:
                    time.sleep(0.5)
                    p2 = subprocess.Popen(
                        [sys.executable, coord_script, "--id", "coord2", "--run-name", run_name],
                        cwd=str(BASE_DIR), env=env,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        text=True, encoding="utf-8", errors="replace", bufsize=1,
                    )
                    active_streams += 1
                    p2_started = True
                    threading.Thread(target=pump_stream, args=(p2, "coord2", out_q), daemon=True).start()

                if label == "coord1" and not suspended and "Чекпоинт сохранен" in line:
                    m = pct_re.search(line)
                    if m and float(m.group(1)) >= cfg["freeze_at_pct"]:
                        time.sleep(0.15)
                        suspend_ts = time.time()
                        print(f"\n[RUNNER] >>> ПРИОСТАНАВЛИВАЮ ЛИДЕРА coord1 (PID={p1.pid}) на {int(cfg['freeze_seconds'])} сек... <<<\n", flush=True)
                        psutil.Process(p1.pid).suspend()
                        suspended = True

                if label == "coord2" and "СТАЛ ЛИДЕРОМ" in line and suspend_ts is not None:
                    ts_file = BASE_DIR / "logs" / f"{run_name}_coord2.leader_ts"
                    leader_ts = float(ts_file.read_text(encoding="utf-8")) if ts_file.exists() else time.time()
                    failover_sec = leader_ts - suspend_ts
                    print(f"\n[RUNNER] >>> СМЕНА ЛИДЕРА: coord2 стал лидером через {failover_sec:.2f} сек! <<<\n", flush=True)

                if label == "coord2" and "записал результат" in line:
                    p2_finished = True

            if suspended and not resumed:
                elapsed = time.time() - suspend_ts
                p2_done = p2_finished or (p2 is not None and p2.poll() is not None)
                if elapsed >= cfg["freeze_seconds"] and p2_done:
                    print(f"\n[RUNNER] >>> Прошло {elapsed:.1f} сек. ВОЗОБНОВЛЯЮ процесс coord1 (PID={p1.pid})... <<<\n", flush=True)
                    psutil.Process(p1.pid).resume()
                    resumed = True

        p1.wait(timeout=30)
        if p2 is not None:
            p2.wait(timeout=30)
    finally:
        for proc in (p1, p2):
            if proc is not None and proc.poll() is None:
                try:
                    psutil.Process(proc.pid).resume()
                    proc.terminate()
                except Exception:
                    pass

    ptr_data, ptr_version = read_pointer_state(cfg)
    writes_count = sum(1 for l in all_lines if "записал результат" in l)
    bad_ver_count = sum(1 for l in all_lines if "BadVersionError" in l)

    ptr_json = json.loads(ptr_data)
    result = {
        "run_name": run_name,
        "fencing": fencing,
        "failover_sec": round(failover_sec, 2) if failover_sec is not None else None,
        "writes_count": writes_count,
        "bad_version_count": bad_ver_count,
        "pointer_version": ptr_version,
        "pointer_data": ptr_json,
    }

    print("\n" + "-" * 74)
    print(f"ИТОГ {title}:")
    print(f"  Время смены лидера:              {result['failover_sec']} сек.")
    print(f"  Строк 'записал результат':       {writes_count}")
    print(f"  Ошибок BadVersionError:          {bad_ver_count}")
    print(f"  Указатель /rustfs_lab4/pointer:  version={ptr_version}, leader={ptr_json.get('leader')}")
    print(f"  Контрольные числа (Python):      вакансий={ptr_json.get('matched_python_vacancies')}, "
          f"топ-навык={ptr_json.get('top_1_skill')}, топ-пара={ptr_json.get('top_1_pair')}")
    print("-" * 74)
    return result


def main():
    cfg = get_config()
    ensure_zookeeper(cfg)
    ensure_rustfs(cfg)

    r3 = run_stage(cfg, "stage3_no_fencing", fencing=0)
    r4 = run_stage(cfg, "stage4_fencing", fencing=1)

    summary_text = (
        "=== ИТОГОВАЯ СВОДКА ДЛЯ ОТЧЕТА И ЗАЩИТЫ (ЛР №4) ===\n\n"
        f"1) Этап 3 (FENCING=0, без защиты):\n"
        f"   - Время смены лидера: {r3['failover_sec']} сек.\n"
        f"   - Строк 'записал результат': {r3['writes_count']} (ожидается 2)\n"
        f"   - Версия указателя /rustfs_lab4/pointer: version={r3['pointer_version']} (ожидается 2)\n"
        f"   - Последний записавший лидер: {r3['pointer_data'].get('leader')}\n"
        f"   - Топ-5 навыков: {r3['pointer_data'].get('top_5_skills')}\n"
        f"   - Топ-5 пар:     {r3['pointer_data'].get('top_5_pairs')}\n\n"
        f"2) Этап 4 (FENCING=1, с защитой по эпохе):\n"
        f"   - Время смены лидера: {r4['failover_sec']} сек.\n"
        f"   - Строк 'записал результат': {r4['writes_count']} (ожидается 1)\n"
        f"   - Ошибок BadVersionError: {r4['bad_version_count']} (ожидается 1)\n"
        f"   - Версия указателя /rustfs_lab4/pointer: version={r4['pointer_version']} (ожидается 1)\n"
        f"   - Записавший лидер: {r4['pointer_data'].get('leader')} (ожидается coord2)\n"
        f"   - Топ-5 навыков: {r4['pointer_data'].get('top_5_skills')}\n"
        f"   - Топ-5 пар:     {r4['pointer_data'].get('top_5_pairs')}\n\n"
        "Все журналы запусков сохранены в папке logs/.\n"
    )
    summary_path = BASE_DIR / "logs" / "SUMMARY_FOR_REPORT.txt"
    summary_path.write_text(summary_text, encoding="utf-8")

    print("\n" + summary_text)
    print(f"Сводка сохранена в: {summary_path}")


if __name__ == "__main__":
    main()
