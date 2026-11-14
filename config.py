import os
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
ZK_SESSION_TIMEOUT = 10.0

# Твои реальные настройки RustFS из ДЗ №3
DEFAULT_S3_ENDPOINT = "http://127.0.0.1:9001"
DEFAULT_S3_ACCESS_KEY = "admin"
DEFAULT_S3_SECRET_KEY = "MySecretPassword123"
S3_BUCKET = "hh-lake"
S3_KEY = "it_vacancies_2024.csv"

CHUNK_ROWS = 5000
FREEZE_AT_PCT = 35.0
FREEZE_SECONDS = 40.0


def detect_from_lab3():
    found = {}
    search_files = [
        BASE_DIR.parent / "lab3" / "lab3.py",
        BASE_DIR.parent / "Lab3" / "lab3.py",
        BASE_DIR.parent / "lab3.py",
        Path.home() / "Desktop" / "Lab3" / "lab3.py",
    ]
    for candidate in search_files:
        if not candidate.exists():
            continue
        text = candidate.read_text(encoding="utf-8", errors="ignore")
        ep = re.search(r'(?:endpoint_url|S3_ENDPOINT)\s*=\s*["\'](https?://[^"\']+)["\']', text)
        ak = re.search(r'(?:aws_access_key_id|S3_ACCESS_KEY)\s*=\s*["\']([^"\']+)["\']', text)
        sk = re.search(r'(?:aws_secret_access_key|S3_SECRET_KEY)\s*=\s*["\']([^"\']+)["\']', text)
        if ep and "endpoint" not in found:
            found["endpoint"] = ep.group(1)
        if ak and "access_key" not in found:
            found["access_key"] = ak.group(1)
        if sk and "secret_key" not in found:
            found["secret_key"] = sk.group(1)
    return found


def get_config():
    auto = detect_from_lab3()
    return {
        "zk_hosts": os.environ.get("ZK_HOSTS", ZK_HOSTS),
        "zk_timeout": float(os.environ.get("ZK_SESSION_TIMEOUT", ZK_SESSION_TIMEOUT)),
        "s3_endpoint": os.environ.get("S3_ENDPOINT", auto.get("endpoint", DEFAULT_S3_ENDPOINT)),
        "s3_access_key": os.environ.get("S3_ACCESS_KEY", auto.get("access_key", DEFAULT_S3_ACCESS_KEY)),
        "s3_secret_key": os.environ.get("S3_SECRET_KEY", auto.get("secret_key", DEFAULT_S3_SECRET_KEY)),
        "s3_bucket": os.environ.get("S3_BUCKET", S3_BUCKET),
        "s3_key": os.environ.get("S3_KEY", S3_KEY),
        "chunk_rows": CHUNK_ROWS,
        "freeze_at_pct": FREEZE_AT_PCT,
        "freeze_seconds": FREEZE_SECONDS,
        "auto_detected_lab3": bool(auto),
    }
