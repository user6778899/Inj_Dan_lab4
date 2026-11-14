import argparse
import csv
import io
import itertools
import json
import logging
import os
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from kazoo.client import KazooClient
from kazoo.exceptions import (
    BadVersionError,
    ConnectionLoss,
    KazooException,
    NodeExistsError,
    SessionExpiredError,
)

from config import BASE_DIR, get_config

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

csv.field_size_limit(10 * 1024 * 1024)

ZK_ROOT = "/rustfs_lab4"
LEADER_PATH = f"{ZK_ROOT}/leader"
CKPT_PATH = f"{ZK_ROOT}/checkpoint"
POINTER_PATH = f"{ZK_ROOT}/pointer"


class CountingRawStream(io.RawIOBase):
    def __init__(self, body):
        self.body = body
        self.bytes_read = 0

    def readable(self):
        return True

    def readinto(self, b):
        data = self.body.read(len(b))
        if not data:
            return 0
        n = len(data)
        b[:n] = data
        self.bytes_read += n
        return n


def setup_logger(run_name: str, coord_id: str) -> logging.Logger:
    logs_dir = BASE_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{run_name}_{coord_id}.log"

    logger = logging.getLogger(f"{run_name}_{coord_id}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s.%(msecs)03d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def find_col(headers_lower: list[str], candidates: list[str]) -> int:
    for cand in candidates:
        if cand in headers_lower:
            return headers_lower.index(cand)
    for idx, col in enumerate(headers_lower):
        for cand in candidates:
            if cand in col:
                return idx
    raise ValueError(f"Не найдена колонка из {candidates}. Доступные: {headers_lower}")


def parse_skills_like_lab3(raw: str) -> list[str]:
    """Логика 1-в-1 как в lab3.py: split(',') -> strip() -> sorted()."""
    if not raw or not raw.strip():
        return []
    if raw.strip().lower() in ("nan", "none", "null"):
        return []
    skills = [s.strip() for s in raw.split(";")]
    return sorted(skills) if skills else []


def make_s3_client(cfg: dict):
    return boto3.client(
        "s3",
        endpoint_url=cfg["s3_endpoint"],
        aws_access_key_id=cfg["s3_access_key"],
        aws_secret_access_key=cfg["s3_secret_key"],
        config=BotoConfig(connect_timeout=10, read_timeout=120, retries={"max_attempts": 2}),
    )


def open_csv_stream(s3, cfg: dict, skip_rows: int = 0):
    resp = s3.get_object(Bucket=cfg["s3_bucket"], Key=cfg["s3_key"])
    body = resp["Body"]
    raw_stream = CountingRawStream(body)
    text_stream = io.TextIOWrapper(
        io.BufferedReader(raw_stream, buffer_size=1024 * 1024),
        encoding="utf-8-sig",
        errors="replace",
        newline="",
    )
    first_line = text_stream.readline()
    delimiter = "," if "," in first_line else ";"
    headers = next(csv.reader([first_line], delimiter=delimiter))
    headers_lower = [h.strip().lower() for h in headers]
    title_idx = find_col(headers_lower, ["name", "title", "vacancy_name"])
    skills_idx = find_col(headers_lower, ["key_skills", "skills"])
    reader = csv.reader(text_stream, delimiter=delimiter)
    if skip_rows > 0:
        for _ in itertools.islice(reader, skip_rows):
            pass
    return body, raw_stream, reader, title_idx, skills_idx


def acquire_leadership(zk: KazooClient, coord_id: str, run_name: str, logger: logging.Logger):
    waiting_logged = False
    while True:
        try:
            zk.create(LEADER_PATH, value=coord_id.encode("utf-8"), ephemeral=True)
            ts = time.time()
            ts_file = BASE_DIR / "logs" / f"{run_name}_{coord_id}.leader_ts"
            ts_file.write_text(f"{ts:.6f}", encoding="utf-8")
            logger.info("[%s] СТАЛ ЛИДЕРОМ! (timestamp=%.3f)", coord_id, ts)
            return
        except NodeExistsError:
            if not waiting_logged:
                logger.info("[%s] Лидер уже активен, перехожу в режим ОЖИДАНИЯ (standby)...", coord_id)
                waiting_logged = True
            ev = threading.Event()
            stat = zk.exists(LEADER_PATH, watch=lambda event: ev.set())
            if stat is not None:
                ev.wait(timeout=1.0)


def save_checkpoint(zk: KazooClient, run_name: str, coord_id: str, chunk_idx: int,
                    matched: int, skill_counts: Counter, pair_counts: Counter, logger: logging.Logger):
    ckpt_dir = BASE_DIR / "checkpoints"
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    ckpt_file = ckpt_dir / f"{run_name}_ckpt.json"
    tmp_file = ckpt_dir / f"{run_name}_{coord_id}_ckpt.tmp"

    payload = {
        "chunk_idx": chunk_idx,
        "matched_vacancies": matched,
        "skill_counts": dict(skill_counts),
        "pair_counts": {" || ".join(k): v for k, v in pair_counts.items()},
    }
    tmp_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_file, ckpt_file)

    zk_meta = json.dumps({"last_chunk": chunk_idx, "ckpt_file": str(ckpt_file)}, ensure_ascii=False).encode("utf-8")
    if zk.connected:
        try:
            if zk.exists(CKPT_PATH):
                zk.set(CKPT_PATH, zk_meta)
            else:
                zk.create(CKPT_PATH, zk_meta)
        except KazooException as exc:
            logger.warning("[%s] Чекпоинт %s не записан в ZK: %s", coord_id, chunk_idx, exc)


def load_checkpoint(zk: KazooClient, coord_id: str, logger: logging.Logger):
    if not zk.exists(CKPT_PATH):
        logger.info("[%s] Чекпоинт отсутствует, начинаю расчёт с чанка 1...", coord_id)
        return 0, 0, Counter(), Counter()

    raw, _ = zk.get(CKPT_PATH)
    if not raw:
        return 0, 0, Counter(), Counter()

    meta = json.loads(raw.decode("utf-8"))
    ckpt_file = Path(meta["ckpt_file"])
    data = json.loads(ckpt_file.read_text(encoding="utf-8"))

    last_chunk = int(data["chunk_idx"])
    matched = int(data["matched_vacancies"])
    skill_counts = Counter(data["skill_counts"])
    pair_counts = Counter({tuple(k.split(" || ")): v for k, v in data["pair_counts"].items()})
    logger.info(
        "[%s] Загружен ЧЕКПОИНТ: чанк=%d, найдено Python-вакансий=%d. Продолжаю с чанка %d...",
        coord_id, last_chunk, matched, last_chunk + 1
    )
    return last_chunk, matched, skill_counts, pair_counts


def publish_result(zk: KazooClient, cfg: dict, s3, run_name: str, coord_id: str,
                   fencing: int, expected_version: int, summary: dict, logger: logging.Logger):
    payload_bytes = json.dumps(summary, ensure_ascii=False).encode("utf-8")

    def _do_commit(client: KazooClient):
        if fencing == 1:
            logger.info("[%s] Публикация результата С ЗАЩИТОЙ по эпохе (expected_version=%d)...",
                        coord_id, expected_version)
            return client.set(POINTER_PATH, payload_bytes, version=expected_version)
        logger.info("[%s] Публикация результата БЕЗ защиты по эпохе (перезапись)...", coord_id)
        return client.set(POINTER_PATH, payload_bytes)

    pub_zk = None
    try:
        use_zk = zk
        if not use_zk.connected:
            logger.info("[%s] Старая сессия ZK истекла за время паузы — переподключаюсь для публикации...", coord_id)
            pub_zk = KazooClient(hosts=cfg["zk_hosts"], timeout=cfg["zk_timeout"])
            pub_zk.start(timeout=15)
            use_zk = pub_zk

        try:
            stat = _do_commit(use_zk)
        except (SessionExpiredError, ConnectionLoss):
            logger.info("[%s] Сессия истекла при записи — открываю новую сессию публикации...", coord_id)
            if pub_zk is None:
                pub_zk = KazooClient(hosts=cfg["zk_hosts"], timeout=cfg["zk_timeout"])
                pub_zk.start(timeout=15)
            stat = _do_commit(pub_zk)

        s3_key = f"lab4_results/{run_name}_final.json"
        s3.put_object(Bucket=cfg["s3_bucket"], Key=s3_key, Body=payload_bytes)

        logger.info(
            "[%s] записал результат (указатель %s обновлен, version=%d, S3=s3://%s/%s)",
            coord_id, POINTER_PATH, stat.version, cfg["s3_bucket"], s3_key
        )
    except BadVersionError:
        logger.error(
            "[%s] ОШИБКА: BadVersionError — запись отклонена защитой по эпохе! "
            "(ожидалась version=%d, но указатель уже обновил запасной координатор)",
            coord_id, expected_version
        )
    finally:
        if pub_zk is not None:
            try:
                pub_zk.stop()
                pub_zk.close()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--run-name", required=True)
    args = parser.parse_args()

    coord_id = args.id
    run_name = args.run_name
    fencing = int(os.environ.get("FENCING", "0"))
    cfg = get_config()
    logger = setup_logger(run_name, coord_id)

    logger.info(
        "[%s] Запуск координатора (PID=%d, FENCING=%d, session_timeout=%.1fs)",
        coord_id, os.getpid(), fencing, cfg["zk_timeout"]
    )

    zk = KazooClient(hosts=cfg["zk_hosts"], timeout=cfg["zk_timeout"])
    zk.start(timeout=15)

    try:
        acquire_leadership(zk, coord_id, run_name, logger)

        _, ptr_stat = zk.get(POINTER_PATH)
        expected_version = ptr_stat.version
        logger.info("[%s] Прочитана текущая версия указателя эпохи: version=%d", coord_id, expected_version)

        last_chunk, matched_vacancies, skill_counts, pair_counts = load_checkpoint(zk, coord_id, logger)

        s3 = make_s3_client(cfg)
        head = s3.head_object(Bucket=cfg["s3_bucket"], Key=cfg["s3_key"])
        total_bytes = int(head["ContentLength"])

        chunk_rows = cfg["chunk_rows"]
        current_chunk = last_chunk
        last_ckpt_pct = 0.0
        eof = False
        body = None

        while not eof:
            try:
                body, raw_stream, reader, title_idx, skills_idx = open_csv_stream(
                    s3, cfg, skip_rows=current_chunk * chunk_rows
                )
                if current_chunk > 0:
                    last_ckpt_pct = min(99.0, (raw_stream.bytes_read / total_bytes) * 100.0)
                    logger.info("[%s] Пропущено %d чанков (%.1f%%), продолжаю обработку...",
                                coord_id, current_chunk, last_ckpt_pct)

                while True:
                    t0 = time.time()
                    rows_in_chunk = 0
                    c_matched = 0
                    c_skills = Counter()
                    c_pairs = Counter()

                    for row in itertools.islice(reader, chunk_rows):
                        rows_in_chunk += 1
                        if len(row) <= max(title_idx, skills_idx):
                            continue
                        if "python" not in row[title_idx].lower():
                            continue
                        c_matched += 1
                        skills = parse_skills_like_lab3(row[skills_idx])
                        if not skills:
                            continue
                        c_skills.update(skills)
                        if len(skills) >= 2:
                            c_pairs.update(itertools.combinations(skills, 2))

                    if rows_in_chunk == 0:
                        eof = True
                        break

                    current_chunk += 1
                    matched_vacancies += c_matched
                    skill_counts.update(c_skills)
                    pair_counts.update(c_pairs)

                    dt = time.time() - t0
                    if dt < 0.05:
                        time.sleep(0.05 - dt)

                    pct = min(100.0, (raw_stream.bytes_read / total_bytes) * 100.0)
                    if pct - last_ckpt_pct >= 5.0 or rows_in_chunk < chunk_rows:
                        save_checkpoint(
                            zk, run_name, coord_id, current_chunk,
                            matched_vacancies, skill_counts, pair_counts, logger
                        )
                        last_ckpt_pct = pct
                        logger.info(
                            "[%s] Чанк %d | Прогресс: %.1f%% | Вакансий Python: %d | Чекпоинт сохранен",
                            coord_id, current_chunk, pct, matched_vacancies
                        )

                    if rows_in_chunk < chunk_rows:
                        eof = True
                        break

            except Exception as stream_err:
                if eof:
                    break
                logger.warning(
                    "[%s] Поток S3 прервался (%s). Переоткрываю с чанка %d...",
                    coord_id, stream_err, current_chunk + 1
                )
                time.sleep(0.5)
            finally:
                if body is not None:
                    try:
                        body.close()
                    except Exception:
                        pass
                    body = None

        top_skills = skill_counts.most_common(5)
        top_pairs = [[" + ".join(p), cnt] for p, cnt in pair_counts.most_common(5)]

        summary = {
            "leader": coord_id,
            "fencing": fencing,
            "matched_python_vacancies": matched_vacancies,
            "top_1_skill": top_skills[0] if top_skills else None,
            "top_1_pair": top_pairs[0] if top_pairs else None,
            "top_5_skills": top_skills,
            "top_5_pairs": top_pairs,
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        results_dir = BASE_DIR / "results"
        results_dir.mkdir(parents=True, exist_ok=True)
        (results_dir / f"{run_name}_{coord_id}.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        logger.info("[%s] === РАСЧЁТ ЗАВЕРШЁН ===", coord_id)
        logger.info("[%s] Вакансий Python: %d | Топ-1 навык: %s | Топ-1 пара: %s",
                    coord_id, matched_vacancies, summary["top_1_skill"], summary["top_1_pair"])

        publish_result(zk, cfg, s3, run_name, coord_id, fencing, expected_version, summary, logger)

    finally:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
