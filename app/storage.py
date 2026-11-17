"""SQLite 历史库：只增不改的变更日志 + 捕获状态。

关键约束：
- 所有写入在一把进程内锁内串行化，SQLite 开启 WAL；
- 一个源事务的全部行变更与该事务的提交点在同一个 SQLite 事务里落盘，
  保证多行事务整体可见、不会出现半条事务；
- 复制槽的推进位置（last_commit_lsn）与对应数据在同一事务更新，
  崩溃重启后靠 xid 去重，绝不重复、也不丢数据。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS capture_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS txns (
    xid            INTEGER PRIMARY KEY,
    commit_lsn     TEXT NOT NULL,
    commit_lsn_num INTEGER NOT NULL,
    commit_ts      TEXT
);

CREATE TABLE IF NOT EXISTS artifact_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_no  INTEGER NOT NULL,
    change_type  TEXT NOT NULL,              -- BASELINE / INSERT / UPDATE / DELETE
    service_name TEXT,
    version      TEXT,
    sha256       TEXT,
    xid          INTEGER,
    commit_lsn   TEXT,
    commit_lsn_num INTEGER,
    captured_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hist_artifact ON artifact_history(artifact_no, id);
CREATE INDEX IF NOT EXISTS idx_hist_lsn ON artifact_history(commit_lsn_num);

-- 一次性核验（只增不改；任何一次核验的结论一经形成即不可变）
CREATE TABLE IF NOT EXISTS verifications (
    id               TEXT PRIMARY KEY,
    status           TEXT NOT NULL,          -- PENDING/CONSISTENT/DIVERGED/UNCONFIRMED
    boundary_lsn     TEXT NOT NULL,
    boundary_lsn_num INTEGER NOT NULL,
    expected_json    TEXT NOT NULL,          -- 边界时刻源库整目录快照（JSON）
    source_rows      INTEGER NOT NULL,
    missing_json     TEXT,                   -- 结论形成后才写入，随后不可变
    extra_json       TEXT,
    mismatches_json  TEXT,
    captured_rows    INTEGER,
    unconfirmed_reason TEXT,
    error            TEXT,
    created_at       TEXT NOT NULL,
    finished_at      TEXT
);
"""

PHASE_INITIALIZING = "INITIALIZING"
PHASE_STREAMING = "STREAMING"
PHASE_ERROR = "ERROR"

# 核验状态机
VERIFY_PENDING = "PENDING"            # 边界已固定，等待复制流完整落入
VERIFY_CONSISTENT = "CONSISTENT"      # 同一边界逐编号比较全部相同
VERIFY_DIVERGED = "DIVERGED"          # 存在缺失/额外/字段不符
VERIFY_UNCONFIRMED = "UNCONFIRMED"    # 源断连/应用重启导致中断，可重新发起
_VERIFY_TERMINAL = frozenset(
    (VERIFY_CONSISTENT, VERIFY_DIVERGED, VERIFY_UNCONFIRMED)
)


def lsn_to_num(lsn: str) -> int:
    """'0/16B3748' -> 整数，便于比较先后。"""
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class HistoryStore:
    def __init__(self, path: str) -> None:
        import os

        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ state
    def _get_state(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM capture_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def get_state(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM capture_state"
            ).fetchall()
            return {r["key"]: r["value"] for r in rows}

    def get_phase(self) -> str:
        return self._get_state("phase") or PHASE_INITIALIZING

    def is_ready(self) -> bool:
        return self.get_phase() == PHASE_STREAMING

    def _set_many(self, items: dict[str, Any]) -> None:
        self._conn.executemany(
            """
            INSERT INTO capture_state(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            [(k, str(v)) for k, v in items.items()],
        )

    def mark_initializing(self, slot_name: str) -> None:
        with self._lock:
            self._set_many(
                {
                    "phase": PHASE_INITIALIZING,
                    "slot_name": slot_name,
                    "initialized_at": utc_now_iso(),
                    "last_error": "",
                }
            )

    def mark_error(self, message: str) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_ERROR, "last_error": message})

    def clear_error(self) -> None:
        with self._lock:
            self._set_many({"last_error": ""})

    def update_last_message_lsn(self, lsn: str) -> None:
        with self._lock:
            self._set_many({"last_message_lsn": lsn})

    def update_stream_watermark(self, lsn_num: int) -> None:
        """记录复制流实际观察到的最高 WAL 位置（含 keepalive）。

        与已落盘提交点不同：keepalive 也会推进该值，因此源库空闲时
        核验无需等待新事务即可判定"边界已完整落入"。单调递增。
        """
        with self._lock:
            prev = self._get_state("stream_watermark_num")
            if prev is None or lsn_num > int(prev):
                self._set_many({"stream_watermark_num": lsn_num})

    def get_stream_watermark(self) -> int | None:
        with self._lock:
            value = self._get_state("stream_watermark_num")
            return int(value) if value is not None else None

    # ----------------------------------------------------------------- writes
    def save_baseline(
        self, rows: Iterable[sqlite3.Row | tuple[int, str, str, str]], lsn: str
    ) -> None:
        """整批基线与基线提交点在一个 SQLite 事务里落盘。"""
        now = utc_now_iso()
        lsn_num = lsn_to_num(lsn)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._set_many(
                    {
                        "phase": PHASE_INITIALIZING,
                        "baseline_lsn": lsn,
                        "baseline_lsn_num": lsn_num,
                        "baseline_captured_at": now,
                    }
                )
                payload = [
                    (
                        r[0],
                        "BASELINE",
                        r[1],
                        r[2],
                        r[3],
                        None,
                        lsn,
                        lsn_num,
                        now,
                    )
                    for r in rows
                ]
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_streaming(self) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_STREAMING, "streaming_at": utc_now_iso()})

    def persist_transaction(
        self,
        xid: int,
        commit_lsn: str,
        commit_ts: str | None,
        changes: list[dict[str, Any]],
    ) -> bool:
        """落盘一个已提交源事务。返回 True=新写入，False=重复事务（已跳过）。"""
        lsn_num = lsn_to_num(commit_lsn)
        now = utc_now_iso()
        with self._lock:
            existed = self._conn.execute(
                "SELECT 1 FROM txns WHERE xid=?", (xid,)
            ).fetchone()
            if existed:
                return False
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO txns(xid, commit_lsn, commit_lsn_num, commit_ts)"
                    " VALUES (?, ?, ?, ?)",
                    (xid, commit_lsn, lsn_num, commit_ts),
                )
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            c["artifact_no"],
                            c["change_type"],
                            c.get("service_name"),
                            c.get("version"),
                            c.get("sha256"),
                            xid,
                            commit_lsn,
                            lsn_num,
                            now,
                        )
                        for c in changes
                    ],
                )
                # 槽位推进位置与数据同事务落盘
                self._set_many(
                    {
                        "last_commit_lsn": commit_lsn,
                        "last_commit_lsn_num": lsn_num,
                        "last_xid": xid,
                    }
                )
                self._conn.execute("COMMIT")
                return True
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ reads
    def get_points(self) -> list[dict[str, Any]]:
        with self._lock:
            points: list[dict[str, Any]] = []
            baseline_lsn = self._get_state("baseline_lsn")
            if baseline_lsn:
                count = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM artifact_history"
                    " WHERE change_type='BASELINE'"
                ).fetchone()["c"]
                points.append(
                    {
                        "kind": "BASELINE",
                        "lsn": baseline_lsn,
                        "lsn_num": lsn_to_num(baseline_lsn),
                        "xid": None,
                        "commit_ts": self._get_state("baseline_captured_at"),
                        "change_count": count,
                    }
                )
            for row in self._conn.execute(
                """
                SELECT t.xid, t.commit_lsn, t.commit_lsn_num, t.commit_ts,
                       (SELECT COUNT(*) FROM artifact_history h
                         WHERE h.xid = t.xid) AS change_count
                FROM txns t ORDER BY t.commit_lsn_num, t.xid
                """
            ).fetchall():
                points.append(
                    {
                        "kind": "TXN",
                        "lsn": row["commit_lsn"],
                        "lsn_num": row["commit_lsn_num"],
                        "xid": row["xid"],
                        "commit_ts": row["commit_ts"],
                        "change_count": row["change_count"],
                    }
                )
            return points

    def get_directory_at(self, lsn_num: int) -> list[dict[str, Any]]:
        """查看某个提交点（含）为止的整目录：每个编号取最后一条变更。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, service_name, version, sha256, change_type
                FROM artifact_history
                WHERE id IN (
                    SELECT MAX(id) FROM artifact_history
                    WHERE commit_lsn_num <= ?
                    GROUP BY artifact_no
                )
                AND change_type != 'DELETE'
                ORDER BY artifact_no
                """,
                (lsn_num,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_history_of(self, artifact_no: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, change_type, service_name, version, sha256,
                       xid, commit_lsn, commit_lsn_num, captured_at
                FROM artifact_history WHERE artifact_no=?
                ORDER BY id
                """,
                (artifact_no,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_progress(self) -> dict[str, Any]:
        with self._lock:
            state = self.get_state()
            txn_count = self._conn.execute(
                "SELECT COUNT(*) AS c FROM txns"
            ).fetchone()["c"]
            history_rows = self._conn.execute(
                "SELECT COUNT(*) AS c FROM artifact_history"
            ).fetchone()["c"]
            return {
                "phase": state.get("phase", PHASE_INITIALIZING),
                "slot_name": state.get("slot_name"),
                "baseline_lsn": state.get("baseline_lsn"),
                "last_commit_lsn": state.get("last_commit_lsn"),
                "last_message_lsn": state.get("last_message_lsn"),
                "last_xid": int(state["last_xid"]) if state.get("last_xid") else None,
                "captured_transactions": txn_count,
                "history_rows": history_rows,
                "last_error": state.get("last_error") or None,
                "initialized_at": state.get("initialized_at"),
            }

    # ------------------------------------------------------------- 核验
    def create_verification(
        self,
        verification_id: str,
        boundary_lsn: str,
        expected_rows: list[dict[str, Any]],
    ) -> bool:
        """登记一次新核验。已存在 PENDING 核验时拒绝（同一把锁内原子检查）。

        返回 True=已创建，False=存在待捕获核验，调用方应返回 409。
        """
        import json

        lsn_num = lsn_to_num(boundary_lsn)
        with self._lock:
            exists = self._conn.execute(
                "SELECT 1 FROM verifications WHERE status=? LIMIT 1",
                (VERIFY_PENDING,),
            ).fetchone()
            if exists is not None:
                return False
            self._conn.execute(
                """
                INSERT INTO verifications(
                    id, status, boundary_lsn, boundary_lsn_num, expected_json,
                    source_rows, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    verification_id,
                    VERIFY_PENDING,
                    boundary_lsn,
                    lsn_num,
                    json.dumps(expected_rows, ensure_ascii=False, sort_keys=True),
                    len(expected_rows),
                    utc_now_iso(),
                ),
            )
            return True

    def has_pending_verification(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM verifications WHERE status=? LIMIT 1",
                (VERIFY_PENDING,),
            ).fetchone()
            return row is not None

    def list_verifications(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM verifications ORDER BY rowid DESC"
            ).fetchall()
            return [self._verification_row_to_dict(r) for r in rows]

    def get_verification(self, verification_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM verifications WHERE id=?", (verification_id,)
            ).fetchone()
            return self._verification_row_to_dict(row) if row else None

    def list_pending_verifications(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM verifications WHERE status=? ORDER BY rowid",
                (VERIFY_PENDING,),
            ).fetchall()
            return [self._verification_row_to_dict(r) for r in rows]

    def finish_verification(
        self,
        verification_id: str,
        status: str,
        missing: list[dict[str, Any]],
        extra: list[dict[str, Any]],
        mismatches: list[dict[str, Any]],
        captured_rows: int,
        error: str | None = None,
    ) -> bool:
        """把核验推进到终态。终态不可变：已是终态则原样保留并返回 False。"""
        import json

        assert status in _VERIFY_TERMINAL and status != VERIFY_UNCONFIRMED
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM verifications WHERE id=?", (verification_id,)
            ).fetchone()
            if row is None or row["status"] != VERIFY_PENDING:
                return False
            self._conn.execute(
                """
                UPDATE verifications SET status=?, missing_json=?, extra_json=?,
                       mismatches_json=?, captured_rows=?, error=?, finished_at=?
                WHERE id=?
                """,
                (
                    status,
                    json.dumps(missing, ensure_ascii=False),
                    json.dumps(extra, ensure_ascii=False),
                    json.dumps(mismatches, ensure_ascii=False),
                    captured_rows,
                    error,
                    utc_now_iso(),
                    verification_id,
                ),
            )
            return True

    def mark_pending_unconfirmed(self, reason: str) -> int:
        """所有未完成核验标记为未确认（应用重启 / 源库进入 ERROR 期）。

        终态（一致/不符/此前已未确认）一律不动：后续提交不得改变既有结论。
        返回被标记的行数。
        """
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE verifications SET status=?, unconfirmed_reason=?,
                       finished_at=COALESCE(finished_at, ?)
                WHERE status=?
                """,
                (VERIFY_UNCONFIRMED, reason, utc_now_iso(), VERIFY_PENDING),
            )
            return cur.rowcount

    @staticmethod
    def _verification_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        import json

        def _load(value: str | None) -> Any:
            return json.loads(value) if value is not None else None

        return {
            "id": row["id"],
            "status": row["status"],
            "boundary_lsn": row["boundary_lsn"],
            "boundary_lsn_num": row["boundary_lsn_num"],
            "expected": _load(row["expected_json"]),
            "source_rows": row["source_rows"],
            "missing": _load(row["missing_json"]),
            "extra": _load(row["extra_json"]),
            "mismatches": _load(row["mismatches_json"]),
            "captured_rows": row["captured_rows"],
            "unconfirmed_reason": row["unconfirmed_reason"],
            "error": row["error"],
            "created_at": row["created_at"],
            "finished_at": row["finished_at"],
        }
