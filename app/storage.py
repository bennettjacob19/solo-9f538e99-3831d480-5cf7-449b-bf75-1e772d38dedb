"""SQLite 历史库：只增不改的变更日志 + 捕获状态。

关键约束：
- 所有写入在一把进程内锁内串行化，SQLite 开启 WAL；
- 一个源事务的全部行变更与该事务的提交点在同一个 SQLite 事务里落盘，
  保证多行事务整体可见、不会出现半条事务；
- 复制槽的推进位置（last_commit_lsn）与对应数据在同一事务更新，
  崩溃重启后靠 xid 去重，绝不重复、也不丢数据。
"""
from __future__ import annotations

import json
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

-- 一次性核验：发起时固定的复制流边界与源目录快照，状态机推进，仅增不改
CREATE TABLE IF NOT EXISTS verifications (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       TEXT NOT NULL,
    boundary_lsn     TEXT NOT NULL,          -- 复制流边界（临时槽 consistent_point）
    boundary_lsn_num INTEGER NOT NULL,
    status           TEXT NOT NULL,          -- PENDING_CAPTURE/CONSISTENT/DIVERGED/UNCONFIRMED
    source_row_count INTEGER NOT NULL,
    result_json      TEXT,                   -- 比较结果，或 UNCONFIRMED 的中断原因
    completed_at     TEXT
);

-- 发起核验时固定的源目录整表快照（此后源库如何变化都不影响本次结论）
CREATE TABLE IF NOT EXISTS verification_rows (
    verification_id INTEGER NOT NULL,
    artifact_no     INTEGER NOT NULL,
    service_name    TEXT    NOT NULL,
    version         TEXT    NOT NULL,
    sha256          TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_vrows_vid ON verification_rows(verification_id);
"""

PHASE_INITIALIZING = "INITIALIZING"
PHASE_STREAMING = "STREAMING"
PHASE_ERROR = "ERROR"

# 一次性核验状态机
VERIFY_PENDING = "PENDING_CAPTURE"    # 待捕获：复制流边界尚未完整落入 SQLite
VERIFY_CONSISTENT = "CONSISTENT"      # 一致：按同一边界逐编号比较全部相同
VERIFY_DIVERGED = "DIVERGED"          # 不一致：存在缺失/额外/字段不符的制品
VERIFY_UNCONFIRMED = "UNCONFIRMED"    # 未确认：源断连或应用重启导致核验中断


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
        # 迁移：txns 增加"真实提交 LSN"列（COMMIT 消息的 data_start），
        # 供一次性核验做边界比较；历史旧行该列为 NULL，比较时回退 wal_end。
        txn_cols = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(txns)")
        }
        if "commit_record_lsn_num" not in txn_cols:
            self._conn.execute(
                "ALTER TABLE txns ADD COLUMN commit_record_lsn_num INTEGER"
            )

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
        commit_record_lsn_num: int | None = None,
    ) -> bool:
        """落盘一个已提交源事务。返回 True=新写入，False=重复事务（已跳过）。

        commit_lsn 用于展示/排序（wal_end）；commit_record_lsn_num 是该事务
        提交记录的真实 LSN（COMMIT 消息的 data_start），供一次性核验做
        边界比较——wal_end 是发送时的服务器 WAL 末尾，可能晚于真实提交点，
        不能用于"该事务是否落在边界内"的判定。
        """
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
                    "INSERT INTO txns(xid, commit_lsn, commit_lsn_num, commit_ts,"
                    " commit_record_lsn_num) VALUES (?, ?, ?, ?, ?)",
                    (xid, commit_lsn, lsn_num, commit_ts, commit_record_lsn_num),
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

    # ------------------------------------------------------------ 一次性核验
    def create_verification(
        self,
        boundary_lsn: str,
        rows: Iterable[sqlite3.Row | tuple[int, str, str, str]],
    ) -> int:
        """固定一次核验的复制流边界与源目录快照（仅新增，不改写已有历史）。"""
        rows = list(rows)
        boundary_num = lsn_to_num(boundary_lsn)
        now = utc_now_iso()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    INSERT INTO verifications(created_at, boundary_lsn,
                        boundary_lsn_num, status, source_row_count)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (now, boundary_lsn, boundary_num, VERIFY_PENDING, len(rows)),
                )
                vid = int(cur.lastrowid)
                self._conn.executemany(
                    """
                    INSERT INTO verification_rows(verification_id, artifact_no,
                        service_name, version, sha256)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [(vid, r[0], r[1], r[2], r[3]) for r in rows],
                )
                self._conn.execute("COMMIT")
                return vid
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _verification_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "boundary_lsn": row["boundary_lsn"],
            "boundary_lsn_num": row["boundary_lsn_num"],
            "status": row["status"],
            "source_row_count": row["source_row_count"],
            "result": (
                json.loads(row["result_json"]) if row["result_json"] else None
            ),
            "completed_at": row["completed_at"],
        }

    def get_verification(self, vid: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM verifications WHERE id=?", (vid,)
            ).fetchone()
            return self._verification_dict(row) if row else None

    def list_verifications(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM verifications ORDER BY id DESC"
            ).fetchall()
            return [self._verification_dict(r) for r in rows]

    def mark_pending_verifications_unconfirmed(self, reason: str) -> int:
        """把"待捕获"核验标记为"未确认"（源断连/应用重启时调用）。"""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE verifications SET status=?, result_json=? WHERE status=?",
                (VERIFY_UNCONFIRMED, json.dumps({"reason": reason}),
                 VERIFY_PENDING),
            )
            return cur.rowcount

    def _max_commit_record_lsn(self) -> int | None:
        row = self._conn.execute(
            "SELECT MAX(commit_record_lsn_num) AS m FROM txns"
        ).fetchone()
        return row["m"]

    def complete_due_verifications(self) -> int:
        """完成所有边界已完整落入 SQLite 的待捕获核验（捕获线程落盘后调用）。"""
        with self._lock:
            max_lsn = self._max_commit_record_lsn()
            if max_lsn is None:
                return 0
            due = self._conn.execute(
                "SELECT id FROM verifications"
                " WHERE status=? AND boundary_lsn_num<=?",
                (VERIFY_PENDING, max_lsn),
            ).fetchall()
            for row in due:
                self._finalize_verification(row["id"])
            return len(due)

    def try_complete_verification(self, vid: int) -> None:
        """惰性完成单个核验（查询接口调用）：边界已落入则立即比较落终态。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT status, boundary_lsn_num FROM verifications WHERE id=?",
                (vid,),
            ).fetchone()
            if not row or row["status"] != VERIFY_PENDING:
                return
            max_lsn = self._max_commit_record_lsn()
            if max_lsn is None or max_lsn < row["boundary_lsn_num"]:
                return
            self._finalize_verification(vid)

    def _finalize_verification(self, vid: int) -> None:
        """边界已完整落入：按同一边界重建捕获侧目录，逐编号比较并落终态。

        比较基于 <= 边界的仅增历史，该历史此后不再变化，故结论为终态，
        后续源库提交（LSN > 边界）不改变它。调用方需持有 self._lock。
        """
        boundary = self._conn.execute(
            "SELECT boundary_lsn_num FROM verifications WHERE id=?", (vid,)
        ).fetchone()["boundary_lsn_num"]
        source_rows = [
            dict(r)
            for r in self._conn.execute(
                "SELECT artifact_no, service_name, version, sha256"
                " FROM verification_rows WHERE verification_id=?",
                (vid,),
            ).fetchall()
        ]
        captured = self._directory_at_record_lsn_locked(boundary)
        result = _compare_directories(source_rows, captured)
        clean = not (result["missing"] or result["extra"] or result["mismatch"])
        self._conn.execute(
            "UPDATE verifications SET status=?, result_json=?, completed_at=?"
            " WHERE id=?",
            (
                VERIFY_CONSISTENT if clean else VERIFY_DIVERGED,
                json.dumps(result),
                utc_now_iso(),
                vid,
            ),
        )

    def _directory_at_record_lsn_locked(
        self, boundary_num: int
    ) -> list[dict[str, Any]]:
        """按真实提交 LSN 重建边界时刻的捕获侧目录（调用方需持锁）。

        用 txns.commit_record_lsn_num（真实提交 LSN）判定事务是否落在边界
        内；历史旧行该列为 NULL 时回退 commit_lsn_num（wal_end 上界）。
        基线行（xid 为 NULL）以基线 LSN 参与比较。
        """
        rows = self._conn.execute(
            """
            SELECT h.artifact_no, h.service_name, h.version, h.sha256
            FROM artifact_history h
            WHERE h.id IN (
                SELECT MAX(h2.id) FROM artifact_history h2
                LEFT JOIN txns t2 ON h2.xid = t2.xid
                WHERE COALESCE(t2.commit_record_lsn_num, h2.commit_lsn_num) <= ?
                GROUP BY h2.artifact_no
            )
            AND h.change_type != 'DELETE'
            ORDER BY h.artifact_no
            """,
            (boundary_num,),
        ).fetchall()
        return [dict(r) for r in rows]


def _compare_directories(
    source_rows: list[dict[str, Any]], captured_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """逐编号比较：缺失（源有捕获无）、额外（捕获有源无）、字段不符。"""
    src = {r["artifact_no"]: r for r in source_rows}
    cap = {r["artifact_no"]: r for r in captured_rows}
    missing = sorted(no for no in src if no not in cap)
    extra = sorted(no for no in cap if no not in src)
    mismatch: list[dict[str, Any]] = []
    for no in sorted(src):
        if no not in cap:
            continue
        diffs: dict[str, Any] = {}
        for field in ("service_name", "version", "sha256"):
            if src[no][field] != cap[no][field]:
                diffs[field] = {
                    "source": src[no][field],
                    "captured": cap[no][field],
                }
        if diffs:
            mismatch.append({"artifact_no": no, "fields": diffs})
    return {"missing": missing, "extra": extra, "mismatch": mismatch}
