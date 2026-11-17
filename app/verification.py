"""一次性核验：固定边界后的逐编号比对。

流程
----
1. 发起（网页/API）：在一个短的 REPEATABLE READ 事务里快照源库整张
   artifacts 目录，并取 pg_current_wal_lsn() 作为复制流边界；两者一并
   登记到 SQLite（verifications 表，只增不改）。事务立即结束，不在源表
   上持锁，核验等待期间普通 SQL 照常改目录。
2. 等待：捕获线程每收到一条复制流消息（含 keepalive）就推进流水位；
   水位越过边界 LSN 前，核验显示"待捕获"。
3. 结论：水位越过边界后，按同一边界从本地历史重建目录（取 ≤ 边界 LSN
   的最后一条变更），与快照逐编号比较，列出缺失、额外、字段不符；
   全部相同才判"一致"。结论一经写库即不可变，之后源库的任何提交都不会
   改变本次结论。
4. 中断：源断连或应用重启时，未完成的核验一律转"未确认"，可重新发起；
   核验全程只读源库、只写本地核验表，不改写源目录与已有历史。
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

from .source import SourceDB
from .storage import VERIFY_CONSISTENT, VERIFY_DIVERGED, HistoryStore

log = logging.getLogger("verification")

_FIELDS = ("service_name", "version", "sha256")


class VerificationError(Exception):
    """发起核验失败（源库不可达等）。待捕获冲突由存储层原子拒绝。"""


class PendingVerificationConflict(Exception):
    """已存在待捕获核验，新发起被原子拒绝（HTTP 409）。"""


def start_verification(store: HistoryStore, source: SourceDB) -> dict[str, Any]:
    """固定源目录快照与复制流边界，登记一次新核验。"""
    try:
        rows, boundary_lsn = source.snapshot_directory()
    except Exception as exc:  # 源库不可达：不产生任何核验记录
        raise VerificationError(f"无法连接源库固定快照: {exc}") from exc

    verification_id = "v-" + uuid.uuid4().hex
    # 检查与插入在存储层同一把锁内原子完成，并发 POST 只会成功一个
    if not store.create_verification(verification_id, boundary_lsn, rows):
        raise PendingVerificationConflict(
            "已有待捕获的核验，请等待其得出结论或在中断后重新发起"
        )
    log.info(
        "verification %s created at boundary %s with %d source rows",
        verification_id, boundary_lsn, len(rows),
    )
    # 登记后立即尝试一次评估（边界可能恰好已被复制流越过）
    evaluate_pending(store)
    result = store.get_verification(verification_id)
    assert result is not None
    return result


def evaluate_pending(store: HistoryStore) -> int:
    """检查所有待捕获核验：边界已完整落入则形成不可变结论。

    返回本次形成结论的核验数量。任何异常都不向上抛——核验失败不能
    影响捕获主链路。
    """
    finalized = 0
    watermark = store.get_stream_watermark()
    if watermark is None:
        return 0
    for item in store.list_pending_verifications():
        try:
            if watermark < item["boundary_lsn_num"]:
                continue  # 边界尚未完整落入 SQLite，继续显示待捕获
            finalized += 1 if _finalize_one(store, item) else 0
        except Exception:
            log.exception("verification %s evaluation failed", item["id"])
    return finalized


def _finalize_one(store: HistoryStore, item: dict[str, Any]) -> bool:
    boundary_num = item["boundary_lsn_num"]
    # 按同一边界从仅增历史重建本地目录：每个编号取 ≤ 边界的最后一条变更
    captured_rows = store.get_directory_at(boundary_num)
    expected = {r["artifact_no"]: r for r in item["expected"]}
    captured = {r["artifact_no"]: r for r in captured_rows}

    missing = [expected[no] for no in expected.keys() - captured.keys()]
    extra = [captured[no] for no in captured.keys() - expected.keys()]
    mismatches: list[dict[str, Any]] = []
    for no in expected.keys() & captured.keys():
        diffs: list[dict[str, Any]] = []
        for field in _FIELDS:
            if expected[no][field] != captured[no][field]:
                diffs.append(
                    {
                        "field": field,
                        "expected": expected[no][field],
                        "actual": captured[no][field],
                    }
                )
        if diffs:
            mismatches.append({"artifact_no": no, "fields": diffs})

    missing.sort(key=lambda r: r["artifact_no"])
    extra.sort(key=lambda r: r["artifact_no"])
    mismatches.sort(key=lambda r: r["artifact_no"])
    status = (
        VERIFY_CONSISTENT
        if not (missing or extra or mismatches)
        else VERIFY_DIVERGED
    )
    changed = store.finish_verification(
        item["id"],
        status,
        missing,
        extra,
        mismatches,
        captured_rows=len(captured_rows),
    )
    if changed:
        log.info(
            "verification %s finalized as %s (missing=%d extra=%d mismatch=%d)",
            item["id"], status, len(missing), len(extra), len(mismatches),
        )
    return changed
