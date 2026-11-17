"""一次性核验编排：固定源目录 + 复制流边界，等待边界落入后逐编号比较。

一致性与不锁源表的原理
----------------------
1. 复用与基线相同的机制建一个**临时**逻辑复制槽：槽的 consistent_point
   与 pg_export_snapshot 导出的一致性快照严格对齐，作为本次核验的复制
   流边界 B；用该快照读整张源目录并固定存盘——这就是"B 时刻的源目录"。
   读完立即删临时槽，全程只读快照（MVCC），不持有快照、不锁源表，
   核验等待期间普通 SQL 可继续改目录。
2. 提交一个空标记事务（pg_logical_emit_message），其提交 LSN > B，推动
   主复制流前进，保证"边界完整落入 SQLite"的判定在源库静默时也收敛。
3. 捕获线程按 LSN 顺序落盘。当已落盘事务的最大真实提交 LSN（COMMIT
   消息的 data_start）达到 B 时，B 之前的所有事务必然都已落盘——此时
   用同一边界 B 重建捕获侧目录，与固定的源目录逐编号比较。
4. 结论（一致/不一致）一旦落盘即为终态：比较基于 <= B 的仅增历史，
   后续源库提交（LSN > B）不改变本次结论。
"""
from __future__ import annotations

import logging
import threading
from uuid import uuid4

from .source import SourceDB
from .storage import HistoryStore

log = logging.getLogger("verify")

VERIFY_SLOT_PREFIX = "verify_tmp_"


class Verifier:
    def __init__(self, store: HistoryStore, source: SourceDB) -> None:
        self._store = store
        self._source = source
        # 串行化"发起"动作，避免并发建临时槽相互影响
        self._start_lock = threading.Lock()

    def start_verification(self) -> int:
        """固定源目录与复制流边界，返回核验 id。不锁源表、不改源目录。"""
        with self._start_lock:
            # 清理上次可能残留的临时槽（例如应用在删槽前崩溃）
            dropped = self._source.drop_slots_with_prefix(VERIFY_SLOT_PREFIX)
            if dropped:
                log.warning("dropped %d leftover verify slot(s)", dropped)

            slot = f"{VERIFY_SLOT_PREFIX}{uuid4().hex[:12]}"
            boundary, slot_conn, snapshot_id = (
                self._source.create_slot_with_snapshot(slot)
            )
            try:
                rows = self._source.baseline_rows_using_snapshot(snapshot_id)
            finally:
                slot_conn.close()
            # 临时槽使命完成，立即删除（WAL 保留由主复制槽负责）
            try:
                self._source.drop_slot(slot)
            except Exception as exc:
                log.warning("drop verify slot %s failed: %s", slot, exc)

            vid = self._store.create_verification(boundary, rows)

            # 空标记事务：推动主复制流越过边界，保证落入判定收敛。
            # 失败（如非超级用户）不阻塞核验，降级为等待自然事务越过边界。
            try:
                self._source.emit_boundary_marker(str(vid))
            except Exception as exc:
                log.warning(
                    "verify marker txn failed, rely on natural txns: %s", exc
                )
            log.info(
                "verification %d started: boundary=%s, %d source row(s)",
                vid, boundary, len(rows),
            )
            return vid
