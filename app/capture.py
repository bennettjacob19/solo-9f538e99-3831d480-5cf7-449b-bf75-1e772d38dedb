"""捕获工作线程：一致基线 + test_decoding 逻辑复制 + 崩溃续接。

一致性与不丢事务的原理
----------------------
1. 在一个普通事务里调用 pg_create_logical_replication_slot 建槽并
   pg_export_snapshot 导出快照，槽的起始 LSN 对应一个精确一致性点；
2. 另开一条普通连接 SET TRANSACTION SNAPSHOT 复用该快照做整表基线；
3. 从槽的 LSN 开始读逻辑变更流。快照时刻"正在进行中"的事务不会进基线，
   而它们提交后必然出现在复制流里——基线前开始、基线后提交的事务不会漏；
4. 每个源事务先整体写 SQLite（数据 + 提交点同事务），成功后才通过
   复制协议 send_feedback 推进源端槽位。任何时刻强杀：未落盘的事务槽位
   不会前进，重启后重放并按 xid 幂等去重，已保存历史与续接位置永远一致。
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

import psycopg2
import psycopg2.extras

from .config import Settings
from .decoder import parse_message
from .source import SourceDB
from .storage import PHASE_STREAMING, HistoryStore
from .verification import evaluate_pending

log = logging.getLogger("capture")


class CaptureWorker:
    def __init__(self, store: HistoryStore, source: SourceDB,
                 settings: Settings) -> None:
        self._store = store
        self._source = source
        self._settings = settings
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._current_xid: int | None = None
        self._current_changes: list[dict[str, Any]] = []

    # ----------------------------------------------------------------- life
    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._safe_loop, name="capture-worker", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _safe_loop(self) -> None:
        try:
            self._run()
        except Exception as exc:  # 线程兜底：错误暴露到 /api/progress
            log.exception("capture worker crashed: %s", exc)
            try:
                self._store.mark_error(f"capture worker crashed: {exc}")
            except Exception:
                pass

    # ----------------------------------------------------------------- main
    def _run(self) -> None:
        # 应用（重新）启动：上一进程里未走完的核验无法保证原观察过程，
        # 一律转"未确认"，值班工程师可在网页重新发起；已形成的结论不动。
        marked = self._store.mark_pending_unconfirmed(
            "应用重启，上一轮核验观察过程中断"
        )
        if marked:
            log.warning(
                "%d pending verification(s) marked UNCONFIRMED after restart",
                marked,
            )
        self._source.ensure_schema()
        state = self._store.get_state()
        if state.get("baseline_lsn"):
            log.info("resuming capture from slot %s", self._settings.slot_name)
        else:
            self._initialize()
        self._stream_forever()

    def _start_inflight_hook(self):
        """测试钩子：开一个源事务并保持打开，用于构造跨基线事务。

        返回该事务的独立连接；调用方在基线落盘后 commit。默认禁用。
        """
        import psycopg2

        no = self._settings.inflight_hook_no
        if not no:
            return None
        conn = psycopg2.connect(self._settings.pg_dsn)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                "INSERT INTO artifacts(artifact_no, service_name, version,"
                " sha256) VALUES (%s, %s, %s, %s)",
                (no, "inflight-hook-svc", "0.0.9",
                 "a" * 64),
            )
        log.info(
            "TEST hook: inflight transaction opened on artifact_no=%s,"
            " holding for %.1fs before baseline",
            no, self._settings.inflight_hook_sleep_s,
        )
        return conn

    def _initialize(self) -> None:
        """首次基线。初始化未完成前 phase 始终为 INITIALIZING，网页不开放历史。"""
        self._store.mark_initializing(self._settings.slot_name)

        if self._settings.init_sleep_ms:  # 测试钩子：拉长初始化窗口
            self._stop.wait(self._settings.init_sleep_ms / 1000.0)

        slot = self._settings.slot_name
        inflight_conn = None
        try:
            if self._source.slot_exists(slot):
                # 历史库为空但槽已存在（例如初始化中途被杀）：废弃旧槽重来，
                # 因为基线数据从未落盘，旧槽没有任何需要保留的已确认位置。
                log.warning(
                    "orphan slot %s found, dropping for clean baseline", slot
                )
                with self._source.connect() as conn, conn.cursor() as cur:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))

            # 1) 先建槽并导出一致性快照（拿到精确的基线 LSN）
            consistent_lsn, slot_conn, snapshot_id = (
                self._source.create_slot_with_snapshot(slot)
            )
            try:
                # 2) 测试钩子：快照点之后、基线读取之前打开并持有源事务。
                #    它的 xid 对该快照不可见（不进基线），提交后又必然
                #    出现在从槽 LSN 开始的逻辑流里（不遗漏）。
                inflight_conn = self._start_inflight_hook()
                if inflight_conn is not None:
                    self._stop.wait(self._settings.inflight_hook_sleep_s)

                # 3) 复用快照读基线整目录
                rows = self._source.baseline_rows_using_snapshot(snapshot_id)
                # 整批基线在一个 SQLite 事务里落盘；落盘前不向源端确认任何东西
                self._store.save_baseline(rows, consistent_lsn)
                log.info(
                    "baseline saved: %d rows at %s (snapshot %s)",
                    len(rows), consistent_lsn, snapshot_id,
                )
            finally:
                slot_conn.close()
            self._store.mark_streaming()
        finally:
            # 基线已落盘：此刻才提交在途事务。它开始于快照点之后，
            # 不会出现在基线里，必须由逻辑复制流补捕获。
            if inflight_conn is not None:
                try:
                    inflight_conn.commit()
                    log.info(
                        "TEST hook: inflight transaction committed after baseline"
                    )
                finally:
                    inflight_conn.close()

    # ------------------------------------------------------------- streaming
    def _stream_forever(self) -> None:
        slot = self._settings.slot_name
        backoff = max(1, self._settings.reconnect_backoff_s)
        transient_streak = 0  # 连续瞬断次数；未超阈值不降级为 ERROR
        while not self._stop.is_set():
            # 测试钩子：连接建立前暂停（此时不存在 walsender，源端数据
            # 无法进入客户端缓冲），用于确定性构造断连场景。默认关闭。
            if self._settings.stream_connect_delay_ms:
                self._stop.wait(
                    self._settings.stream_connect_delay_ms / 1000.0
                )
            repl_conn = None
            try:
                if not self._source.slot_exists(slot):
                    raise RuntimeError(
                        f"replication slot '{slot}' missing on source"
                    )
                repl_conn = psycopg2.connect(
                    self._settings.pg_dsn,
                    connection_factory=psycopg2.extras.LogicalReplicationConnection,
                )
                # ReplicationConnection 固定 autocommit，属性只读
                cur = repl_conn.cursor()
                cur.start_replication(
                    slot_name=slot,
                    options={"include-xids": "1", "include-timestamp": "1"},
                )
                log.info("replication stream started on slot %s", slot)
                transient_streak = 0
                # 重连成功即恢复正常服务状态，并清空历史断线提示
                if self._store.get_phase() != PHASE_STREAMING:
                    self._store.mark_streaming()
                else:
                    self._store.clear_error()
                self._pump_stream(cur)
            except Exception as exc:
                if self._stop.is_set():
                    break
                transient_streak += 1
                # 源断连即打断核验的连续观察：未完成核验立刻转"未确认"，
                # 不等待重试结果，也不改动任何已形成的结论。
                marked = self._store.mark_pending_unconfirmed(
                    f"源库连接中断: {exc}"
                )
                if marked:
                    log.warning(
                        "%d pending verification(s) marked UNCONFIRMED"
                        " after source disconnect", marked
                    )
                log.warning(
                    "replication stream broken (streak=%d): %r; reconnecting",
                    transient_streak, exc, exc_info=transient_streak >= 3,
                )
                # 应用刚重启时旧 walsender 释放槽位有短暂延迟，属可恢复瞬断：
                # 保留 STREAMING，历史查询继续可用；连续失败才降级为 ERROR。
                if transient_streak >= 3:
                    self._store.mark_error(
                        f"replication stream broken: {exc}"
                    )
                self._stop.wait(backoff)
            finally:
                if repl_conn is not None:
                    try:
                        repl_conn.close()
                    except Exception:
                        pass

    # ------------------------------------------------------------- streaming
    def _pump_stream(self, cur: Any) -> None:
        """自管复制流循环（替代 consume_stream 的阻塞 select）。

        为什么不用 consume_stream：它在源库空闲时阻塞在 select 上，服务端
        最长要等 wal_sender_timeout/2（默认约 30s）才自发发一次 keepalive，
        核验流水位会长时间停滞。这里改为：
        - 用 select 等 socket 可读（最多 1s，停止信号可及时打断），
          再非阻塞排空 libpq 已缓冲的消息；
        - 每 REPL_STATUS_INTERVAL_S 秒主动 send_feedback(reply=1)，
          请求服务端**立即**回一条带当前 WAL 位置的 keepalive，
          从而即使源库空闲，流水位也能秒级推进、核验不会干等。

        psycopg2 的 read_message() 会在 C 层消费 Primary keepalive 但不向
        Python 上抛（goto retry），不过它同时把 cursor.wal_end 更新为
        keepalive 携带的服务端 WAL 位置——因此每轮循环都读 cur.wal_end
        推进流水位，覆盖"消息被吞"的场景；XLogData 消息自身的 wal_end
        也不小于该值，水位单调不减。
        feedback 只携带已落盘提交点（flush_lsn），绝不确认未落盘更改。
        """
        import select

        interval = max(1, self._settings.status_interval_s)
        # 测试钩子：连接建立后先暂停，确定性制造复制积压（默认 0=关闭）
        if self._settings.pump_pause_ms:
            self._stop.wait(self._settings.pump_pause_ms / 1000.0)
        last_feedback = time.monotonic()
        while not self._stop.is_set():
            # 非阻塞排空 libpq 已缓冲的消息（keepalive 在 C 层被消费，
            # 不会返回，但会更新 cur.wal_end）
            while True:
                msg = cur.read_message()  # 无已缓冲消息返回 None
                if msg is None:
                    break
                self._handle_message(msg)
            # 关键：cur.wal_end 同时反映最近 XLogData 与被吞 keepalive 的
            # 服务端 WAL 位置，用它推进核验流水位（0 表示尚未收到任何消息）。
            if int(cur.wal_end) > 0:
                self._store.update_stream_watermark(int(cur.wal_end))
            evaluate_pending(self._store)
            now = time.monotonic()
            if now - last_feedback >= interval:
                last_feedback = now
                # reply=1：请服务端立刻回 keepalive，带回当前 WAL 位置；
                # flush_lsn 用已落盘提交点，保持"先落盘后确认"不变量。
                self._send_periodic_feedback(cur)
            # 等 socket 可读或 1s 超时：既及时收到 keepalive，又能周期唤醒
            try:
                select.select([cur], [], [], 1.0)
            except (OSError, ValueError):
                # 连接关闭/文件描述符失效：下一轮 read_message 抛错，
                # 由外层重连循环统一处理
                if self._stop.wait(0.2):
                    break

    def _send_periodic_feedback(self, cur: Any) -> None:
        state = self._store.get_state()
        flush_text = state.get("last_commit_lsn")
        kwargs: dict[str, Any] = {"reply": True}
        if flush_text:
            high, low = flush_text.split("/", 1)
            kwargs["flush_lsn"] = (int(high, 16) << 32) | int(low, 16)
        # 注意：send_feedback 的 C 签名不接受 None，无已落盘提交点时
        # 省略 flush_lsn（协议默认 0，表示尚未确认任何事务）。
        try:
            cur.send_feedback(**kwargs)
        except Exception:
            # 连接问题交给外层重连循环处理，不在循环内吞错重启
            log.debug("periodic feedback failed", exc_info=True)
            raise

    def _handle_message(self, msg: Any) -> None:
        if self._stop.is_set():
            raise psycopg2.OperationalError("shutting down")

        # 任何复制流消息（含无载荷 keepalive）都带服务端当前 WAL 位置；
        # 用它推进流水位，使源库空闲时核验也能等到"边界完整落入"。
        self._store.update_stream_watermark(int(msg.wal_end))

        if msg.data_start == 0:  # Primary keepalive 消息无载荷
            # 不主动确认：只随已落盘事务推进，绝不确认尚未落盘的更改
            evaluate_pending(self._store)
            return

        data = msg.payload.decode("utf-8")
        parsed = parse_message(data, self._settings.table_name)
        if parsed is None:
            # 无关消息（DDL/其他表/序列…）：不回送任何 feedback。
            # 槽位只随"已落盘事务"通过 send_feedback 推进，
            # 保证任何尚未落盘的更改都不会向源库确认。
            evaluate_pending(self._store)
            return

        kind = parsed["kind"]
        if kind == "BEGIN":
            self._current_xid = parsed["xid"]
            self._current_changes: list[dict[str, Any]] = []
        elif kind == "CHANGE":
            self._current_changes.append(parsed)
        elif kind == "COMMIT":
            # test_decoding: COMMIT 行本身不带 LSN，消息 wal_end 即该事务
            # 提交记录结束位置，用作该提交点的 LSN。
            commit_lsn = msg.wal_end
            self._finish_transaction(parsed["xid"], commit_lsn, data, msg)
            # 事务已落盘、流水位已推进：尝试让待捕获核验在此形成结论
            evaluate_pending(self._store)
        self._store.update_last_message_lsn(str(msg.wal_end))

    def _finish_transaction(self, xid: int, commit_lsn: int,
                            raw_commit_msg: str, msg: Any) -> None:
        changes = getattr(self, "_current_changes", [])
        # include-timestamp=1 时形如 "COMMIT 12345 (at 2026-11-16 10:00:00.0+00)"
        commit_ts = None
        marker = "(at "
        if marker in raw_commit_msg:
            commit_ts = raw_commit_msg.split(marker, 1)[1].rstrip(")")

        lsn_text = f"{commit_lsn >> 32:X}/{commit_lsn & 0xFFFFFFFF:08X}"
        written = self._store.persist_transaction(
            xid, lsn_text, commit_ts, changes
        )
        if written:
            log.info(
                "txn %s persisted at %s with %d change(s)",
                xid, lsn_text, len(changes),
            )
        else:
            log.info("txn %s replayed after restart, skipped (idempotent)", xid)

        # 数据已落盘（或此前已落盘）——现在才通过复制协议向源端确认，
        # 推进该槽的 confirmed_flush_lsn。绝不在落盘前发送 feedback。
        msg.cursor.send_feedback(flush_lsn=commit_lsn)
        self._current_changes = []
        self._current_xid = None
