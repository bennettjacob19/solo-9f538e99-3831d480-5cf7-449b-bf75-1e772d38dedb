"""一次性核验的状态机与逐编号比较单元测试（无需 PostgreSQL）。

运行：python3 -m unittest tests.test_verification -v
"""
import os
import tempfile
import unittest

from app.storage import (
    HistoryStore,
    VERIFY_CONSISTENT,
    VERIFY_DIVERGED,
    VERIFY_PENDING,
    VERIFY_UNCONFIRMED,
)
from app.verification import evaluate_pending


def _row(no, svc="svc", ver="1.0.0", sha="a" * 64):
    return {
        "artifact_no": no,
        "service_name": svc,
        "version": ver,
        "sha256": sha,
    }


class VerificationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = HistoryStore(os.path.join(self.tmp.name, "v.db"))
        # 模拟捕获已进入流式阶段
        self.store.mark_initializing("slot")
        self.store.save_baseline(
            [(1, "svc", "1.0.0", "a" * 64), (2, "svc", "1.0.0", "a" * 64)],
            "0/100",
        )
        self.store.mark_streaming()

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _create(self, vid, boundary, rows):
        return self.store.create_verification(vid, boundary, rows)

    def test_pending_until_watermark_reaches_boundary(self):
        self._create("v1", "0/200", [_row(1), _row(2)])
        self.assertEqual(self.store.get_verification("v1")["status"], VERIFY_PENDING)
        # 水位未到边界：仍然待捕获
        self.store.update_stream_watermark(0x1FF)
        self.assertEqual(evaluate_pending(self.store), 0)
        self.assertEqual(self.store.get_verification("v1")["status"], VERIFY_PENDING)
        # 水位越过边界：形成结论
        self.store.update_stream_watermark(0x200)
        self.assertEqual(evaluate_pending(self.store), 1)
        self.assertEqual(
            self.store.get_verification("v1")["status"], VERIFY_CONSISTENT
        )

    def test_consistent_when_identical(self):
        self._create("v1", "0/200", [_row(1), _row(2)])
        self.store.update_stream_watermark(0x300)
        evaluate_pending(self.store)
        v = self.store.get_verification("v1")
        self.assertEqual(v["status"], VERIFY_CONSISTENT)
        self.assertEqual(v["captured_rows"], 2)
        self.assertEqual(v["missing"], [])
        self.assertEqual(v["extra"], [])
        self.assertEqual(v["mismatches"], [])

    def test_missing_extra_and_field_mismatch(self):
        # 期望：1、2、3；本地（基线）只有 1、2，且 2 的字段不同 ->
        # 3 缺失、字段不符；再造一个本地额外编号 9
        self.store.persist_transaction(
            500, "0/150", None,
            [
                {"change_type": "UPDATE", "artifact_no": 2,
                 "service_name": "changed", "version": "9", "sha256": "c" * 64},
                {"change_type": "INSERT", "artifact_no": 9,
                 "service_name": "ghost", "version": "1", "sha256": "d" * 64},
            ],
        )
        self._create("v1", "0/200", [_row(1), _row(2), _row(3)])
        self.store.update_stream_watermark(0x250)
        evaluate_pending(self.store)
        v = self.store.get_verification("v1")
        self.assertEqual(v["status"], VERIFY_DIVERGED)
        self.assertEqual([r["artifact_no"] for r in v["missing"]], [3])
        self.assertEqual([r["artifact_no"] for r in v["extra"]], [9])
        self.assertEqual(v["mismatches"][0]["artifact_no"], 2)
        fields = {f["field"]: f for f in v["mismatches"][0]["fields"]}
        self.assertEqual(set(fields), {"service_name", "version", "sha256"})
        self.assertEqual(fields["version"]["expected"], "1.0.0")
        self.assertEqual(fields["version"]["actual"], "9")

    def test_terminal_result_immutable_to_later_writes(self):
        self._create("v1", "0/200", [_row(1), _row(2)])
        self.store.update_stream_watermark(0x200)
        evaluate_pending(self.store)
        self.assertEqual(
            self.store.get_verification("v1")["status"], VERIFY_CONSISTENT
        )
        # 之后源库继续提交、水位继续推进，结论不得改变
        self.store.persist_transaction(
            501, "0/300", None,
            [{"change_type": "DELETE", "artifact_no": 1}],
        )
        self.store.update_stream_watermark(0x400)
        self.assertEqual(evaluate_pending(self.store), 0)
        v = self.store.get_verification("v1")
        self.assertEqual(v["status"], VERIFY_CONSISTENT)
        # 断连处理也不得改写终态
        self.assertEqual(self.store.mark_pending_unconfirmed("x"), 0)
        self.assertEqual(v["status"], VERIFY_CONSISTENT)

    def test_restart_and_disconnect_mark_pending_unconfirmed(self):
        import json

        full = [_row(1), _row(2)]
        self._create("v1", "0/200", full)
        # 服务层只允许一个 PENDING；这里直接写库模拟历史遗留的第二条待捕获，
        # 验证重启/断连标记能覆盖所有未完成行而不动终态行。
        with self.store._lock:
            self.store._conn.execute(
                "INSERT INTO verifications(id, status, boundary_lsn,"
                " boundary_lsn_num, expected_json, source_rows, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("v2", VERIFY_PENDING, "0/210", 0x210,
                 json.dumps(full), len(full), "t"),
            )
        self.store.update_stream_watermark(0x205)
        evaluate_pending(self.store)  # v1 完成，v2 仍待捕获
        self.assertEqual(
            self.store.get_verification("v1")["status"], VERIFY_CONSISTENT
        )
        n = self.store.mark_pending_unconfirmed("应用重启，观察过程中断")
        self.assertEqual(n, 1)
        self.assertEqual(
            self.store.get_verification("v2")["status"], VERIFY_UNCONFIRMED
        )
        self.assertIsNotNone(
            self.store.get_verification("v2")["unconfirmed_reason"]
        )
        # 未确认同样是终态：重复标记为 0 行
        self.assertEqual(self.store.mark_pending_unconfirmed("again"), 0)
        # 已确认的结论不受影响
        self.assertEqual(
            self.store.get_verification("v1")["status"], VERIFY_CONSISTENT
        )

    def test_pending_guard_and_listing(self):
        self.assertTrue(not self.store.has_pending_verification())
        self.assertTrue(self._create("v1", "0/200", [_row(1)]))
        self.assertTrue(self.store.has_pending_verification())
        self.assertEqual(len(self.store.list_verifications()), 1)
        self.assertIsNone(self.store.get_verification("nope"))

    def test_create_rejected_while_pending_atomically(self):
        # 已有待捕获核验时，新登记在同一把锁内被原子拒绝
        self.assertTrue(self._create("v1", "0/200", [_row(1)]))
        self.assertFalse(
            self.store.create_verification("v2", "0/210", [_row(1)])
        )
        self.assertIsNone(self.store.get_verification("v2"))
        # v1 完成后才允许新核验
        self.store.update_stream_watermark(0x200)
        evaluate_pending(self.store)
        self.assertTrue(self._create("v3", "0/300", [_row(1)]))

    def test_finish_unknown_or_terminal_is_noop(self):
        self.assertFalse(
            self.store.finish_verification(
                "ghost", VERIFY_DIVERGED, [], [], [], 0
            )
        )


if __name__ == "__main__":
    unittest.main()
