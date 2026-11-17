#!/usr/bin/env bash
# 一次性核验端到端自测：便携 PostgreSQL + uvicorn，覆盖
#   1. 正常核验：待捕获 -> 一致；结论不受后续提交影响（不可变）
#   2. 核验等待期间并发改目录：普通 SQL 不被阻塞，结论仍与边界快照一致
#   3. 应用重启：中断的核验显示未确认，可重新发起；既有结论保留
#   4. 源断连：中断的核验显示未确认，重连后可重新发起
#   5. 冲突保护、缺失/额外/字段不符展示、不锁源表
# 用法：bash tests/selftest_verify.sh
set -u
cd "$(dirname "$0")/.."

PGBIN=${PGBIN:-/tmp/pgdeb/extracted/usr/lib/postgresql/15/bin}
PGCLIENT=${PGCLIENT:-/tmp/pgdeb/client/usr/lib/postgresql/15/bin}
export PATH="$PGCLIENT:$PGBIN:$PATH"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/tmp/pgdeb/extracted/usr/lib/x86_64-linux-gnu
PGDATA=/tmp/selftest-verify-pgdata
PGPORT=55434
PGSOCK=/tmp/selftest-verify-pgsock
SQLDIR=/tmp/selftest-verify-sqlite
APPPORT=8012
BASE="http://127.0.0.1:$APPPORT"
SHA64="a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
DSN="host=$PGSOCK port=$PGPORT dbname=postgres user=postgres"
PASS=0; FAIL=0

ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
check(){ if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 (got '$1' want '$2')"; fi; }
api(){ curl -s -m 10 "http://127.0.0.1:$APPPORT$1"; }
# 发起核验：uvicorn 刚就绪时首个 POST 可能连接失败，做有限重试
post_verify(){
  local r id
  for _ in $(seq 1 10); do
    r=$(curl -s -m 10 -X POST "$BASE/api/verifications" 2>/dev/null || true)
    id=$(echo "$r" | jq -r .id 2>/dev/null)
    case "$id" in v-*) echo "$r"; return 0;; esac
    sleep 0.5
  done
  echo "$r"
}
psqlq(){ psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc "$1"; }
wait_phase(){
  local want=$1 deadline=$((SECONDS+$2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(api /api/progress | jq -r .phase)" = "$want" ] && return 0
    sleep 0.5
  done
  return 1
}
wait_status(){ # wait_status <vid> <status> <timeout_s>
  local vid=$1 want=$2 deadline=$((SECONDS+$3)) s
  while [ "$SECONDS" -lt "$deadline" ]; do
    s=$(api "/api/verifications/$vid" | jq -r .status)
    [ "$s" = "$want" ] && return 0
    sleep 0.5
  done
  return 1
}
app_pid(){ ps -eo pid,comm,args | awk '$2=="python3" && /uvicorn app.main:app/ {print $1}'; }
pg_ctl_q(){ pg_ctl -D "$PGDATA" -l /tmp/selftest-verify-pg.log "$@"; }

start_app(){ # start_app [pump_pause_ms] [connect_delay_ms]
  local pause_ms=${1:-0} connect_delay_ms=${2:-0}
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "$BASE/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$SQLDIR"
  PG_DSN="$DSN" SQLITE_PATH="$SQLDIR/history.db" \
    TEST_PUMP_PAUSE_MS="$pause_ms" \
    TEST_STREAM_CONNECT_DELAY_MS="$connect_delay_ms" \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-verify-app.log 2>&1 < /dev/null &
  local pid=$!
  for _ in $(seq 1 40); do
    kill -0 "$pid" 2>/dev/null || { echo "app exited:"; cat /tmp/selftest-verify-app.log; return 1; }
    if curl -s -m 1 "$BASE/api/progress" >/dev/null 2>&1 && [ "$(app_pid)" = "$pid" ]; then
      return 0
    fi
    sleep 0.5
  done
  echo "app not ready:"; cat /tmp/selftest-verify-app.log; return 1
}
stop_app(){
  local pid; pid=$(app_pid)
  [ -n "$pid" ] && kill $pid 2>/dev/null || true
  for _ in $(seq 1 20); do app_pid | grep -q . || return 0; sleep 0.5; done
}
cleanup(){
  stop_app || true
  pg_ctl_q stop -m fast >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ------------------------------------------------------------- 0. 准备
echo "=== 0. 准备全新源 PostgreSQL ==="
pg_ctl_q stop -m fast >/dev/null 2>&1 || true
rm -rf "$PGDATA" "$PGSOCK" "$SQLDIR"; mkdir -p "$PGSOCK"
initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --no-sync >/dev/null
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '$PGSOCK'
listen_addresses = ''
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF
pg_ctl_q start -w >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -q -f init/01_schema.sql
check "$(psqlq 'select count(*) from artifacts')" "3" "源表 3 行示例数据"

start_app || { echo "应用启动失败"; exit 1; }
wait_phase STREAMING 20 && ok "捕获进入 STREAMING" || bad "初始化超时"

# ------------------------------------------------------------- 1. 正常核验
echo "=== 1. 正常核验：发起固定边界，最终一致 ==="
vid=$(post_verify | jq -r .id)
case "$vid" in v-*) ok "核验已发起: $vid";; *) bad "核验发起失败: $vid";; esac
st1=$(api "/api/verifications/$vid" | jq -r .status)
case "$st1" in
  PENDING|CONSISTENT) ok "初始状态合法（待捕获/已完成）: $st1" ;;
  *) bad "核验初始状态异常: $st1" ;;
esac
check "$(api "/api/verifications/$vid" | jq -r .source_rows)" "3" "固定了发起时刻整张目录（3 行）"
wait_status "$vid" CONSISTENT 15 && ok "核验完成：一致" || bad "核验未在超时内完成"
v=$(api "/api/verifications/$vid")
check "$(echo "$v" | jq -r .captured_rows)" "3" "本地按同一边界重建也是 3 行"
check "$(echo "$v" | jq -r '[.missing,.extra,.mismatches]|map(length)|add')" "0" "缺失/额外/字段不符均为空"
boundary=$(echo "$v" | jq -r .boundary_lsn)
case "$boundary" in 0/*) ok "复制流边界已固定: $boundary";; *) bad "边界 LSN 异常: $boundary";; esac

# 结论不可变：核验完成后继续改目录，结论保持一致
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":7001,\"service_name\":\"after\",\"version\":\"1\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 3
check "$(api "/api/verifications/$vid" | jq -r .status)" "CONSISTENT" "后续提交不改变本次结论（不可变）"
check "$(api "/api/verifications/$vid" | jq -r .source_rows)" "3" "结论里的快照仍为发起时刻的 3 行"

# 已有/曾有核验不阻止发起新核验（只有 PENDING 冲突）
vid2=$(post_verify | jq -r .id)
case "$vid2" in v-*) ok "上一核验完成后可再次发起";; *) bad "再次发起失败: $vid2";; esac
wait_status "$vid2" CONSISTENT 15 && ok "第二次核验一致" || bad "第二次核验超时"
check "$(api "/api/verifications/$vid2" | jq -r .source_rows)" "4" "新核验固定的是当前目录（4 行）"

# ------------------------------------------------------------- 2. 并发修改
echo "=== 2. 核验等待期间并发修改目录（确定性待捕获窗口）==="
# 用测试钩子 TEST_PUMP_PAUSE_MS 让复制泵在连接后暂停 8s，
# 期间源库写入确定性地形成积压，发起的核验稳定处于"待捕获"。
stop_app
for n in 7101 7102 7103; do
  psqlq "insert into artifacts values ($n,'svc$n','2','$SHA64')"
done
check "$(psqlq 'select count(*) from artifacts')" "7" "核验前源目录已有 7 行"
start_app 15000 || bad "应用重启失败"
# 等应用 HTTP 就绪（此时泵仍在暂停窗口内，积压尚未被消费）
sleep 1
id3=$(post_verify | jq -r .id)
check "$(api "/api/verifications/$id3" | jq -r .status)" "PENDING" \
  "复制积压期间发起核验确定性显示待捕获: $id3"
# PENDING 期间重复发起 -> 409（此时窗口确定存在）
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/verifications")
check "$code" "409" "待捕获期间重复发起被 409 拒绝"
# 待捕获期间普通 SQL 照常提交，不被核验阻塞
t0=$(date +%s%N)
psqlq "insert into artifacts values (7201,'concurrent','3','$SHA64')"
t1=$(date +%s%N)
ms=$(( (t1-t0)/1000000 ))
check "$(psqlq 'select count(*) from artifacts where artifact_no=7201')" "1" "等待期间普通 SQL 可继续改目录"
[ "$ms" -lt 3000 ] && ok "核验不持有长时间源表锁（写入耗时 ${ms}ms）" \
  || bad "源表写入耗时 ${ms}ms，疑似被核验阻塞"
wait_phase STREAMING 20 || true
wait_status "$id3" CONSISTENT 20 && ok "积压与并发写入全部捕获后核验一致" || bad "并发场景核验未一致"
check "$(api "/api/verifications/$id3" | jq -r .source_rows)" "7" \
  "结论按发起边界固定为 7 行（不含等待期间的 7201）"
check "$(api "/api/verifications/$id3" | jq -r .captured_rows)" "7" "本地同边界重建 7 行"

# ------------------------------------------------------------- 3. 应用重启
echo "=== 3. 应用重启 -> 未确认，可重新发起；既有结论保留 ==="
stop_app
for n in 7301 7302 7303 7304 7305; do
  psqlq "insert into artifacts values ($n,'backlog$n','4','$SHA64')"
done
start_app 15000 || bad "应用重启失败"
sleep 1
vid4=$(post_verify | jq -r .id)
check "$(api "/api/verifications/$vid4" | jq -r .status)" "PENDING" "重启前已发起待捕获核验 $vid4"
stop_app
start_app || bad "二次重启失败"
check "$(api "/api/verifications/$vid4" | jq -r .status)" "UNCONFIRMED" "重启后中断核验显示未确认"
check "$(api "/api/verifications/$vid"  | jq -r .status)" "CONSISTENT" "已形成的一致结论不受重启影响"
vid5=$(post_verify | jq -r .id)
case "$vid5" in v-*) ok "未确认后可重新发起: $vid5";; *) bad "重新发起失败（PENDING 未清理？）: $vid5";; esac
wait_status "$vid5" CONSISTENT 20 && ok "重新发起的核验完成且一致" || bad "重新发起核验超时"

# ------------------------------------------------------------- 4. 源断连
echo "=== 4. 源库断连 -> 未确认，重连后可重新发起 ==="
stop_app
for n in 7401 7402 7403 7404 7405 7406; do
  psqlq "insert into artifacts values ($n,'disc$n','5','$SHA64')"
done
# 连接建立前暂停 15s：此时没有 walsender，源端数据不可能进入客户端缓冲；
# 应用 HTTP 已就绪（phase 仍为持久化的 STREAMING），可正常发起核验。
start_app 0 15000 || bad "应用重启失败"
sleep 1
vid6=$(post_verify | jq -r .id)
check "$(api "/api/verifications/$vid6" | jq -r .status)" "PENDING" "断连前已发起待捕获核验 $vid6"
# 此时复制连接尚未建立，停源库不会有任何已缓冲数据被本地继续消费；
# 暂停结束后连接失败，待捕获核验立即转未确认。
pg_ctl_q stop -m immediate >/dev/null
for _ in $(seq 1 50); do
  [ "$(api "/api/verifications/$vid6" | jq -r .status)" = "UNCONFIRMED" ] && break
  sleep 0.5
done
check "$(api "/api/verifications/$vid6" | jq -r .status)" "UNCONFIRMED" "源断连后核验显示未确认"
# immediate 崩溃后重启，PostgreSQL 自动崩溃恢复
pg_ctl_q start -w >/dev/null
wait_phase STREAMING 25 && ok "源库恢复后应用自动重连 STREAMING" || bad "重连超时"
vid7=$(post_verify | jq -r .id)
case "$vid7" in v-*) ok "重连后可重新发起: $vid7";; *) bad "重连后发起失败: $vid7";; esac
wait_status "$vid7" CONSISTENT 20 && ok "重连后新核验一致" || bad "重连后核验超时"

# ------------------------------------------------------------- 5. 列表与 404
echo "=== 5. 列表接口与不存在核验 404 ==="
n_all=$(api /api/verifications | jq '.verifications|length')
[ "$n_all" -ge 6 ] && ok "核验记录只增保留，共 $n_all 条" || bad "核验记录数异常: $n_all"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/verifications/v-nope")
check "$code" "404" "不存在的核验返回 404"
# 未确认记录带原因
reason=$(api "/api/verifications/$vid6" | jq -r .unconfirmed_reason)
[ -n "$reason" ] && [ "$reason" != "null" ] && ok "未确认记录保留中断原因" || bad "缺少中断原因"

# ------------------------------------------------------------- 6. 不改写源与历史
echo "=== 6. 核验只读：源目录与历史链不被改写 ==="
src_count=$(psqlq 'select count(*) from artifacts')
hist_before=$(api /api/progress | jq .history_rows)
post_verify >/dev/null
sleep 2
check "$(psqlq 'select count(*) from artifacts')" "$src_count" "核验前后源目录行数不变"
check "$(api /api/progress | jq .history_rows)" "$hist_before" "核验不向 artifact_history 写变更"

echo
echo "======== 核验自测结果: $PASS passed / $FAIL failed ========"
[ "$FAIL" -eq 0 ]
