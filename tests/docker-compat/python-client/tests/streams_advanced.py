"""Streams Advanced tests - consumer groups, pending, claiming, trimming."""

import time
from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int,
)


def test_streams_advanced(r, prefix):
    results = []
    cat = "streams_advanced"

    # XGROUP_CREATE: Create consumer group, verify with XINFO GROUPS
    def t_xgroup_create():
        k = key(prefix, "sa:xgroup")
        try:
            r.xadd(k, {"f": "v"}, id="1-1")
            r.xgroup_create(k, "mygroup", "0")
            groups = r.xinfo_groups(k)
            assert_equal_int(1, len(groups))
            assert_equal("mygroup", groups[0]["name"])
        finally:
            r.delete(k)
    results.append(run_test("XGROUP_CREATE", cat, t_xgroup_create))

    # XREADGROUP_basic: Create group, XREADGROUP, verify message delivered
    def t_xreadgroup_basic():
        k = key(prefix, "sa:xreadgroup")
        try:
            r.xadd(k, {"msg": "hello"}, id="1-1")
            r.xgroup_create(k, "grp1", "0")
            res = r.xreadgroup("grp1", "consumer1", {k: ">"}, count=10)
            assert_equal_int(1, len(res))
            # res = [[stream_name, [(id, {field: value}), ...]]]
            messages = res[0][1]
            assert_equal_int(1, len(messages))
            assert_equal("hello", messages[0][1]["msg"])
        finally:
            r.delete(k)
    results.append(run_test("XREADGROUP_basic", cat, t_xreadgroup_basic))

    # XREADGROUP_pending: XREADGROUP without ACK, verify pending
    def t_xreadgroup_pending():
        k = key(prefix, "sa:xreadpend")
        try:
            r.xadd(k, {"f": "v"}, id="1-1")
            r.xadd(k, {"f": "v2"}, id="2-1")
            r.xgroup_create(k, "grp1", "0")
            r.xreadgroup("grp1", "c1", {k: ">"}, count=10)
            pending = r.xpending(k, "grp1")
            assert_equal_int(2, pending["pending"])
        finally:
            r.delete(k)
    results.append(run_test("XREADGROUP_pending", cat, t_xreadgroup_pending))

    # XACK_basic: Read message, XACK, verify removed from pending
    def t_xack_basic():
        k = key(prefix, "sa:xack")
        try:
            entry_id = r.xadd(k, {"f": "v"})
            r.xgroup_create(k, "grp1", "0")
            r.xreadgroup("grp1", "c1", {k: ">"}, count=10)

            # Verify pending before ACK
            pending = r.xpending(k, "grp1")
            assert_equal_int(1, pending["pending"])

            # ACK
            n = r.xack(k, "grp1", entry_id)
            assert_equal_int(1, n)

            # Verify pending after ACK
            pending = r.xpending(k, "grp1")
            assert_equal_int(0, pending["pending"])
        finally:
            r.delete(k)
    results.append(run_test("XACK_basic", cat, t_xack_basic))

    # XCLAIM_basic: Read on consumer1, XCLAIM to consumer2
    def t_xclaim_basic():
        k = key(prefix, "sa:xclaim")
        try:
            entry_id = r.xadd(k, {"f": "v"})
            r.xgroup_create(k, "grp1", "0")
            r.xreadgroup("grp1", "consumer1", {k: ">"}, count=10)

            # Claim to consumer2 with 0 min-idle-time
            msgs = r.xclaim(k, "grp1", "consumer2", 0, [entry_id])
            assert_equal_int(1, len(msgs))

            # Verify consumer2 has it in pending
            detail = r.xpending_range(k, "grp1", "-", "+", 10)
            assert_equal_int(1, len(detail))
            assert_equal("consumer2", detail[0]["consumer"])
        finally:
            r.delete(k)
    results.append(run_test("XCLAIM_basic", cat, t_xclaim_basic))

    # XAUTOCLAIM_basic: Read message, wait, XAUTOCLAIM with min-idle
    def t_xautoclaim_basic():
        k = key(prefix, "sa:xautoclaim")
        try:
            r.xadd(k, {"f": "v"}, id="1-1")
            r.xgroup_create(k, "grp1", "0")
            r.xreadgroup("grp1", "consumer1", {k: ">"}, count=10)

            # Small wait to ensure idle time > 0
            time.sleep(0.05)

            # XAUTOCLAIM to consumer2 with very low min-idle
            result = r.xautoclaim(k, "grp1", "consumer2", 1, "0-0", count=10)
            # result = (next_start_id, [(id, {fields}), ...])
            # or (next_start_id, [(id, {fields}), ...], [deleted_ids])
            claimed = result[1]
            assert_true(len(claimed) >= 1,
                        f"expected at least 1 autoclaimed message, got {len(claimed)}")
        finally:
            r.delete(k)
    results.append(run_test("XAUTOCLAIM_basic", cat, t_xautoclaim_basic))

    # XPENDING_summary: Check pending summary
    def t_xpending_summary():
        k = key(prefix, "sa:xpendsumm")
        try:
            r.xadd(k, {"f": "v1"}, id="1-1")
            r.xadd(k, {"f": "v2"}, id="2-1")
            r.xadd(k, {"f": "v3"}, id="3-1")
            r.xgroup_create(k, "grp1", "0")

            r.xreadgroup("grp1", "c1", {k: ">"}, count=2)
            r.xreadgroup("grp1", "c2", {k: ">"}, count=1)

            pending = r.xpending(k, "grp1")
            assert_equal_int(3, pending["pending"])
            assert_equal("1-1", pending["min"])
            assert_equal("3-1", pending["max"])
            # Should have 2 consumers
            assert_equal_int(2, len(pending["consumers"]))
        finally:
            r.delete(k)
    results.append(run_test("XPENDING_summary", cat, t_xpending_summary))

    # XPENDING_detail: Check per-consumer pending detail
    def t_xpending_detail():
        k = key(prefix, "sa:xpenddet")
        try:
            r.xadd(k, {"f": "v1"}, id="1-1")
            r.xadd(k, {"f": "v2"}, id="2-1")
            r.xgroup_create(k, "grp1", "0")
            r.xreadgroup("grp1", "c1", {k: ">"}, count=10)

            detail = r.xpending_range(k, "grp1", "-", "+", 10)
            assert_equal_int(2, len(detail))
            assert_equal("c1", detail[0]["consumer"])
            assert_equal("1-1", detail[0]["message_id"])
        finally:
            r.delete(k)
    results.append(run_test("XPENDING_detail", cat, t_xpending_detail))

    # XINFO_STREAM: Verify full stream info including length, groups, first/last entry
    def t_xinfo_stream():
        k = key(prefix, "sa:xinfost")
        try:
            r.xadd(k, {"f": "v1"}, id="1-1")
            r.xadd(k, {"f": "v2"}, id="2-1")
            r.xadd(k, {"f": "v3"}, id="3-1")
            r.xgroup_create(k, "grp1", "0")

            info = r.xinfo_stream(k)
            assert_equal_int(3, info["length"])
            assert_equal_int(1, info["groups"])
            assert_equal("1-1", info["first-entry"][0])
            assert_equal("3-1", info["last-entry"][0])
        finally:
            r.delete(k)
    results.append(run_test("XINFO_STREAM", cat, t_xinfo_stream))

    # XINFO_CONSUMERS: Verify consumer info
    def t_xinfo_consumers():
        k = key(prefix, "sa:xinfocons")
        try:
            r.xadd(k, {"f": "v"}, id="1-1")
            r.xadd(k, {"f": "v2"}, id="2-1")
            r.xgroup_create(k, "grp1", "0")
            r.xreadgroup("grp1", "consumer1", {k: ">"}, count=10)

            consumers = r.xinfo_consumers(k, "grp1")
            assert_equal_int(1, len(consumers))
            assert_equal("consumer1", consumers[0]["name"])
            assert_equal_int(2, consumers[0]["pending"])
        finally:
            r.delete(k)
    results.append(run_test("XINFO_CONSUMERS", cat, t_xinfo_consumers))

    # XTRIM_MAXLEN: Add entries, XTRIM MAXLEN 5, verify only 5 remain
    def t_xtrim_maxlen():
        k = key(prefix, "sa:xtrimmax")
        try:
            for i in range(10):
                r.xadd(k, {"i": str(i)})
            r.xtrim(k, maxlen=5, approximate=False)
            assert_equal_int(5, r.xlen(k))
        finally:
            r.delete(k)
    results.append(run_test("XTRIM_MAXLEN", cat, t_xtrim_maxlen))

    # XTRIM_MINID: Add entries, XTRIM MINID, verify entries before ID removed
    def t_xtrim_minid():
        k = key(prefix, "sa:xtrimmin")
        try:
            r.xadd(k, {"f": "v1"}, id="1-1")
            r.xadd(k, {"f": "v2"}, id="2-1")
            r.xadd(k, {"f": "v3"}, id="3-1")
            r.xadd(k, {"f": "v4"}, id="4-1")
            r.xadd(k, {"f": "v5"}, id="5-1")

            r.xtrim(k, minid="3-1")
            assert_equal_int(3, r.xlen(k))
            # Verify first entry is now 3-1
            msgs = r.xrange(k, "-", "+")
            assert_equal("3-1", msgs[0][0])
        finally:
            r.delete(k)
    results.append(run_test("XTRIM_MINID", cat, t_xtrim_minid))

    return results


register("streams_advanced", test_streams_advanced)
