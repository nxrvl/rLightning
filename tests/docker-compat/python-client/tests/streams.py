"""Category 12: Streams tests."""

from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int,
)


def test_streams(r, prefix):
    results = []
    cat = "streams"

    def t_xadd_auto():
        k = key(prefix, "stream:auto")
        try:
            entry_id = r.xadd(k, {"field": "value1"})
            assert_true("-" in entry_id, f"auto ID should contain '-': {entry_id}")
        finally:
            r.delete(k)
    results.append(run_test("XADD_auto_ID", cat, t_xadd_auto))

    def t_xadd_explicit():
        k = key(prefix, "stream:explicit")
        try:
            entry_id = r.xadd(k, {"field": "value"}, id="1-1")
            assert_equal("1-1", entry_id)
        finally:
            r.delete(k)
    results.append(run_test("XADD_explicit_ID", cat, t_xadd_explicit))

    def t_xlen():
        k = key(prefix, "stream:len")
        try:
            for i in range(5):
                r.xadd(k, {"i": str(i)})
            assert_equal_int(5, r.xlen(k))
        finally:
            r.delete(k)
    results.append(run_test("XLEN", cat, t_xlen))

    def t_xrange_xrevrange():
        k = key(prefix, "stream:range")
        try:
            r.xadd(k, {"a": "1"}, id="1-1")
            r.xadd(k, {"b": "2"}, id="2-1")
            r.xadd(k, {"c": "3"}, id="3-1")

            msgs = r.xrange(k, "-", "+")
            assert_equal(3, len(msgs))
            assert_equal("1-1", msgs[0][0])

            rmsgs = r.xrevrange(k, "+", "-")
            assert_equal(3, len(rmsgs))
            assert_equal("3-1", rmsgs[0][0])
        finally:
            r.delete(k)
    results.append(run_test("XRANGE_XREVRANGE", cat, t_xrange_xrevrange))

    def t_xread():
        k = key(prefix, "stream:read")
        try:
            r.xadd(k, {"x": "1"}, id="1-1")
            r.xadd(k, {"x": "2"}, id="2-1")
            res = r.xread({k: "0"}, count=10)
            assert_equal(1, len(res))
            # res is list of [stream_name, messages]
            assert_equal(2, len(res[0][1]))
        finally:
            r.delete(k)
    results.append(run_test("XREAD", cat, t_xread))

    def t_xinfo():
        k = key(prefix, "stream:info")
        try:
            r.xadd(k, {"f": "v"})
            info = r.xinfo_stream(k)
            assert_equal_int(1, info["length"])
        finally:
            r.delete(k)
    results.append(run_test("XINFO_STREAM", cat, t_xinfo))

    def t_xtrim():
        k = key(prefix, "stream:trim")
        try:
            for i in range(10):
                r.xadd(k, {"i": str(i)})
            n = r.xtrim(k, maxlen=5, approximate=False)
            assert_true(n >= 0, "XTRIM should return non-negative count")
            length = r.xlen(k)
            assert_true(length <= 5, "stream should have <= 5 entries after trim")
        finally:
            r.delete(k)
    results.append(run_test("XTRIM_MAXLEN", cat, t_xtrim))

    def t_xdel():
        k = key(prefix, "stream:del")
        try:
            entry_id = r.xadd(k, {"f": "v"})
            r.xadd(k, {"f2": "v2"})
            n = r.xdel(k, entry_id)
            assert_equal_int(1, n)
            assert_equal_int(1, r.xlen(k))
        finally:
            r.delete(k)
    results.append(run_test("XDEL", cat, t_xdel))

    return results


register("streams", test_streams)
