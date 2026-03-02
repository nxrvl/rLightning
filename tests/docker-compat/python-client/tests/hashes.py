"""Category 3: Hash Operations tests."""

from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int, assert_equal_float, assert_list_equal, assert_list_equal_unordered,
)


def test_hashes(r, prefix):
    results = []
    cat = "hashes"

    def t_hset_hget():
        k = key(prefix, "hash:single")
        try:
            r.hset(k, "field1", "value1")
            assert_equal("value1", r.hget(k, "field1"))
        finally:
            r.delete(k)
    results.append(run_test("HSET_HGET_single", cat, t_hset_hget))

    def t_hset_multi():
        k = key(prefix, "hash:multi")
        try:
            r.hset(k, mapping={"f1": "v1", "f2": "v2", "f3": "v3"})
            assert_equal("v1", r.hget(k, "f1"))
            assert_equal("v2", r.hget(k, "f2"))
            assert_equal("v3", r.hget(k, "f3"))
        finally:
            r.delete(k)
    results.append(run_test("HSET_multiple_fields", cat, t_hset_multi))

    def t_hmset_hmget():
        k = key(prefix, "hash:hmset")
        try:
            r.hset(k, mapping={"a": "1", "b": "2", "c": "3"})
            vals = r.hmget(k, "a", "b", "c", "missing")
            assert_equal(4, len(vals))
            assert_equal("1", vals[0])
            assert_equal("2", vals[1])
            assert_equal("3", vals[2])
            assert_true(vals[3] is None, "missing field should be None")
        finally:
            r.delete(k)
    results.append(run_test("HMSET_HMGET", cat, t_hmset_hmget))

    def t_hgetall():
        k = key(prefix, "hash:getall")
        try:
            r.hset(k, mapping={"x": "10", "y": "20"})
            m = r.hgetall(k)
            assert_equal(2, len(m))
            assert_equal("10", m["x"])
            assert_equal("20", m["y"])
        finally:
            r.delete(k)
    results.append(run_test("HGETALL", cat, t_hgetall))

    def t_hdel_hexists():
        k = key(prefix, "hash:del")
        try:
            r.hset(k, "field", "val")
            assert_true(r.hexists(k, "field"), "field should exist")
            n = r.hdel(k, "field")
            assert_equal_int(1, n)
            assert_true(not r.hexists(k, "field"), "field should not exist after HDEL")
        finally:
            r.delete(k)
    results.append(run_test("HDEL_HEXISTS", cat, t_hdel_hexists))

    def t_hlen_hkeys_hvals():
        k = key(prefix, "hash:lkv")
        try:
            r.hset(k, mapping={"a": "1", "b": "2", "c": "3"})
            assert_equal_int(3, r.hlen(k))
            keys = sorted(r.hkeys(k))
            assert_list_equal(["a", "b", "c"], keys)
            vals = sorted(r.hvals(k))
            assert_list_equal(["1", "2", "3"], vals)
        finally:
            r.delete(k)
    results.append(run_test("HLEN_HKEYS_HVALS", cat, t_hlen_hkeys_hvals))

    def t_hincrby():
        k = key(prefix, "hash:incr")
        try:
            r.hset(k, "counter", "10")
            val = r.hincrby(k, "counter", 5)
            assert_equal_int(15, val)
            r.hset(k, "float_counter", "10.5")
            fval = r.hincrbyfloat(k, "float_counter", 0.1)
            assert_equal_float(10.6, fval, 0.001)
        finally:
            r.delete(k)
    results.append(run_test("HINCRBY_HINCRBYFLOAT", cat, t_hincrby))

    def t_hsetnx():
        k = key(prefix, "hash:setnx")
        try:
            ok = r.hsetnx(k, "field", "first")
            assert_true(ok == 1 or ok is True, "HSETNX should succeed on new field")
            ok = r.hsetnx(k, "field", "second")
            assert_true(ok == 0 or ok is False, "HSETNX should fail on existing field")
            assert_equal("first", r.hget(k, "field"))
        finally:
            r.delete(k)
    results.append(run_test("HSETNX", cat, t_hsetnx))

    def t_hrandfield():
        k = key(prefix, "hash:rand")
        try:
            r.hset(k, mapping={"a": "1", "b": "2", "c": "3"})
            fields = r.hrandfield(k, 1)
            assert_true(len(fields) == 1, "should return 1 field")
            assert_true(fields[0] in ("a", "b", "c"), "returned field should be one of a, b, c")
        finally:
            r.delete(k)
    results.append(run_test("HRANDFIELD", cat, t_hrandfield))

    def t_hscan():
        k = key(prefix, "hash:scan")
        try:
            for i in range(20):
                r.hset(k, f"field{i}", f"val{i}")
            all_keys = []
            cursor = 0
            while True:
                cursor, data = r.hscan(k, cursor, match="*", count=10)
                all_keys.extend(data.keys())
                if cursor == 0:
                    break
            assert_true(len(all_keys) == 20, f"HSCAN should return all 20 fields, got {len(all_keys)}")
        finally:
            r.delete(k)
    results.append(run_test("HSCAN", cat, t_hscan))

    return results


register("hashes", test_hashes)
