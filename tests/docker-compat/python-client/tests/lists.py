"""Category 4: List Operations tests."""

from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int, assert_list_equal,
)


def test_lists(r, prefix):
    results = []
    cat = "lists"

    def t_push():
        k = key(prefix, "list:push")
        try:
            r.lpush(k, "a")
            r.rpush(k, "b")
            r.lpush(k, "c", "d")   # d, c, a, b
            r.rpush(k, "e", "f")   # d, c, a, b, e, f
            vals = r.lrange(k, 0, -1)
            assert_list_equal(["d", "c", "a", "b", "e", "f"], vals)
        finally:
            r.delete(k)
    results.append(run_test("LPUSH_RPUSH_single_and_multiple", cat, t_push))

    def t_pop():
        k = key(prefix, "list:pop")
        try:
            r.rpush(k, "a", "b", "c")
            left = r.lpop(k)
            assert_equal("a", left)
            right = r.rpop(k)
            assert_equal("c", right)
        finally:
            r.delete(k)
    results.append(run_test("LPOP_RPOP", cat, t_pop))

    def t_lrange():
        k = key(prefix, "list:range")
        try:
            r.rpush(k, "a", "b", "c", "d", "e")
            vals = r.lrange(k, 1, 3)
            assert_list_equal(["b", "c", "d"], vals)
        finally:
            r.delete(k)
    results.append(run_test("LRANGE", cat, t_lrange))

    def t_llen():
        k = key(prefix, "list:len")
        try:
            r.rpush(k, "a", "b", "c")
            assert_equal_int(3, r.llen(k))
        finally:
            r.delete(k)
    results.append(run_test("LLEN", cat, t_llen))

    def t_lindex():
        k = key(prefix, "list:idx")
        try:
            r.rpush(k, "a", "b", "c")
            assert_equal("b", r.lindex(k, 1))
            assert_equal("c", r.lindex(k, -1))
        finally:
            r.delete(k)
    results.append(run_test("LINDEX", cat, t_lindex))

    def t_lset():
        k = key(prefix, "list:set")
        try:
            r.rpush(k, "a", "b", "c")
            r.lset(k, 1, "B")
            assert_equal("B", r.lindex(k, 1))
        finally:
            r.delete(k)
    results.append(run_test("LSET", cat, t_lset))

    def t_linsert():
        k = key(prefix, "list:ins")
        try:
            r.rpush(k, "a", "c")
            r.linsert(k, "BEFORE", "c", "b")  # a, b, c
            r.linsert(k, "AFTER", "c", "d")   # a, b, c, d
            vals = r.lrange(k, 0, -1)
            assert_list_equal(["a", "b", "c", "d"], vals)
        finally:
            r.delete(k)
    results.append(run_test("LINSERT_BEFORE_AFTER", cat, t_linsert))

    def t_lrem():
        k = key(prefix, "list:rem")
        try:
            r.rpush(k, "a", "b", "a", "c", "a")
            n = r.lrem(k, 2, "a")
            assert_equal_int(2, n)
            vals = r.lrange(k, 0, -1)
            assert_list_equal(["b", "c", "a"], vals)
        finally:
            r.delete(k)
    results.append(run_test("LREM", cat, t_lrem))

    def t_ltrim():
        k = key(prefix, "list:trim")
        try:
            r.rpush(k, "a", "b", "c", "d", "e")
            r.ltrim(k, 1, 3)
            vals = r.lrange(k, 0, -1)
            assert_list_equal(["b", "c", "d"], vals)
        finally:
            r.delete(k)
    results.append(run_test("LTRIM", cat, t_ltrim))

    def t_lpos():
        k = key(prefix, "list:pos")
        try:
            r.rpush(k, "a", "b", "c", "b", "d")
            pos = r.lpos(k, "b")
            assert_equal_int(1, pos)
        finally:
            r.delete(k)
    results.append(run_test("LPOS", cat, t_lpos))

    def t_lmpop():
        k = key(prefix, "list:mpop")
        try:
            r.rpush(k, "a", "b", "c", "d")
            result = r.lmpop(1, k, direction="LEFT", count=2)
            assert_true(result is not None, "LMPOP should return a result")
            popped_key, vals = result
            assert_equal(k, popped_key)
            assert_list_equal(["a", "b"], vals)
        finally:
            r.delete(k)
    results.append(run_test("LMPOP", cat, t_lmpop))

    return results


register("lists", test_lists)
