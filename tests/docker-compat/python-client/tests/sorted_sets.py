"""Category 6: Sorted Set Operations tests."""

from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int, assert_equal_float, assert_list_equal,
)


def test_sorted_sets(r, prefix):
    results = []
    cat = "sorted_sets"

    def t_zadd():
        k = key(prefix, "zset:add")
        try:
            n = r.zadd(k, {"a": 1})
            assert_equal_int(1, n)
            n = r.zadd(k, {"b": 2, "c": 3})
            assert_equal_int(2, n)
        finally:
            r.delete(k)
    results.append(run_test("ZADD_single_and_multiple", cat, t_zadd))

    def t_zadd_flags():
        k = key(prefix, "zset:flags")
        try:
            r.zadd(k, {"a": 5})
            # NX: only add new
            n = r.zadd(k, {"a": 10}, nx=True)
            assert_equal_int(0, n)
            assert_equal_float(5, r.zscore(k, "a"), 0.001)
            # XX: only update existing
            n = r.zadd(k, {"a": 10}, xx=True)
            assert_equal_int(0, n)  # 0 new members
            assert_equal_float(10, r.zscore(k, "a"), 0.001)
            # GT: only update if new > current
            r.zadd(k, {"a": 5}, gt=True)
            assert_equal_float(10, r.zscore(k, "a"), 0.001)
            # LT: only update if new < current
            r.zadd(k, {"a": 3}, lt=True)
            assert_equal_float(3, r.zscore(k, "a"), 0.001)
        finally:
            r.delete(k)
    results.append(run_test("ZADD_NX_XX_GT_LT", cat, t_zadd_flags))

    def t_zscore_zmscore():
        k = key(prefix, "zset:score")
        try:
            r.zadd(k, {"a": 1.5, "b": 2.5})
            assert_equal_float(1.5, r.zscore(k, "a"), 0.001)
            scores = r.zmscore(k, ["a", "b", "nonexist"])
            assert_equal_float(1.5, scores[0], 0.001)
            assert_equal_float(2.5, scores[1], 0.001)
        finally:
            r.delete(k)
    results.append(run_test("ZSCORE_ZMSCORE", cat, t_zscore_zmscore))

    def t_zrange():
        k = key(prefix, "zset:range")
        try:
            r.zadd(k, {"a": 1, "b": 2, "c": 3})
            vals = r.zrange(k, 0, -1)
            assert_list_equal(["a", "b", "c"], vals)
        finally:
            r.delete(k)
    results.append(run_test("ZRANGE_basic", cat, t_zrange))

    def t_zrangebyscore():
        k = key(prefix, "zset:rbyscore")
        try:
            r.zadd(k, {"a": 1, "b": 2, "c": 3, "d": 4})
            vals = r.zrangebyscore(k, 2, 3)
            assert_list_equal(["b", "c"], vals)
        finally:
            r.delete(k)
    results.append(run_test("ZRANGEBYSCORE", cat, t_zrangebyscore))

    def t_zrevrangebyscore():
        k = key(prefix, "zset:rrbyscore")
        try:
            r.zadd(k, {"a": 1, "b": 2, "c": 3})
            vals = r.zrevrangebyscore(k, 3, 1)
            assert_list_equal(["c", "b", "a"], vals)
        finally:
            r.delete(k)
    results.append(run_test("ZREVRANGEBYSCORE", cat, t_zrevrangebyscore))

    def t_zrank():
        k = key(prefix, "zset:rank")
        try:
            r.zadd(k, {"a": 1, "b": 2, "c": 3})
            assert_equal_int(1, r.zrank(k, "b"))
            assert_equal_int(1, r.zrevrank(k, "b"))
        finally:
            r.delete(k)
    results.append(run_test("ZRANK_ZREVRANK", cat, t_zrank))

    def t_zrem():
        k = key(prefix, "zset:rem")
        try:
            r.zadd(k, {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
            n = r.zrem(k, "a")
            assert_equal_int(1, n)
            n = r.zremrangebyscore(k, 2, 3)
            assert_equal_int(2, n)
            n = r.zremrangebyrank(k, 0, 0)
            assert_equal_int(1, n)
        finally:
            r.delete(k)
    results.append(run_test("ZREM_ZREMRANGEBYSCORE_ZREMRANGEBYRANK", cat, t_zrem))

    def t_zcard_zcount():
        k = key(prefix, "zset:card")
        try:
            r.zadd(k, {"a": 1, "b": 2, "c": 3})
            assert_equal_int(3, r.zcard(k))
            assert_equal_int(2, r.zcount(k, 1, 2))
        finally:
            r.delete(k)
    results.append(run_test("ZCARD_ZCOUNT", cat, t_zcard_zcount))

    def t_zincrby():
        k = key(prefix, "zset:incr")
        try:
            r.zadd(k, {"a": 10})
            new_score = r.zincrby(k, 5, "a")
            assert_equal_float(15, new_score, 0.001)
        finally:
            r.delete(k)
    results.append(run_test("ZINCRBY", cat, t_zincrby))

    def t_zunion_zinter():
        k1 = key(prefix, "zset:u1")
        k2 = key(prefix, "zset:u2")
        d1 = key(prefix, "zset:union")
        d2 = key(prefix, "zset:inter")
        try:
            r.zadd(k1, {"a": 1, "b": 2})
            r.zadd(k2, {"b": 3, "c": 4})
            n = r.zunionstore(d1, [k1, k2])
            assert_equal_int(3, n)
            score = r.zscore(d1, "b")
            assert_equal_float(5, score, 0.001)
            n = r.zinterstore(d2, [k1, k2])
            assert_equal_int(1, n)
        finally:
            r.delete(k1, k2, d1, d2)
    results.append(run_test("ZUNIONSTORE_ZINTERSTORE", cat, t_zunion_zinter))

    def t_zrandmember():
        k = key(prefix, "zset:rand")
        try:
            r.zadd(k, {"a": 1, "b": 2, "c": 3})
            members = r.zrandmember(k, 1)
            assert_true(len(members) == 1, "should return 1 member")
            assert_true(members[0] in ("a", "b", "c"), "member should be a, b, or c")
        finally:
            r.delete(k)
    results.append(run_test("ZRANDMEMBER", cat, t_zrandmember))

    def t_zscan():
        k = key(prefix, "zset:scan")
        try:
            for i in range(20):
                r.zadd(k, {f"m{i}": float(i)})
            all_members = []
            cursor = 0
            while True:
                cursor, data = r.zscan(k, cursor, match="*", count=10)
                all_members.extend(m for m, _ in data)
                if cursor == 0:
                    break
            assert_true(len(all_members) == 20, f"ZSCAN should return 20 members, got {len(all_members)}")
        finally:
            r.delete(k)
    results.append(run_test("ZSCAN", cat, t_zscan))

    def t_zpopmin_zpopmax():
        k = key(prefix, "zset:pop")
        try:
            r.zadd(k, {"a": 1, "b": 2, "c": 3})
            min_result = r.zpopmin(k, 1)
            assert_equal("a", min_result[0][0])
            max_result = r.zpopmax(k, 1)
            assert_equal("c", max_result[0][0])
        finally:
            r.delete(k)
    results.append(run_test("ZPOPMIN_ZPOPMAX", cat, t_zpopmin_zpopmax))

    return results


register("sorted_sets", test_sorted_sets)
