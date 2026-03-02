"""Category 5: Set Operations tests."""

from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int, assert_list_equal_unordered,
)


def test_sets(r, prefix):
    results = []
    cat = "sets"

    def t_sadd():
        k = key(prefix, "set:add")
        try:
            n = r.sadd(k, "a")
            assert_equal_int(1, n)
            n = r.sadd(k, "b", "c", "d")
            assert_equal_int(3, n)
            n = r.sadd(k, "a")
            assert_equal_int(0, n)
        finally:
            r.delete(k)
    results.append(run_test("SADD_single_and_multiple", cat, t_sadd))

    def t_smembers():
        k = key(prefix, "set:members")
        try:
            r.sadd(k, "a", "b", "c")
            members = list(r.smembers(k))
            assert_list_equal_unordered(["a", "b", "c"], members)
            assert_true(r.sismember(k, "a"), "a should be a member")
            assert_true(not r.sismember(k, "z"), "z should not be a member")
            mis = r.smismember(k, "a", "z", "c")
            assert_true(mis[0] == 1 or mis[0] is True, "a should be member")
            assert_true(mis[1] == 0 or mis[1] is False, "z should not be member")
            assert_true(mis[2] == 1 or mis[2] is True, "c should be member")
        finally:
            r.delete(k)
    results.append(run_test("SMEMBERS_SISMEMBER_SMISMEMBER", cat, t_smembers))

    def t_srem_spop():
        k = key(prefix, "set:rem")
        try:
            r.sadd(k, "a", "b", "c", "d", "e")
            n = r.srem(k, "a", "b")
            assert_equal_int(2, n)
            popped = r.spop(k)
            assert_true(popped in ("c", "d", "e"), "popped should be c, d, or e")
            rand = r.srandmember(k)
            assert_true(rand is not None and len(rand) > 0, "SRANDMEMBER should return a member")
        finally:
            r.delete(k)
    results.append(run_test("SREM_SPOP_SRANDMEMBER", cat, t_srem_spop))

    def t_scard():
        k = key(prefix, "set:card")
        try:
            r.sadd(k, "a", "b", "c")
            assert_equal_int(3, r.scard(k))
        finally:
            r.delete(k)
    results.append(run_test("SCARD", cat, t_scard))

    def t_sunion_sinter_sdiff():
        k1 = key(prefix, "set:op1")
        k2 = key(prefix, "set:op2")
        try:
            r.sadd(k1, "a", "b", "c")
            r.sadd(k2, "b", "c", "d")
            union = list(r.sunion(k1, k2))
            assert_list_equal_unordered(["a", "b", "c", "d"], union)
            inter = list(r.sinter(k1, k2))
            assert_list_equal_unordered(["b", "c"], inter)
            diff = list(r.sdiff(k1, k2))
            assert_list_equal_unordered(["a"], diff)
        finally:
            r.delete(k1, k2)
    results.append(run_test("SUNION_SINTER_SDIFF", cat, t_sunion_sinter_sdiff))

    def t_store_ops():
        k1 = key(prefix, "set:store1")
        k2 = key(prefix, "set:store2")
        d1 = key(prefix, "set:union_dest")
        d2 = key(prefix, "set:inter_dest")
        d3 = key(prefix, "set:diff_dest")
        try:
            r.sadd(k1, "a", "b", "c")
            r.sadd(k2, "b", "c", "d")
            n = r.sunionstore(d1, k1, k2)
            assert_equal_int(4, n)
            n = r.sinterstore(d2, k1, k2)
            assert_equal_int(2, n)
            n = r.sdiffstore(d3, k1, k2)
            assert_equal_int(1, n)
        finally:
            r.delete(k1, k2, d1, d2, d3)
    results.append(run_test("SUNIONSTORE_SINTERSTORE_SDIFFSTORE", cat, t_store_ops))

    def t_smove():
        src = key(prefix, "set:move_src")
        dst = key(prefix, "set:move_dst")
        try:
            r.sadd(src, "a", "b")
            r.sadd(dst, "c")
            ok = r.smove(src, dst, "a")
            assert_true(ok, "SMOVE should succeed")
            assert_list_equal_unordered(["b"], list(r.smembers(src)))
            assert_list_equal_unordered(["a", "c"], list(r.smembers(dst)))
        finally:
            r.delete(src, dst)
    results.append(run_test("SMOVE", cat, t_smove))

    def t_sscan():
        k = key(prefix, "set:scan")
        try:
            for i in range(20):
                r.sadd(k, f"member{i}")
            all_members = []
            cursor = 0
            while True:
                cursor, members = r.sscan(k, cursor, match="*", count=10)
                all_members.extend(members)
                if cursor == 0:
                    break
            assert_true(len(all_members) == 20, f"SSCAN should return all 20 members, got {len(all_members)}")
        finally:
            r.delete(k)
    results.append(run_test("SSCAN", cat, t_sscan))

    def t_sintercard():
        k1 = key(prefix, "set:ic1")
        k2 = key(prefix, "set:ic2")
        try:
            r.sadd(k1, "a", "b", "c")
            r.sadd(k2, "b", "c", "d")
            n = r.sintercard(2, keys=[k1, k2])
            assert_equal_int(2, n)
        finally:
            r.delete(k1, k2)
    results.append(run_test("SINTERCARD", cat, t_sintercard))

    return results


register("sets", test_sets)
