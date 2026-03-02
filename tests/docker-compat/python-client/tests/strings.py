"""Category 2: String Operations tests."""

import time as time_mod
from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int, assert_equal_float,
)


def test_strings(r, prefix):
    results = []
    cat = "strings"

    def t_set_get():
        k = key(prefix, "str:basic")
        try:
            r.set(k, "hello")
            assert_equal("hello", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("SET_GET_basic", cat, t_set_get))

    def t_set_nx():
        k = key(prefix, "str:setnx")
        try:
            ok = r.set(k, "first", nx=True)
            assert_true(ok is True, "first SetNX should succeed")
            ok = r.set(k, "second", nx=True)
            assert_true(ok is None, "second SetNX should fail")
            assert_equal("first", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("SET_NX_flag", cat, t_set_nx))

    def t_set_xx():
        k = key(prefix, "str:setxx")
        try:
            ok = r.set(k, "nope", xx=True)
            assert_true(ok is None, "SetXX on non-existent key should fail")
            r.set(k, "exists")
            ok = r.set(k, "updated", xx=True)
            assert_true(ok is True, "SetXX on existing key should succeed")
            assert_equal("updated", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("SET_XX_flag", cat, t_set_xx))

    def t_set_ex():
        k = key(prefix, "str:setex")
        try:
            r.set(k, "ttlval", ex=10)
            ttl = r.ttl(k)
            assert_true(0 < ttl <= 10, "TTL should be between 0 and 10s")
        finally:
            r.delete(k)
    results.append(run_test("SET_with_EX", cat, t_set_ex))

    def t_set_px():
        k = key(prefix, "str:setpx")
        try:
            r.set(k, "pxval", px=10000)
            pttl = r.pttl(k)
            assert_true(pttl > 0, "PTTL should be positive")
        finally:
            r.delete(k)
    results.append(run_test("SET_with_PX", cat, t_set_px))

    def t_mset_mget():
        k1 = key(prefix, "str:m1")
        k2 = key(prefix, "str:m2")
        k3 = key(prefix, "str:m3")
        try:
            r.mset({k1: "a", k2: "b", k3: "c"})
            vals = r.mget(k1, k2, k3)
            assert_equal(3, len(vals))
            assert_equal("a", vals[0])
            assert_equal("b", vals[1])
            assert_equal("c", vals[2])
        finally:
            r.delete(k1, k2, k3)
    results.append(run_test("MSET_MGET", cat, t_mset_mget))

    def t_incr_decr():
        k = key(prefix, "str:incr")
        try:
            r.set(k, "10")
            val = r.incr(k)
            assert_equal_int(11, val)
            val = r.decr(k)
            assert_equal_int(10, val)
        finally:
            r.delete(k)
    results.append(run_test("INCR_DECR", cat, t_incr_decr))

    def t_incrby_decrby():
        k = key(prefix, "str:incrby")
        try:
            r.set(k, "100")
            val = r.incrby(k, 25)
            assert_equal_int(125, val)
            val = r.decrby(k, 50)
            assert_equal_int(75, val)
        finally:
            r.delete(k)
    results.append(run_test("INCRBY_DECRBY", cat, t_incrby_decrby))

    def t_incrbyfloat():
        k = key(prefix, "str:incrf")
        try:
            r.set(k, "10.5")
            val = r.incrbyfloat(k, 0.1)
            assert_equal_float(10.6, val, 0.001)
        finally:
            r.delete(k)
    results.append(run_test("INCRBYFLOAT", cat, t_incrbyfloat))

    def t_append_strlen():
        k = key(prefix, "str:append")
        try:
            r.set(k, "hello")
            new_len = r.append(k, " world")
            assert_equal_int(11, new_len)
            slen = r.strlen(k)
            assert_equal_int(11, slen)
        finally:
            r.delete(k)
    results.append(run_test("APPEND_STRLEN", cat, t_append_strlen))

    def t_getrange_setrange():
        k = key(prefix, "str:range")
        try:
            r.set(k, "Hello, World!")
            sub = r.getrange(k, 7, 11)
            assert_equal("World", sub)
            r.setrange(k, 7, "Redis")
            assert_equal("Hello, Redis!", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("GETRANGE_SETRANGE", cat, t_getrange_setrange))

    def t_getset():
        k = key(prefix, "str:getset")
        try:
            r.set(k, "old")
            old = r.getset(k, "new")
            assert_equal("old", old)
            assert_equal("new", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("GETSET_deprecated", cat, t_getset))

    def t_getdel():
        k = key(prefix, "str:getdel")
        try:
            r.set(k, "deleteme")
            val = r.getdel(k)
            assert_equal("deleteme", val)
            assert_true(r.get(k) is None, "key should be gone after GETDEL")
        finally:
            r.delete(k)
    results.append(run_test("GETDEL", cat, t_getdel))

    def t_setnx_cmd():
        k = key(prefix, "str:setnxcmd")
        try:
            ok = r.setnx(k, "val")
            assert_true(ok is True, "SETNX should succeed on new key")
            ok = r.setnx(k, "val2")
            assert_true(ok is False, "SETNX should fail on existing key")
        finally:
            r.delete(k)
    results.append(run_test("SETNX_command", cat, t_setnx_cmd))

    def t_setex():
        k = key(prefix, "str:setexcmd")
        try:
            r.setex(k, 10, "val")
            assert_equal("val", r.get(k))
            ttl = r.ttl(k)
            assert_true(ttl > 0, "TTL should be positive after SETEX")
        finally:
            r.delete(k)
    results.append(run_test("SETEX_command", cat, t_setex))

    def t_psetex():
        k = key(prefix, "str:psetex")
        try:
            r.psetex(k, 10000, "val")
            pttl = r.pttl(k)
            assert_true(pttl > 0, "PTTL should be positive after PSETEX")
        finally:
            r.delete(k)
    results.append(run_test("PSETEX_command", cat, t_psetex))

    def t_set_exat():
        k = key(prefix, "str:exat")
        try:
            expire_at = int(time_mod.time()) + 60
            r.set(k, "val", exat=expire_at)
            ttl = r.ttl(k)
            assert_true(50 < ttl <= 60, "TTL should be ~60s")
        finally:
            r.delete(k)
    results.append(run_test("SET_with_EXAT", cat, t_set_exat))

    def t_mget_missing():
        k1 = key(prefix, "str:mget_exists")
        try:
            r.set(k1, "val")
            vals = r.mget(k1, key(prefix, "str:mget_nonexist"))
            assert_equal(2, len(vals))
            assert_equal("val", vals[0])
            assert_true(vals[1] is None, "missing key should return None")
        finally:
            r.delete(k1)
    results.append(run_test("MGET_with_missing_keys", cat, t_mget_missing))

    return results


register("strings", test_strings)
