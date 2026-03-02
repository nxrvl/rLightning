"""Category 7: Key Management tests."""

import time as time_mod
from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int,
)


def test_keys(r, prefix):
    results = []
    cat = "keys"

    def t_del():
        k1 = key(prefix, "key:del1")
        k2 = key(prefix, "key:del2")
        k3 = key(prefix, "key:del3")
        r.set(k1, "a")
        r.set(k2, "b")
        r.set(k3, "c")
        n = r.delete(k1)
        assert_equal_int(1, n)
        n = r.delete(k2, k3)
        assert_equal_int(2, n)
    results.append(run_test("DEL_single_and_multiple", cat, t_del))

    def t_exists():
        k1 = key(prefix, "key:ex1")
        k2 = key(prefix, "key:ex2")
        try:
            r.set(k1, "a")
            r.set(k2, "b")
            n = r.exists(k1)
            assert_equal_int(1, n)
            n = r.exists(k1, k2, key(prefix, "key:nonexist"))
            assert_equal_int(2, n)
        finally:
            r.delete(k1, k2)
    results.append(run_test("EXISTS_single_and_multiple", cat, t_exists))

    def t_expire_ttl():
        k = key(prefix, "key:expire")
        try:
            r.set(k, "val")
            ok = r.expire(k, 10)
            assert_true(ok, "EXPIRE should succeed")
            ttl = r.ttl(k)
            assert_true(0 < ttl <= 10, "TTL should be between 0 and 10s")
        finally:
            r.delete(k)
    results.append(run_test("EXPIRE_TTL", cat, t_expire_ttl))

    def t_pexpire_pttl():
        k = key(prefix, "key:pexpire")
        try:
            r.set(k, "val")
            ok = r.pexpire(k, 10000)
            assert_true(ok, "PEXPIRE should succeed")
            pttl = r.pttl(k)
            assert_true(pttl > 0, "PTTL should be positive")
        finally:
            r.delete(k)
    results.append(run_test("PEXPIRE_PTTL", cat, t_pexpire_pttl))

    def t_expireat():
        k = key(prefix, "key:expireat")
        try:
            r.set(k, "val")
            future = int(time_mod.time()) + 60
            ok = r.expireat(k, future)
            assert_true(ok, "EXPIREAT should succeed")
            ttl = r.ttl(k)
            assert_true(ttl > 50, "TTL should be ~60s")

            k2 = key(prefix, "key:pexpireat")
            r.set(k2, "val")
            ok = r.pexpireat(k2, future * 1000)
            assert_true(ok, "PEXPIREAT should succeed")
        finally:
            r.delete(k, key(prefix, "key:pexpireat"))
    results.append(run_test("EXPIREAT_PEXPIREAT", cat, t_expireat))

    def t_persist():
        k = key(prefix, "key:persist")
        try:
            r.set(k, "val", ex=10)
            ok = r.persist(k)
            assert_true(ok, "PERSIST should succeed")
            ttl = r.ttl(k)
            # -1 means no TTL
            assert_true(ttl == -1, "TTL should be -1 after PERSIST (no expiration)")
        finally:
            r.delete(k)
    results.append(run_test("PERSIST", cat, t_persist))

    def t_type():
        ks = key(prefix, "key:type_s")
        kl = key(prefix, "key:type_l")
        kh = key(prefix, "key:type_h")
        kz = key(prefix, "key:type_z")
        kset = key(prefix, "key:type_set")
        try:
            r.set(ks, "val")
            r.lpush(kl, "val")
            r.hset(kh, "f", "v")
            r.zadd(kz, {"a": 1})
            r.sadd(kset, "a")
            assert_equal("string", r.type(ks))
            assert_equal("list", r.type(kl))
            assert_equal("hash", r.type(kh))
            assert_equal("zset", r.type(kz))
            assert_equal("set", r.type(kset))
        finally:
            r.delete(ks, kl, kh, kz, kset)
    results.append(run_test("TYPE", cat, t_type))

    def t_rename():
        k1 = key(prefix, "key:rename_src")
        k2 = key(prefix, "key:rename_dst")
        k3 = key(prefix, "key:renamenx_dst")
        try:
            r.set(k1, "val")
            r.rename(k1, k2)
            assert_equal("val", r.get(k2))
            # RENAMENX
            r.set(k1, "new")
            r.set(k3, "exists")
            ok = r.renamenx(k1, k3)
            assert_true(not ok, "RENAMENX should fail when dest exists")
            r.delete(k3)
            r.set(k1, "new")
            ok = r.renamenx(k1, k3)
            assert_true(ok, "RENAMENX should succeed when dest doesn't exist")
        finally:
            r.delete(k1, k2, k3)
    results.append(run_test("RENAME_RENAMENX", cat, t_rename))

    def t_keys_pattern():
        k1 = key(prefix, "key:pat_a")
        k2 = key(prefix, "key:pat_b")
        k3 = key(prefix, "key:pat_c")
        try:
            r.set(k1, "1")
            r.set(k2, "2")
            r.set(k3, "3")
            keys = r.keys(key(prefix, "key:pat_*"))
            assert_true(len(keys) >= 3, "KEYS should return at least 3 keys")
        finally:
            r.delete(k1, k2, k3)
    results.append(run_test("KEYS_pattern", cat, t_keys_pattern))

    def t_scan():
        test_keys = []
        for i in range(10):
            k = key(prefix, f"key:scan_{i}")
            test_keys.append(k)
            r.set(k, str(i))
        try:
            all_keys = []
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match=key(prefix, "key:scan_*"), count=5)
                all_keys.extend(keys)
                if cursor == 0:
                    break
            assert_true(len(all_keys) >= 10, f"SCAN should find at least 10 keys, got {len(all_keys)}")
        finally:
            for k in test_keys:
                r.delete(k)
    results.append(run_test("SCAN_with_pattern", cat, t_scan))

    def t_unlink():
        k = key(prefix, "key:unlink")
        r.set(k, "val")
        n = r.unlink(k)
        assert_equal_int(1, n)
        assert_equal_int(0, r.exists(k))
    results.append(run_test("UNLINK", cat, t_unlink))

    def t_object_encoding():
        k = key(prefix, "key:obj_enc")
        try:
            r.set(k, "12345")
            enc = r.object("encoding", k)
            assert_true(len(str(enc)) > 0, "OBJECT ENCODING should return a value")
        finally:
            r.delete(k)
    results.append(run_test("OBJECT_ENCODING", cat, t_object_encoding))

    def t_copy():
        src = key(prefix, "key:copy_src")
        dst = key(prefix, "key:copy_dst")
        try:
            r.set(src, "copyval")
            res = r.copy(src, dst)
            assert_true(res, "COPY should succeed")
            assert_equal("copyval", r.get(dst))
        finally:
            r.delete(src, dst)
    results.append(run_test("COPY", cat, t_copy))

    def t_dump_restore():
        src = key(prefix, "key:dump")
        dst = key(prefix, "key:restore")
        try:
            # Need a non-decode_responses client for DUMP/RESTORE
            import redis as redis_lib
            rb = redis_lib.Redis(
                host=r.connection_pool.connection_kwargs["host"],
                port=r.connection_pool.connection_kwargs["port"],
                password=r.connection_pool.connection_kwargs.get("password"),
                db=0, decode_responses=False,
            )
            rb.set(src, "dumpval")
            dump = rb.dump(src)
            assert_true(len(dump) > 0, "DUMP should return data")
            rb.restore(dst, 0, dump)
            val = rb.get(dst)
            assert_equal(b"dumpval", val)
            rb.close()
        finally:
            r.delete(src, dst)
    results.append(run_test("DUMP_RESTORE", cat, t_dump_restore))

    return results


register("keys", test_keys)
