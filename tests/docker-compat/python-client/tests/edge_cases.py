"""Category 14: Edge Cases & Error Handling tests."""

import threading
import redis as redis_lib
from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int,
)


def test_edge_cases(r, prefix):
    results = []
    cat = "edge_cases"

    def t_wrongtype():
        k = key(prefix, "edge:wrongtype")
        try:
            r.set(k, "string_value")
            errored = False
            err_msg = ""
            try:
                r.lpush(k, "bad")
            except redis_lib.exceptions.ResponseError as e:
                errored = True
                err_msg = str(e)
            assert_true(errored, "LPUSH on string key should raise ResponseError")
            assert_true("WRONGTYPE" in err_msg, f"error should be WRONGTYPE: {err_msg}")
        finally:
            r.delete(k)
    results.append(run_test("wrong_type_error", cat, t_wrongtype))

    def t_nonexistent_get():
        val = r.get(key(prefix, "edge:nonexist"))
        assert_true(val is None, "GET nonexistent key should return None")
    results.append(run_test("nonexistent_key_GET", cat, t_nonexistent_get))

    def t_nonexistent_ops():
        k = key(prefix, "edge:nonexist_ops")
        n = r.llen(k)
        assert_equal_int(0, n)
        m = r.hgetall(k)
        assert_equal(0, len(m))
        n = r.scard(k)
        assert_equal_int(0, n)
    results.append(run_test("nonexistent_key_operations", cat, t_nonexistent_ops))

    def t_empty_string():
        k = key(prefix, "edge:empty")
        try:
            r.set(k, "")
            val = r.get(k)
            assert_equal("", val)
            assert_equal_int(0, r.strlen(k))
        finally:
            r.delete(k)
    results.append(run_test("empty_string_value", cat, t_empty_string))

    def t_binary_data():
        k = key(prefix, "edge:binary")
        import redis as redis_lib
        rb = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=False,
        )
        try:
            bin_val = b"hello\x00world\x00\xff\xfe"
            rb.set(k, bin_val)
            val = rb.get(k)
            assert_equal(bin_val, val)
        finally:
            rb.delete(k)
            rb.close()
    results.append(run_test("binary_data_null_bytes", cat, t_binary_data))

    def t_large_value():
        k = key(prefix, "edge:large")
        try:
            large_val = "x" * (1024 * 1024)
            r.set(k, large_val)
            val = r.get(k)
            assert_equal(len(large_val), len(val))
        finally:
            r.delete(k)
    results.append(run_test("large_value_1MB", cat, t_large_value))

    def t_unicode():
        k = key(prefix, "edge:utf8")
        try:
            unicode_val = "Hello 世界 🌍 Ñoño café"
            r.set(k, unicode_val)
            val = r.get(k)
            assert_equal(unicode_val, val)
        finally:
            r.delete(k)
    results.append(run_test("unicode_utf8", cat, t_unicode))

    def t_special_keys():
        keys = [
            key(prefix, "edge:space key"),
            key(prefix, "edge:newline\nkey"),
            key(prefix, "edge:colon:key"),
            key(prefix, "edge:tab\tkey"),
        ]
        try:
            for i, k in enumerate(keys):
                val = f"val_{i}"
                r.set(k, val)
                got = r.get(k)
                assert_equal(val, got)
        finally:
            for k in keys:
                r.delete(k)
    results.append(run_test("keys_with_special_chars", cat, t_special_keys))

    def t_int_ops():
        k = key(prefix, "edge:intops")
        try:
            r.set(k, "10")
            val = r.incrby(k, -20)
            assert_equal_int(-10, val)

            k2 = key(prefix, "edge:intzero")
            val = r.incr(k2)
            assert_equal_int(1, val)
            r.delete(k2)

            k3 = key(prefix, "edge:intnan")
            r.set(k3, "notanumber")
            errored = False
            try:
                r.incr(k3)
            except redis_lib.exceptions.ResponseError:
                errored = True
            assert_true(errored, "INCR on non-numeric should raise ResponseError")
            r.delete(k3)
        finally:
            r.delete(k)
    results.append(run_test("negative_zero_overflow_integers", cat, t_int_ops))

    def t_concurrent():
        k = key(prefix, "edge:concurrent")
        try:
            import redis as redis_lib
            success_count = [0]
            lock = threading.Lock()
            n = 50

            def worker():
                client = redis_lib.Redis(
                    host=r.connection_pool.connection_kwargs["host"],
                    port=r.connection_pool.connection_kwargs["port"],
                    password=r.connection_pool.connection_kwargs.get("password"),
                    db=0, decode_responses=True,
                )
                try:
                    client.incr(k)
                    with lock:
                        success_count[0] += 1
                except Exception:
                    pass
                finally:
                    client.close()

            threads = []
            for _ in range(n):
                t = threading.Thread(target=worker)
                threads.append(t)
                t.start()
            for t in threads:
                t.join()

            assert_equal_int(n, success_count[0])
            assert_equal("50", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("concurrent_operations", cat, t_concurrent))

    return results


register("edge_cases", test_edge_cases)
