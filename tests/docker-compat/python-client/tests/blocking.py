"""Category: Blocking commands tests."""

import threading
import time

import redis as redis_lib

from .framework import (
    register, run_test, key, assert_equal, assert_true,
)


def test_blocking(r, prefix):
    results = []
    cat = "blocking"

    def create_client():
        return redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
        )

    # BLPOP_with_data: LPUSH then BLPOP, verify immediate return
    def t_blpop_with_data():
        k = key(prefix, "block:blpop_data")
        try:
            r.rpush(k, "val1")
            result = r.blpop(k, timeout=1)
            assert_true(result is not None, "BLPOP should return a value")
            assert_equal(k, result[0])
            assert_equal("val1", result[1])
        finally:
            r.delete(k)
    results.append(run_test("BLPOP_with_data", cat, t_blpop_with_data))

    # BLPOP_timeout: BLPOP on empty key with 1s timeout, verify timeout return
    def t_blpop_timeout():
        k = key(prefix, "block:blpop_timeout")
        try:
            start = time.time()
            result = r.blpop(k, timeout=1)
            elapsed = (time.time() - start) * 1000  # ms

            assert_true(result is None, "BLPOP should return None on timeout")
            assert_true(800 <= elapsed <= 2500,
                        f"Timeout duration out of range: {elapsed:.0f}ms")
        finally:
            r.delete(k)
    results.append(run_test("BLPOP_timeout", cat, t_blpop_timeout))

    # BLPOP_multi_key: BLPOP on multiple keys, LPUSH to second key, verify correct key returned
    def t_blpop_multi_key():
        k1 = key(prefix, "block:multi1")
        k2 = key(prefix, "block:multi2")
        try:
            r.rpush(k2, "from_k2")
            result = r.blpop([k1, k2], timeout=1)
            assert_true(result is not None, "BLPOP should return a value")
            assert_equal(k2, result[0])
            assert_equal("from_k2", result[1])
        finally:
            r.delete(k1, k2)
    results.append(run_test("BLPOP_multi_key", cat, t_blpop_multi_key))

    # BRPOP_basic: Same as BLPOP but from right side
    def t_brpop_basic():
        k = key(prefix, "block:brpop")
        try:
            r.rpush(k, "first", "second")
            result = r.brpop(k, timeout=1)
            assert_true(result is not None, "BRPOP should return a value")
            assert_equal(k, result[0])
            assert_equal("second", result[1])
        finally:
            r.delete(k)
    results.append(run_test("BRPOP_basic", cat, t_brpop_basic))

    # BLMOVE_basic: BLMOVE from source to dest list, verify move
    def t_blmove_basic():
        src = key(prefix, "block:blmove_src")
        dst = key(prefix, "block:blmove_dst")
        try:
            r.rpush(src, "item1")
            val = r.blmove(src, dst, timeout=1, src="LEFT", dest="RIGHT")
            assert_equal("item1", val)

            # Verify item is in destination
            dst_vals = r.lrange(dst, 0, -1)
            assert_equal(1, len(dst_vals))
            assert_equal("item1", dst_vals[0])
        finally:
            r.delete(src, dst)
    results.append(run_test("BLMOVE_basic", cat, t_blmove_basic))

    # BLMPOP_basic: BLMPOP with LEFT/RIGHT direction
    def t_blmpop_basic():
        k = key(prefix, "block:blmpop")
        try:
            r.rpush(k, "a", "b", "c")
            # BLMPOP timeout numkeys key ... LEFT|RIGHT
            result = r.execute_command("BLMPOP", "1", "1", k, "LEFT")
            assert_true(result is not None, "BLMPOP should return a value")
            assert_true(isinstance(result, (list, tuple)), "BLMPOP should return a list")
            assert_equal(k, result[0])
            assert_true(isinstance(result[1], (list, tuple)), "BLMPOP elements should be a list")
            assert_equal("a", result[1][0])
        finally:
            r.delete(k)
    results.append(run_test("BLMPOP_basic", cat, t_blmpop_basic))

    # BZPOPMIN_basic: BZPOPMIN with data and timeout variants
    def t_bzpopmin_basic():
        k = key(prefix, "block:bzpopmin")
        try:
            r.zadd(k, {"a": 1.0, "b": 2.0})
            result = r.bzpopmin(k, timeout=1)
            assert_true(result is not None, "BZPOPMIN should return a value")
            assert_equal(k, result[0])
            assert_equal("a", result[1])
            assert_equal(1.0, float(result[2]))
        finally:
            r.delete(k)
    results.append(run_test("BZPOPMIN_basic", cat, t_bzpopmin_basic))

    # BZPOPMAX_basic: BZPOPMAX with data and timeout variants
    def t_bzpopmax_basic():
        k = key(prefix, "block:bzpopmax")
        try:
            r.zadd(k, {"a": 1.0, "b": 2.0})
            result = r.bzpopmax(k, timeout=1)
            assert_true(result is not None, "BZPOPMAX should return a value")
            assert_equal(k, result[0])
            assert_equal("b", result[1])
            assert_equal(2.0, float(result[2]))
        finally:
            r.delete(k)
    results.append(run_test("BZPOPMAX_basic", cat, t_bzpopmax_basic))

    # BLOCKING_concurrent: Producer-consumer pattern
    def t_blocking_concurrent():
        k = key(prefix, "block:concurrent")
        r2 = create_client()
        try:
            push_error = [None]

            def producer():
                try:
                    time.sleep(0.3)
                    r2.rpush(k, "async_val")
                except Exception as e:
                    push_error[0] = e

            t = threading.Thread(target=producer)
            t.start()

            # BLPOP should block then receive pushed value
            result = r.blpop(k, timeout=5)
            assert_true(result is not None, "BLPOP should receive the pushed value")
            assert_equal("async_val", result[1])

            t.join(timeout=5)
            assert_true(push_error[0] is None,
                        f"Producer push failed: {push_error[0]}")
        finally:
            r2.close()
            r.delete(k)
    results.append(run_test("BLOCKING_concurrent", cat, t_blocking_concurrent))

    # BLOCKING_timeout_accuracy: Verify timeout duration is within acceptable range
    def t_blocking_timeout_accuracy():
        k = key(prefix, "block:timeout_accuracy")
        try:
            start = time.time()
            result = r.blpop(k, timeout=1)
            elapsed = (time.time() - start) * 1000  # ms

            assert_true(result is None, "BLPOP should return None on timeout")
            diff = abs(elapsed - 1000)
            assert_true(diff <= 500,
                        f"Timeout accuracy out of range: expected ~1000ms, got {elapsed:.0f}ms (diff: {diff:.0f}ms)")
        finally:
            r.delete(k)
    results.append(run_test("BLOCKING_timeout_accuracy", cat, t_blocking_timeout_accuracy))

    return results


register("blocking", test_blocking)
