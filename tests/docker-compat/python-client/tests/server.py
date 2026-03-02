"""Category 15: Server Commands tests."""

import redis as redis_lib
from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int,
)


def test_server(r, prefix):
    results = []
    cat = "server"

    def t_config_get():
        res = r.config_get("maxmemory")
        assert_true(len(res) > 0, "CONFIG GET should return results")
    results.append(run_test("CONFIG_GET", cat, t_config_get))

    def t_config_set():
        # Save original value to restore later
        orig = r.config_get("maxmemory-policy")
        orig_val = orig["maxmemory-policy"]
        try:
            r.config_set("maxmemory-policy", "allkeys-lru")
            res = r.config_get("maxmemory-policy")
            assert_equal("allkeys-lru", res["maxmemory-policy"])
        finally:
            r.config_set("maxmemory-policy", orig_val)
    results.append(run_test("CONFIG_SET_and_GET", cat, t_config_set))

    def t_flushdb():
        k1 = key(prefix, "srv:flush1")
        k2 = key(prefix, "srv:flush2")
        r.set(k1, "a")
        r.set(k2, "b")
        # Use a dedicated DB to avoid flushing test keys
        r2 = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=2, decode_responses=True,
        )
        try:
            r2.set("flush_test", "val")
            r2.flushdb()
            size = r2.dbsize()
            assert_equal_int(0, size)
        finally:
            r2.close()
        r.delete(k1, k2)
    results.append(run_test("FLUSHDB", cat, t_flushdb))

    def t_time():
        t = r.time()
        assert_true(len(t) == 2, "TIME should return (seconds, microseconds)")
        assert_true(t[0] > 0, "TIME seconds should be positive")
    results.append(run_test("TIME", cat, t_time))

    def t_randomkey():
        k = key(prefix, "srv:randkey")
        try:
            r.set(k, "val")
            rk = r.randomkey()
            assert_true(rk is not None and len(rk) > 0, "RANDOMKEY should return a key name")
        finally:
            r.delete(k)
    results.append(run_test("RANDOMKEY", cat, t_randomkey))

    def t_command_count():
        n = r.command_count()
        assert_true(n > 50, "COMMAND COUNT should be > 50")
    results.append(run_test("COMMAND_COUNT", cat, t_command_count))

    def t_wait():
        n = r.execute_command("WAIT", 0, 100)
        assert_true(int(n) >= 0, "WAIT should return >= 0 replicas")
    results.append(run_test("WAIT_with_timeout", cat, t_wait))

    return results


register("server", test_server)
