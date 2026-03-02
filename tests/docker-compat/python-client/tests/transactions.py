"""Category 8: Transactions tests."""

import redis as redis_lib
from .framework import register, run_test, key, assert_equal, assert_true


def test_transactions(r, prefix):
    results = []
    cat = "transactions"

    def t_multi_exec():
        k1 = key(prefix, "tx:basic1")
        k2 = key(prefix, "tx:basic2")
        try:
            pipe = r.pipeline(transaction=True)
            pipe.set(k1, "a")
            pipe.set(k2, "b")
            pipe.execute()
            assert_equal("a", r.get(k1))
            assert_equal("b", r.get(k2))
        finally:
            r.delete(k1, k2)
    results.append(run_test("MULTI_EXEC_basic", cat, t_multi_exec))

    def t_multi_discard():
        k = key(prefix, "tx:discard")
        try:
            r.set(k, "original")
            # Use a dedicated connection for MULTI/DISCARD since these are
            # connection-stateful commands that must execute on the same connection
            conn = r.connection_pool.get_connection("MULTI")
            try:
                conn.send_command("MULTI")
                conn.read_response()
                conn.send_command("SET", k, "changed")
                conn.read_response()  # QUEUED
                conn.send_command("DISCARD")
                conn.read_response()
            finally:
                r.connection_pool.release(conn)
            assert_equal("original", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("MULTI_DISCARD", cat, t_multi_discard))

    def t_watch():
        k = key(prefix, "tx:watch")
        try:
            r.set(k, "10")
            with r.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(k)
                        val = pipe.get(k)
                        assert_equal("10", val)
                        pipe.multi()
                        pipe.set(k, "20")
                        pipe.execute()
                        break
                    except redis_lib.WatchError:
                        continue
            assert_equal("20", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("WATCH_UNWATCH_optimistic_lock", cat, t_watch))

    def t_watch_modified():
        k = key(prefix, "tx:watchmod")
        try:
            r.set(k, "original")
            r2 = redis_lib.Redis(
                host=r.connection_pool.connection_kwargs["host"],
                port=r.connection_pool.connection_kwargs["port"],
                password=r.connection_pool.connection_kwargs.get("password"),
                db=0, decode_responses=True,
            )
            watch_error = False
            with r.pipeline() as pipe:
                try:
                    pipe.watch(k)
                    # Another client modifies the key
                    r2.set(k, "modified_by_other")
                    pipe.multi()
                    pipe.set(k, "should_fail")
                    pipe.execute()
                except redis_lib.WatchError:
                    watch_error = True
            r2.close()
            assert_true(watch_error, "should get WatchError because key was modified")
            assert_equal("modified_by_other", r.get(k))
        finally:
            r.delete(k)
    results.append(run_test("WATCH_key_modification_detection", cat, t_watch_modified))

    def t_queued_errors():
        k = key(prefix, "tx:qerr")
        try:
            r.set(k, "string_value")
            pipe = r.pipeline(transaction=True)
            pipe.set(k, "new_value")
            pipe.lpush(k, "bad")  # WRONGTYPE
            results_list = pipe.execute(raise_on_error=False)
            # First command should succeed
            assert_true(results_list[0] is True, "SET in tx should succeed")
            # Second should be an error
            assert_true(
                isinstance(results_list[1], Exception),
                "LPUSH should fail with WRONGTYPE",
            )
        finally:
            r.delete(k)
    results.append(run_test("queued_command_errors", cat, t_queued_errors))

    return results


register("transactions", test_transactions)
