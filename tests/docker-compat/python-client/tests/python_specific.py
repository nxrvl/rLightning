"""Python-specific tests for redis-py features."""

import redis as redis_lib
from .framework import register, run_test, key, assert_equal, assert_true


def test_python_specific(r, prefix):
    results = []
    cat = "python_specific"

    def t_connection_pool():
        pool = redis_lib.ConnectionPool(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            max_connections=5,
            decode_responses=True,
        )
        client = redis_lib.Redis(connection_pool=pool)
        try:
            for i in range(10):
                k = key(prefix, f"pyspec:pool_{i}")
                client.set(k, str(i))
                client.delete(k)
            info = pool.get_connection("_")
            pool.release(info)
            assert_true(True, "connection pool should work without errors")
        finally:
            client.close()
            pool.disconnect()
    results.append(run_test("connection_pool", cat, t_connection_pool))

    def t_decode_responses():
        # Test with decode_responses=True (default for our client)
        k = key(prefix, "pyspec:decode")
        try:
            r.set(k, "hello")
            val = r.get(k)
            assert_true(isinstance(val, str), "with decode_responses=True, GET should return str")
            assert_equal("hello", val)
        finally:
            r.delete(k)

        # Test with decode_responses=False
        rb = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=False,
        )
        try:
            rb.set(k, "hello")
            val = rb.get(k)
            assert_true(isinstance(val, bytes), "with decode_responses=False, GET should return bytes")
            assert_equal(b"hello", val)
        finally:
            rb.delete(k)
            rb.close()
    results.append(run_test("decode_responses_True_False", cat, t_decode_responses))

    def t_pubsub_thread():
        ch = key(prefix, "pyspec:ps_thread")
        received = []
        pub = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=True,
        )
        # Use a separate client for the pubsub to avoid sharing connection pool
        ps_client = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=True,
        )
        ps = ps_client.pubsub()
        thread = None
        try:
            def handler(message):
                received.append(message["data"])

            ps.subscribe(**{ch: handler})
            thread = ps.run_in_thread(sleep_time=0.01)
            import time
            time.sleep(0.2)
            pub.publish(ch, "thread_msg")
            time.sleep(0.5)
            assert_true(len(received) > 0, "pubsub thread should receive messages")
            assert_equal("thread_msg", received[0])
        finally:
            if thread is not None:
                thread.stop()
            try:
                ps.unsubscribe()
                ps.close()
            except Exception:
                pass
            pub.close()
            ps_client.close()
    results.append(run_test("pubsub_thread_pattern", cat, t_pubsub_thread))

    def t_pipeline_vs_transaction():
        k = key(prefix, "pyspec:pipetx")
        try:
            # Regular pipeline (no MULTI/EXEC)
            pipe = r.pipeline(transaction=False)
            pipe.set(k, "pipe_val")
            pipe.get(k)
            res = pipe.execute()
            assert_true(res[0] is True, "pipeline SET should succeed")
            assert_equal("pipe_val", res[1])

            # Transaction pipeline (MULTI/EXEC)
            pipe = r.pipeline(transaction=True)
            pipe.set(k, "tx_val")
            pipe.get(k)
            res = pipe.execute()
            assert_true(res[0] is True, "tx SET should succeed")
            assert_equal("tx_val", res[1])
        finally:
            r.delete(k)
    results.append(run_test("pipeline_vs_transaction", cat, t_pipeline_vs_transaction))

    def t_asyncio_redis():
        # Test that async redis module is importable (redis-py includes asyncio support)
        try:
            import redis.asyncio as aioredis
            assert_true(hasattr(aioredis, "Redis"), "redis.asyncio should have Redis class")
        except ImportError:
            raise Exception("redis.asyncio should be available in redis-py >= 5.0")
    results.append(run_test("asyncio_redis_available", cat, t_asyncio_redis))

    return results


register("python_specific", test_python_specific)
