"""Category 10: Pipelining & Performance tests."""

from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int, assert_list_equal,
)


def test_pipeline(r, prefix):
    results = []
    cat = "pipeline"

    def t_100_sets():
        keys = []
        pipe = r.pipeline(transaction=False)
        for i in range(100):
            k = key(prefix, f"pipe:set_{i}")
            keys.append(k)
            pipe.set(k, str(i))
        res = pipe.execute()
        assert_equal(100, len(res))
        for k in keys:
            r.delete(k)
    results.append(run_test("pipeline_100_SETs", cat, t_100_sets))

    def t_100_gets():
        keys = []
        for i in range(100):
            k = key(prefix, f"pipe:get_{i}")
            keys.append(k)
            r.set(k, str(i))
        try:
            pipe = r.pipeline(transaction=False)
            for k in keys:
                pipe.get(k)
            res = pipe.execute()
            for i, val in enumerate(res):
                assert_equal(str(i), val)
        finally:
            for k in keys:
                r.delete(k)
    results.append(run_test("pipeline_100_GETs", cat, t_100_gets))

    def t_mixed():
        k1 = key(prefix, "pipe:mix1")
        k2 = key(prefix, "pipe:mix2")
        k3 = key(prefix, "pipe:mix3")
        try:
            pipe = r.pipeline(transaction=False)
            pipe.set(k1, "hello")
            pipe.incr(k2)
            pipe.lpush(k3, "a", "b")
            pipe.get(k1)
            pipe.lrange(k3, 0, -1)
            res = pipe.execute()
            assert_true(res[0] is True, "SET should succeed")
            assert_equal_int(1, res[1])
            assert_equal_int(2, res[2])
            assert_equal("hello", res[3])
            assert_list_equal(["b", "a"], res[4])
        finally:
            r.delete(k1, k2, k3)
    results.append(run_test("pipeline_mixed_commands", cat, t_mixed))

    def t_with_errors():
        k = key(prefix, "pipe:err")
        try:
            r.set(k, "string_val")
            pipe = r.pipeline(transaction=False)
            pipe.get(k)
            pipe.lpush(k, "bad")
            res = pipe.execute(raise_on_error=False)
            assert_equal("string_val", res[0])
            assert_true(isinstance(res[1], Exception), "LPUSH should fail with WRONGTYPE")
        finally:
            r.delete(k)
    results.append(run_test("pipeline_with_errors", cat, t_with_errors))

    def t_ordering():
        keys = []
        for i in range(10):
            k = key(prefix, f"pipe:order_{i}")
            keys.append(k)
            r.set(k, f"val_{i}")
        try:
            pipe = r.pipeline(transaction=False)
            for k in keys:
                pipe.get(k)
            res = pipe.execute()
            for i, val in enumerate(res):
                expected = f"val_{i}"
                assert_equal(expected, val)
        finally:
            for k in keys:
                r.delete(k)
    results.append(run_test("pipeline_result_ordering", cat, t_ordering))

    return results


register("pipeline", test_pipeline)
