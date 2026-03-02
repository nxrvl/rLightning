"""Category 11: Lua Scripting tests."""

import redis as redis_lib
from .framework import register, run_test, key, assert_equal, assert_true


def test_scripting(r, prefix):
    results = []
    cat = "scripting"

    def t_eval_simple():
        res = r.eval("return 42", 0)
        assert_equal(42, res)
    results.append(run_test("EVAL_simple", cat, t_eval_simple))

    def t_eval_keys_args():
        k = key(prefix, "lua:kv")
        try:
            script = """
                redis.call('SET', KEYS[1], ARGV[1])
                return redis.call('GET', KEYS[1])
            """
            res = r.eval(script, 1, k, "lua_value")
            assert_equal("lua_value", res)
        finally:
            r.delete(k)
    results.append(run_test("EVAL_with_keys_and_args", cat, t_eval_keys_args))

    def t_evalsha():
        k = key(prefix, "lua:sha")
        try:
            script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
            sha = r.script_load(script)
            assert_true(len(sha) == 40, "SHA should be 40 hex chars")
            res = r.evalsha(sha, 1, k, "sha_value")
            assert_equal("sha_value", res)
        finally:
            r.delete(k)
    results.append(run_test("EVALSHA_after_SCRIPT_LOAD", cat, t_evalsha))

    def t_error_handling():
        errored = False
        try:
            r.eval("return redis.call('GET')", 0)
        except redis_lib.exceptions.ResponseError:
            errored = True
        assert_true(errored, "EVAL with bad call should raise ResponseError")
    results.append(run_test("script_error_handling", cat, t_error_handling))

    def t_call_vs_pcall():
        k = key(prefix, "lua:pcall")
        try:
            r.set(k, "string_val")
            # redis.call raises error on WRONGTYPE
            errored = False
            try:
                r.eval("return redis.call('LPUSH', KEYS[1], 'bad')", 1, k)
            except redis_lib.exceptions.ResponseError:
                errored = True
            assert_true(errored, "redis.call should propagate WRONGTYPE error")
            # pcall catches the error
            res = r.eval(
                "local ok, err = pcall(redis.call, 'LPUSH', KEYS[1], 'bad'); "
                "if ok then return 'ok' else return 'caught' end",
                1, k,
            )
            assert_equal("caught", res)
        finally:
            r.delete(k)
    results.append(run_test("redis_call_vs_pcall", cat, t_call_vs_pcall))

    def t_script_exists_flush():
        script = "return 'test_exists'"
        sha = r.script_load(script)
        exists = r.script_exists(sha, "0" * 40)
        assert_true(exists[0], "loaded script should exist")
        assert_true(not exists[1], "fake SHA should not exist")
        r.script_flush()
        exists = r.script_exists(sha)
        assert_true(not exists[0], "script should not exist after FLUSH")
    results.append(run_test("SCRIPT_EXISTS_FLUSH", cat, t_script_exists_flush))

    return results


register("scripting", test_scripting)
