"""Category: Lua Scripting Advanced tests."""

import redis as redis_lib
from .framework import register, run_test, key, assert_equal, assert_true


def test_scripting_advanced(r, prefix):
    results = []
    cat = "scripting_advanced"

    # EVAL_return_types: Test Lua returning string, integer, table, nil, boolean, error
    def t_eval_return_types():
        # String
        res = r.eval("return 'hello'", 0)
        assert_equal("hello", res)

        # Integer
        res = r.eval("return 123", 0)
        assert_equal(123, res)

        # Table (array) - returned as list
        res = r.eval("return {1, 2, 3}", 0)
        assert_true(isinstance(res, list), "table should return as list")
        assert_true(len(res) == 3, "table should have 3 elements")

        # Nil (Lua false returns as None in Redis)
        res = r.eval("return false", 0)
        assert_true(res is None, "false should return as None")

        # Boolean true -> integer 1
        res = r.eval("return true", 0)
        assert_equal(1, res)

        # Negative integer
        res = r.eval("return -42", 0)
        assert_equal(-42, res)

    results.append(run_test("EVAL_return_types", cat, t_eval_return_types))

    # EVAL_redis_call: Lua calling redis.call('SET', KEYS[1], ARGV[1])
    def t_eval_redis_call():
        k = key(prefix, "lua:adv:call")
        try:
            # SET via redis.call
            res = r.eval(
                "return redis.call('SET', KEYS[1], ARGV[1])", 1, k, "myval"
            )
            assert_true(res is True or str(res) == "OK" or res == "OK",
                        "SET should return OK status")

            # Verify via direct GET
            val = r.get(k)
            assert_equal("myval", val)

            # Multi-command script via redis.call
            res = r.eval(
                "redis.call('SET', KEYS[1], ARGV[1])\n"
                "local v = redis.call('GET', KEYS[1])\n"
                "redis.call('APPEND', KEYS[1], ARGV[2])\n"
                "return redis.call('GET', KEYS[1])",
                1, k, "hello", "_world",
            )
            assert_equal("hello_world", res)
        finally:
            r.delete(k)

    results.append(run_test("EVAL_redis_call", cat, t_eval_redis_call))

    # EVAL_redis_pcall: Lua calling redis.pcall() with error handling
    def t_eval_redis_pcall():
        k = key(prefix, "lua:adv:pcall")
        try:
            r.set(k, "str_val")

            # pcall catches the WRONGTYPE error
            res = r.eval(
                "local ok, err = pcall(redis.call, 'LPUSH', KEYS[1], 'x')\n"
                "if not ok then\n"
                "  return 'error_caught'\n"
                "end\n"
                "return 'no_error'",
                1, k,
            )
            assert_equal("error_caught", res)

            # redis.pcall also catches the error (direct wrapper)
            res = r.eval(
                "local result = redis.pcall('LPUSH', KEYS[1], 'x')\n"
                "if type(result) == 'table' and result.err then\n"
                "  return 'pcall_error: ' .. result.err\n"
                "end\n"
                "return 'no_error'",
                1, k,
            )
            assert_true(
                "pcall_error" in str(res), "pcall should catch error"
            )
        finally:
            r.delete(k)

    results.append(run_test("EVAL_redis_pcall", cat, t_eval_redis_pcall))

    # EVALSHA_cached: SCRIPT LOAD, then EVALSHA, verify execution
    def t_evalsha_cached():
        k = key(prefix, "lua:adv:sha")
        try:
            script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
            sha = r.script_load(script)
            assert_true(len(sha) == 40, "SHA should be 40 hex chars")

            # First call
            res = r.evalsha(sha, 1, k, "cached1")
            assert_equal("cached1", res)

            # Second call with different args (cached script reused)
            res = r.evalsha(sha, 1, k, "cached2")
            assert_equal("cached2", res)

            # EVALSHA with bad SHA should fail with NOSCRIPT
            errored = False
            try:
                r.evalsha("0" * 40, 1, k, "x")
            except redis_lib.exceptions.ResponseError:
                errored = True
            assert_true(errored, "EVALSHA with bad SHA should fail with NOSCRIPT")
        finally:
            r.delete(k)

    results.append(run_test("EVALSHA_cached", cat, t_evalsha_cached))

    # SCRIPT_EXISTS: SCRIPT LOAD, SCRIPT EXISTS with SHA, verify true
    def t_script_exists():
        sha1 = r.script_load("return 'exists_test_1'")
        sha2 = r.script_load("return 'exists_test_2'")
        fake_sha = "0" * 40

        exists = r.script_exists(sha1, sha2, fake_sha)
        assert_true(len(exists) == 3, "should return 3 results")
        assert_true(exists[0], "script1 should exist")
        assert_true(exists[1], "script2 should exist")
        assert_true(not exists[2], "fake SHA should not exist")

    results.append(run_test("SCRIPT_EXISTS", cat, t_script_exists))

    # SCRIPT_FLUSH: Load scripts, SCRIPT FLUSH, verify NOSCRIPT on EVALSHA
    def t_script_flush():
        script = "return 'flush_test'"
        sha = r.script_load(script)

        # Verify script exists
        exists = r.script_exists(sha)
        assert_true(exists[0], "script should exist before flush")

        # Flush
        r.script_flush()

        # Verify NOSCRIPT error on EVALSHA
        errored = False
        err_msg = ""
        try:
            r.evalsha(sha, 0)
        except redis_lib.exceptions.ResponseError as e:
            errored = True
            err_msg = str(e)
        assert_true(errored, "EVALSHA should fail after flush")
        assert_true("NOSCRIPT" in err_msg, "error should be NOSCRIPT")

    results.append(run_test("SCRIPT_FLUSH", cat, t_script_flush))

    # FUNCTION_LOAD: Load Lua function library (Redis 7.0 FUNCTION LOAD)
    def t_function_load():
        # Clean up
        try:
            r.execute_command("FUNCTION", "FLUSH")
        except Exception:
            pass

        code = (
            "#!lua name=testlib\n"
            "redis.register_function('myfunc', function(keys, args) "
            "return 'hello_from_func' end)"
        )
        try:
            res = r.execute_command("FUNCTION", "LOAD", code)
            assert_equal("testlib", res)

            # Verify library appears in FUNCTION LIST
            list_res = r.execute_command("FUNCTION", "LIST")
            assert_true(
                list_res is not None, "FUNCTION LIST should return results"
            )
        finally:
            try:
                r.execute_command("FUNCTION", "FLUSH")
            except Exception:
                pass

    results.append(run_test("FUNCTION_LOAD", cat, t_function_load))

    # FCALL_basic: FCALL loaded function, verify execution
    def t_fcall_basic():
        k = key(prefix, "lua:adv:fcall")
        try:
            # Clean up
            try:
                r.execute_command("FUNCTION", "FLUSH")
            except Exception:
                pass

            # Load function that sets and returns a value
            code = (
                "#!lua name=fcallib\n"
                "redis.register_function('setget', function(keys, args) "
                "redis.call('SET', keys[1], args[1]); "
                "return redis.call('GET', keys[1]) end)"
            )
            r.execute_command("FUNCTION", "LOAD", code)

            # Call the function
            res = r.execute_command("FCALL", "setget", 1, k, "fcall_value")
            assert_equal("fcall_value", res)

            # Verify via direct GET
            val = r.get(k)
            assert_equal("fcall_value", val)
        finally:
            r.delete(k)
            try:
                r.execute_command("FUNCTION", "FLUSH")
            except Exception:
                pass

    results.append(run_test("FCALL_basic", cat, t_fcall_basic))

    # EVAL_KEYS_ARGV: Verify KEYS and ARGV arrays passed correctly
    def t_eval_keys_argv():
        k1 = key(prefix, "lua:adv:k1")
        k2 = key(prefix, "lua:adv:k2")
        k3 = key(prefix, "lua:adv:k3")
        try:
            script = (
                "redis.call('SET', KEYS[1], ARGV[1])\n"
                "redis.call('SET', KEYS[2], ARGV[2])\n"
                "redis.call('SET', KEYS[3], ARGV[3])\n"
                "return {\n"
                "  redis.call('GET', KEYS[1]),\n"
                "  redis.call('GET', KEYS[2]),\n"
                "  redis.call('GET', KEYS[3])\n"
                "}"
            )
            res = r.eval(script, 3, k1, k2, k3, "val1", "val2", "val3")
            assert_true(isinstance(res, list), "should return list")
            assert_true(len(res) == 3, "should return 3 values")
            assert_equal("val1", res[0])
            assert_equal("val2", res[1])
            assert_equal("val3", res[2])
        finally:
            r.delete(k1, k2, k3)

    results.append(run_test("EVAL_KEYS_ARGV", cat, t_eval_keys_argv))

    # EVAL_error_handling: Verify error propagation from Lua to client
    def t_eval_error_handling():
        # Syntax error in Lua
        errored = False
        try:
            r.eval("this is not valid lua", 0)
        except redis_lib.exceptions.ResponseError:
            errored = True
        assert_true(errored, "syntax error should propagate to client")

        # Runtime error via redis.error_reply
        errored = False
        err_msg = ""
        try:
            r.eval("return redis.error_reply('MY_CUSTOM_ERROR')", 0)
        except redis_lib.exceptions.ResponseError as e:
            errored = True
            err_msg = str(e)
        assert_true(errored, "redis.error_reply should propagate as error")
        assert_true(
            "MY_CUSTOM_ERROR" in err_msg, "custom error should propagate"
        )

        # Runtime error via redis.call with wrong args
        errored = False
        try:
            r.eval("return redis.call('SET')", 0)
        except redis_lib.exceptions.ResponseError:
            errored = True
        assert_true(
            errored, "redis.call with wrong args should propagate error"
        )

        # Successful redis.status_reply
        res = r.eval("return redis.status_reply('PONG')", 0)
        assert_true(
            res is True or str(res) == "PONG" or res == "PONG",
            "status_reply should return status",
        )

    results.append(run_test("EVAL_error_handling", cat, t_eval_error_handling))

    return results


register("scripting_advanced", test_scripting_advanced)
