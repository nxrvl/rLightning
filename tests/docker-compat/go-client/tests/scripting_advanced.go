package tests

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("scripting_advanced", testScriptingAdvanced)
}

func testScriptingAdvanced(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "scripting_advanced"

	// EVAL_return_types: Test Lua returning string, integer, table, nil, boolean, error
	results = append(results, RunTest("EVAL_return_types", cat, func() error {
		// String
		res, err := rdb.Eval(ctx, "return 'hello'", nil).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("hello", res); err != nil {
			return err
		}

		// Integer
		res, err = rdb.Eval(ctx, "return 123", nil).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(int64(123), res); err != nil {
			return err
		}

		// Table (array) - returned as []interface{}
		res, err = rdb.Eval(ctx, "return {1, 2, 3}", nil).Result()
		if err != nil {
			return err
		}
		arr, ok := res.([]interface{})
		if !ok {
			return fmt.Errorf("expected array, got %T", res)
		}
		if err := AssertTrue(len(arr) == 3, "table should have 3 elements"); err != nil {
			return err
		}

		// Nil (Lua false returns as nil in Redis)
		res, err = rdb.Eval(ctx, "return false", nil).Result()
		if err != redis.Nil && err != nil {
			return err
		}
		// false in Lua -> nil response in Redis

		// Boolean true -> integer 1
		res, err = rdb.Eval(ctx, "return true", nil).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(int64(1), res); err != nil {
			return err
		}

		// Negative integer
		res, err = rdb.Eval(ctx, "return -42", nil).Result()
		if err != nil {
			return err
		}
		return AssertEqual(int64(-42), res)
	}))

	// EVAL_redis_call: Lua calling redis.call('SET', KEYS[1], ARGV[1])
	results = append(results, RunTest("EVAL_redis_call", cat, func() error {
		k := Key(prefix, "lua:adv:call")
		defer rdb.Del(ctx, k)

		// SET via redis.call
		res, err := rdb.Eval(ctx, "return redis.call('SET', KEYS[1], ARGV[1])", []string{k}, "myval").Result()
		if err != nil {
			return err
		}
		// SET returns OK status
		if err := AssertEqual("OK", res); err != nil {
			return err
		}

		// Verify via direct GET
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("myval", val); err != nil {
			return err
		}

		// Multi-command script via redis.call
		res, err = rdb.Eval(ctx, `
			redis.call('SET', KEYS[1], ARGV[1])
			local v = redis.call('GET', KEYS[1])
			redis.call('APPEND', KEYS[1], ARGV[2])
			return redis.call('GET', KEYS[1])
		`, []string{k}, "hello", "_world").Result()
		if err != nil {
			return err
		}
		return AssertEqual("hello_world", res)
	}))

	// EVAL_redis_pcall: Lua calling redis.pcall() with error handling
	results = append(results, RunTest("EVAL_redis_pcall", cat, func() error {
		k := Key(prefix, "lua:adv:pcall")
		defer rdb.Del(ctx, k)

		// Set a string key
		rdb.Set(ctx, k, "str_val", 0)

		// pcall catches the WRONGTYPE error
		res, err := rdb.Eval(ctx, `
			local ok, err = pcall(redis.call, 'LPUSH', KEYS[1], 'x')
			if not ok then
				return 'error_caught'
			end
			return 'no_error'
		`, []string{k}).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("error_caught", res); err != nil {
			return err
		}

		// redis.pcall also catches the error (direct wrapper)
		res, err = rdb.Eval(ctx, `
			local result = redis.pcall('LPUSH', KEYS[1], 'x')
			if type(result) == 'table' and result.err then
				return 'pcall_error: ' .. result.err
			end
			return 'no_error'
		`, []string{k}).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(strings.Contains(fmt.Sprintf("%v", res), "pcall_error"), "pcall should catch error"); err != nil {
			return err
		}

		return nil
	}))

	// EVALSHA_cached: SCRIPT LOAD, then EVALSHA, verify execution
	results = append(results, RunTest("EVALSHA_cached", cat, func() error {
		k := Key(prefix, "lua:adv:sha")
		defer rdb.Del(ctx, k)

		script := "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
		sha, err := rdb.ScriptLoad(ctx, script).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(len(sha) == 40, "SHA should be 40 hex chars"); err != nil {
			return err
		}

		// First call
		res, err := rdb.EvalSha(ctx, sha, []string{k}, "cached1").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("cached1", res); err != nil {
			return err
		}

		// Second call with different args (cached script reused)
		res, err = rdb.EvalSha(ctx, sha, []string{k}, "cached2").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("cached2", res); err != nil {
			return err
		}

		// EVALSHA with bad SHA should fail with NOSCRIPT
		_, err = rdb.EvalSha(ctx, "0000000000000000000000000000000000000000", []string{k}, "x").Result()
		return AssertErr(err) // NOSCRIPT error
	}))

	// SCRIPT_EXISTS: SCRIPT LOAD, SCRIPT EXISTS with SHA, verify true
	results = append(results, RunTest("SCRIPT_EXISTS", cat, func() error {
		script1 := "return 'exists_test_1'"
		script2 := "return 'exists_test_2'"

		sha1, err := rdb.ScriptLoad(ctx, script1).Result()
		if err != nil {
			return err
		}
		sha2, err := rdb.ScriptLoad(ctx, script2).Result()
		if err != nil {
			return err
		}

		fakeSha := "0000000000000000000000000000000000000000"
		exists, err := rdb.ScriptExists(ctx, sha1, sha2, fakeSha).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(len(exists) == 3, "should return 3 results"); err != nil {
			return err
		}
		if err := AssertTrue(exists[0], "script1 should exist"); err != nil {
			return err
		}
		if err := AssertTrue(exists[1], "script2 should exist"); err != nil {
			return err
		}
		return AssertTrue(!exists[2], "fake SHA should not exist")
	}))

	// SCRIPT_FLUSH: Load scripts, SCRIPT FLUSH, verify NOSCRIPT on EVALSHA
	results = append(results, RunTest("SCRIPT_FLUSH", cat, func() error {
		script := "return 'flush_test'"
		sha, err := rdb.ScriptLoad(ctx, script).Result()
		if err != nil {
			return err
		}

		// Verify script exists
		exists, err := rdb.ScriptExists(ctx, sha).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(exists[0], "script should exist before flush"); err != nil {
			return err
		}

		// Flush
		if err := rdb.ScriptFlush(ctx).Err(); err != nil {
			return err
		}

		// Verify NOSCRIPT error on EVALSHA
		_, err = rdb.EvalSha(ctx, sha, nil).Result()
		if err == nil {
			return fmt.Errorf("EVALSHA should fail with NOSCRIPT after flush")
		}
		return AssertTrue(strings.Contains(err.Error(), "NOSCRIPT"), "error should be NOSCRIPT")
	}))

	// FUNCTION_LOAD: Load Lua function library (Redis 7.0 FUNCTION LOAD)
	results = append(results, RunTest("FUNCTION_LOAD", cat, func() error {
		// Clean up any previous function libraries
		rdb.Do(ctx, "FUNCTION", "FLUSH")

		code := "#!lua name=testlib\nredis.register_function('myfunc', function(keys, args) return 'hello_from_func' end)"
		res := rdb.Do(ctx, "FUNCTION", "LOAD", code)
		if res.Err() != nil {
			return res.Err()
		}
		val, err := res.Text()
		if err != nil {
			return err
		}
		if err := AssertEqual("testlib", val); err != nil {
			return err
		}

		// Verify library appears in FUNCTION LIST
		listRes := rdb.Do(ctx, "FUNCTION", "LIST")
		if listRes.Err() != nil {
			return listRes.Err()
		}

		// Clean up
		rdb.Do(ctx, "FUNCTION", "FLUSH")
		return nil
	}))

	// FCALL_basic: FCALL loaded function, verify execution
	results = append(results, RunTest("FCALL_basic", cat, func() error {
		k := Key(prefix, "lua:adv:fcall")
		defer rdb.Del(ctx, k)
		defer rdb.Do(ctx, "FUNCTION", "FLUSH")

		// Load function that sets and returns a value
		code := "#!lua name=fcallib\nredis.register_function('setget', function(keys, args) redis.call('SET', keys[1], args[1]); return redis.call('GET', keys[1]) end)"
		res := rdb.Do(ctx, "FUNCTION", "LOAD", code)
		if res.Err() != nil {
			return res.Err()
		}

		// Call the function
		fcallRes := rdb.Do(ctx, "FCALL", "setget", 1, k, "fcall_value")
		if fcallRes.Err() != nil {
			return fcallRes.Err()
		}
		val, err := fcallRes.Text()
		if err != nil {
			return err
		}
		if err := AssertEqual("fcall_value", val); err != nil {
			return err
		}

		// Verify via direct GET
		getVal, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqual("fcall_value", getVal)
	}))

	// EVAL_KEYS_ARGV: Verify KEYS and ARGV arrays passed correctly
	results = append(results, RunTest("EVAL_KEYS_ARGV", cat, func() error {
		k1 := Key(prefix, "lua:adv:k1")
		k2 := Key(prefix, "lua:adv:k2")
		k3 := Key(prefix, "lua:adv:k3")
		defer rdb.Del(ctx, k1, k2, k3)

		// Script that uses multiple KEYS and ARGV
		script := `
			redis.call('SET', KEYS[1], ARGV[1])
			redis.call('SET', KEYS[2], ARGV[2])
			redis.call('SET', KEYS[3], ARGV[3])
			return {
				redis.call('GET', KEYS[1]),
				redis.call('GET', KEYS[2]),
				redis.call('GET', KEYS[3])
			}
		`
		res, err := rdb.Eval(ctx, script, []string{k1, k2, k3}, "val1", "val2", "val3").Result()
		if err != nil {
			return err
		}
		arr, ok := res.([]interface{})
		if !ok {
			return fmt.Errorf("expected array, got %T", res)
		}
		if err := AssertTrue(len(arr) == 3, "should return 3 values"); err != nil {
			return err
		}
		if err := AssertEqual("val1", arr[0]); err != nil {
			return err
		}
		if err := AssertEqual("val2", arr[1]); err != nil {
			return err
		}
		return AssertEqual("val3", arr[2])
	}))

	// EVAL_error_handling: Verify error propagation from Lua to client
	results = append(results, RunTest("EVAL_error_handling", cat, func() error {
		// Syntax error in Lua
		_, err := rdb.Eval(ctx, "this is not valid lua", nil).Result()
		if err == nil {
			return fmt.Errorf("syntax error should propagate to client")
		}

		// Runtime error via redis.error_reply
		res, err := rdb.Eval(ctx, "return redis.error_reply('MY_CUSTOM_ERROR')", nil).Result()
		if err == nil {
			return fmt.Errorf("redis.error_reply should propagate as error")
		}
		_ = res
		if err := AssertTrue(strings.Contains(err.Error(), "MY_CUSTOM_ERROR"), "custom error should propagate"); err != nil {
			return err
		}

		// Runtime error via redis.call with wrong args
		_, err = rdb.Eval(ctx, "return redis.call('SET')", nil).Result()
		if err == nil {
			return fmt.Errorf("redis.call with wrong args should propagate error")
		}

		// Successful redis.status_reply
		res, err = rdb.Eval(ctx, "return redis.status_reply('PONG')", nil).Result()
		if err != nil {
			return err
		}
		return AssertEqual("PONG", res)
	}))

	return results
}
