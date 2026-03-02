package tests

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("scripting", testScripting)
}

func testScripting(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "scripting"

	results = append(results, RunTest("EVAL_simple", cat, func() error {
		res, err := rdb.Eval(ctx, "return 42", nil).Result()
		if err != nil {
			return err
		}
		return AssertEqual(int64(42), res)
	}))

	results = append(results, RunTest("EVAL_with_keys_and_args", cat, func() error {
		k := Key(prefix, "lua:kv")
		defer rdb.Del(ctx, k)
		script := `
			redis.call('SET', KEYS[1], ARGV[1])
			return redis.call('GET', KEYS[1])
		`
		res, err := rdb.Eval(ctx, script, []string{k}, "lua_value").Result()
		if err != nil {
			return err
		}
		return AssertEqual("lua_value", res)
	}))

	results = append(results, RunTest("EVALSHA_after_SCRIPT_LOAD", cat, func() error {
		k := Key(prefix, "lua:sha")
		defer rdb.Del(ctx, k)
		script := "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
		sha, err := rdb.ScriptLoad(ctx, script).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(len(sha) == 40, "SHA should be 40 hex chars"); err != nil {
			return err
		}
		res, err := rdb.EvalSha(ctx, sha, []string{k}, "sha_value").Result()
		if err != nil {
			return err
		}
		return AssertEqual("sha_value", res)
	}))

	results = append(results, RunTest("script_error_handling", cat, func() error {
		// Script with a runtime error
		_, err := rdb.Eval(ctx, "return redis.call('GET')", nil).Result()
		return AssertErr(err) // should return error
	}))

	results = append(results, RunTest("redis_call_vs_pcall", cat, func() error {
		k := Key(prefix, "lua:pcall")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "string_val", 0)

		// redis.call raises error on WRONGTYPE
		_, err := rdb.Eval(ctx, "return redis.call('LPUSH', KEYS[1], 'bad')", []string{k}).Result()
		if err == nil {
			return fmt.Errorf("redis.call should propagate WRONGTYPE error")
		}

		// redis.pcall returns error as table
		res, err := rdb.Eval(ctx, "local ok, err = pcall(redis.call, 'LPUSH', KEYS[1], 'bad'); if ok then return 'ok' else return 'caught' end", []string{k}).Result()
		if err != nil {
			return err
		}
		return AssertEqual("caught", res)
	}))

	results = append(results, RunTest("SCRIPT_EXISTS_FLUSH", cat, func() error {
		script := "return 'test_exists'"
		sha, err := rdb.ScriptLoad(ctx, script).Result()
		if err != nil {
			return err
		}
		exists, err := rdb.ScriptExists(ctx, sha, "0000000000000000000000000000000000000000").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(exists[0], "loaded script should exist"); err != nil {
			return err
		}
		if err := AssertTrue(!exists[1], "fake SHA should not exist"); err != nil {
			return err
		}
		if err := rdb.ScriptFlush(ctx).Err(); err != nil {
			return err
		}
		exists, _ = rdb.ScriptExists(ctx, sha).Result()
		return AssertTrue(!exists[0], "script should not exist after FLUSH")
	}))

	return results
}
