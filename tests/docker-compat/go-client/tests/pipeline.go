package tests

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("pipeline", testPipeline)
}

func testPipeline(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "pipeline"

	results = append(results, RunTest("pipeline_100_SETs", cat, func() error {
		pipe := rdb.Pipeline()
		keys := make([]string, 100)
		for i := 0; i < 100; i++ {
			k := Key(prefix, "pipe:set_"+strconv.Itoa(i))
			keys[i] = k
			pipe.Set(ctx, k, strconv.Itoa(i), 0)
		}
		cmds, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}
		if err := AssertEqual(100, len(cmds)); err != nil {
			return err
		}
		// Cleanup
		for _, k := range keys {
			rdb.Del(ctx, k)
		}
		return nil
	}))

	results = append(results, RunTest("pipeline_100_GETs", cat, func() error {
		keys := make([]string, 100)
		for i := 0; i < 100; i++ {
			k := Key(prefix, "pipe:get_"+strconv.Itoa(i))
			keys[i] = k
			rdb.Set(ctx, k, strconv.Itoa(i), 0)
		}
		defer func() {
			for _, k := range keys {
				rdb.Del(ctx, k)
			}
		}()

		pipe := rdb.Pipeline()
		getCmds := make([]*redis.StringCmd, 100)
		for i, k := range keys {
			getCmds[i] = pipe.Get(ctx, k)
		}
		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}
		for i, cmd := range getCmds {
			val, err := cmd.Result()
			if err != nil {
				return fmt.Errorf("GET %d: %v", i, err)
			}
			if val != strconv.Itoa(i) {
				return fmt.Errorf("GET %d: expected %d, got %s", i, i, val)
			}
		}
		return nil
	}))

	results = append(results, RunTest("pipeline_mixed_commands", cat, func() error {
		k1 := Key(prefix, "pipe:mix1")
		k2 := Key(prefix, "pipe:mix2")
		k3 := Key(prefix, "pipe:mix3")
		defer rdb.Del(ctx, k1, k2, k3)

		pipe := rdb.Pipeline()
		setCmd := pipe.Set(ctx, k1, "hello", 0)
		incrCmd := pipe.Incr(ctx, k2)
		lpushCmd := pipe.LPush(ctx, k3, "a", "b")
		getCmd := pipe.Get(ctx, k1)
		lrangeCmd := pipe.LRange(ctx, k3, 0, -1)
		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}

		if err := AssertNil(setCmd.Err()); err != nil {
			return err
		}
		if v, _ := incrCmd.Result(); v != 1 {
			return fmt.Errorf("INCR expected 1, got %d", v)
		}
		if v, _ := lpushCmd.Result(); v != 2 {
			return fmt.Errorf("LPUSH expected 2, got %d", v)
		}
		if v, _ := getCmd.Result(); v != "hello" {
			return fmt.Errorf("GET expected hello, got %s", v)
		}
		vals, _ := lrangeCmd.Result()
		return AssertStringSliceEqual([]string{"b", "a"}, vals)
	}))

	results = append(results, RunTest("pipeline_with_errors", cat, func() error {
		k := Key(prefix, "pipe:err")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "string_val", 0)

		pipe := rdb.Pipeline()
		getCmd := pipe.Get(ctx, k)
		// This will error because k is a string, not a list
		lpushCmd := pipe.LPush(ctx, k, "bad")
		pipe.Exec(ctx) // may return error

		// GET should succeed
		val, err := getCmd.Result()
		if err != nil {
			return fmt.Errorf("GET should succeed: %v", err)
		}
		if err := AssertEqual("string_val", val); err != nil {
			return err
		}

		// LPUSH should fail with WRONGTYPE
		if lpushCmd.Err() == nil {
			return fmt.Errorf("LPUSH should fail with WRONGTYPE")
		}
		return nil
	}))

	results = append(results, RunTest("pipeline_result_ordering", cat, func() error {
		pipe := rdb.Pipeline()
		cmds := make([]*redis.StringCmd, 10)
		keys := make([]string, 10)
		for i := 0; i < 10; i++ {
			k := Key(prefix, "pipe:order_"+strconv.Itoa(i))
			keys[i] = k
			rdb.Set(ctx, k, fmt.Sprintf("val_%d", i), 0)
			cmds[i] = pipe.Get(ctx, k)
		}
		defer func() {
			for _, k := range keys {
				rdb.Del(ctx, k)
			}
		}()

		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}

		for i, cmd := range cmds {
			val, err := cmd.Result()
			if err != nil {
				return fmt.Errorf("cmd %d: %v", i, err)
			}
			expected := fmt.Sprintf("val_%d", i)
			if val != expected {
				return fmt.Errorf("cmd %d: expected %s, got %s (ordering broken)", i, expected, val)
			}
		}
		return nil
	}))

	return results
}
