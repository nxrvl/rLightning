package tests

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("lists", testLists)
}

func testLists(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "lists"

	results = append(results, RunTest("LPUSH_RPUSH_single_and_multiple", cat, func() error {
		k := Key(prefix, "list:push")
		defer rdb.Del(ctx, k)
		rdb.LPush(ctx, k, "a")
		rdb.RPush(ctx, k, "b")
		rdb.LPush(ctx, k, "c", "d")  // d, c, a, b
		rdb.RPush(ctx, k, "e", "f")  // d, c, a, b, e, f
		vals, err := rdb.LRange(ctx, k, 0, -1).Result()
		if err != nil {
			return err
		}
		return AssertStringSliceEqual([]string{"d", "c", "a", "b", "e", "f"}, vals)
	}))

	results = append(results, RunTest("LPOP_RPOP", cat, func() error {
		k := Key(prefix, "list:pop")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "c")
		left, err := rdb.LPop(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("a", left); err != nil {
			return err
		}
		right, err := rdb.RPop(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqual("c", right)
	}))

	results = append(results, RunTest("LRANGE", cat, func() error {
		k := Key(prefix, "list:range")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "c", "d", "e")
		vals, err := rdb.LRange(ctx, k, 1, 3).Result()
		if err != nil {
			return err
		}
		return AssertStringSliceEqual([]string{"b", "c", "d"}, vals)
	}))

	results = append(results, RunTest("LLEN", cat, func() error {
		k := Key(prefix, "list:len")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "c")
		n, err := rdb.LLen(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(3, n)
	}))

	results = append(results, RunTest("LINDEX", cat, func() error {
		k := Key(prefix, "list:idx")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "c")
		val, err := rdb.LIndex(ctx, k, 1).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("b", val); err != nil {
			return err
		}
		// Negative index
		val, err = rdb.LIndex(ctx, k, -1).Result()
		if err != nil {
			return err
		}
		return AssertEqual("c", val)
	}))

	results = append(results, RunTest("LSET", cat, func() error {
		k := Key(prefix, "list:set")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "c")
		if err := rdb.LSet(ctx, k, 1, "B").Err(); err != nil {
			return err
		}
		val, _ := rdb.LIndex(ctx, k, 1).Result()
		return AssertEqual("B", val)
	}))

	results = append(results, RunTest("LINSERT_BEFORE_AFTER", cat, func() error {
		k := Key(prefix, "list:ins")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "c")
		rdb.LInsertBefore(ctx, k, "c", "b")  // a, b, c
		rdb.LInsertAfter(ctx, k, "c", "d")   // a, b, c, d
		vals, err := rdb.LRange(ctx, k, 0, -1).Result()
		if err != nil {
			return err
		}
		return AssertStringSliceEqual([]string{"a", "b", "c", "d"}, vals)
	}))

	results = append(results, RunTest("LREM", cat, func() error {
		k := Key(prefix, "list:rem")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "a", "c", "a")
		n, err := rdb.LRem(ctx, k, 2, "a").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(2, n); err != nil {
			return err
		}
		vals, _ := rdb.LRange(ctx, k, 0, -1).Result()
		return AssertStringSliceEqual([]string{"b", "c", "a"}, vals)
	}))

	results = append(results, RunTest("LTRIM", cat, func() error {
		k := Key(prefix, "list:trim")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "c", "d", "e")
		if err := rdb.LTrim(ctx, k, 1, 3).Err(); err != nil {
			return err
		}
		vals, _ := rdb.LRange(ctx, k, 0, -1).Result()
		return AssertStringSliceEqual([]string{"b", "c", "d"}, vals)
	}))

	results = append(results, RunTest("LPOS", cat, func() error {
		k := Key(prefix, "list:pos")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "c", "b", "d")
		pos, err := rdb.LPos(ctx, k, "b", redis.LPosArgs{}).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(1, pos)
	}))

	results = append(results, RunTest("LMPOP", cat, func() error {
		k := Key(prefix, "list:mpop")
		defer rdb.Del(ctx, k)
		rdb.RPush(ctx, k, "a", "b", "c", "d")
		key, vals, err := rdb.LMPop(ctx, "left", 2, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(k, key); err != nil {
			return err
		}
		return AssertStringSliceEqual([]string{"a", "b"}, vals)
	}))

	return results
}
