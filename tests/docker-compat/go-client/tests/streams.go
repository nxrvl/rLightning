package tests

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("streams", testStreams)
}

func testStreams(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "streams"

	results = append(results, RunTest("XADD_auto_ID", cat, func() error {
		k := Key(prefix, "stream:auto")
		defer rdb.Del(ctx, k)
		id, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: k,
			Values: map[string]interface{}{"field": "value1"},
		}).Result()
		if err != nil {
			return err
		}
		// Auto ID should contain "-"
		return AssertTrue(strings.Contains(id, "-"), "auto ID should contain '-': "+id)
	}))

	results = append(results, RunTest("XADD_explicit_ID", cat, func() error {
		k := Key(prefix, "stream:explicit")
		defer rdb.Del(ctx, k)
		id, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: k,
			ID:     "1-1",
			Values: map[string]interface{}{"field": "value"},
		}).Result()
		if err != nil {
			return err
		}
		return AssertEqual("1-1", id)
	}))

	results = append(results, RunTest("XLEN", cat, func() error {
		k := Key(prefix, "stream:len")
		defer rdb.Del(ctx, k)
		for i := 0; i < 5; i++ {
			rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: k,
				Values: map[string]interface{}{"i": i},
			})
		}
		n, err := rdb.XLen(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(5, n)
	}))

	results = append(results, RunTest("XRANGE_XREVRANGE", cat, func() error {
		k := Key(prefix, "stream:range")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"a": "1"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "2-1", Values: map[string]interface{}{"b": "2"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "3-1", Values: map[string]interface{}{"c": "3"}})

		msgs, err := rdb.XRange(ctx, k, "-", "+").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(3, len(msgs)); err != nil {
			return err
		}
		if err := AssertEqual("1-1", msgs[0].ID); err != nil {
			return err
		}

		rmsgs, err := rdb.XRevRange(ctx, k, "+", "-").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(3, len(rmsgs)); err != nil {
			return err
		}
		return AssertEqual("3-1", rmsgs[0].ID)
	}))

	results = append(results, RunTest("XREAD", cat, func() error {
		k := Key(prefix, "stream:read")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"x": "1"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "2-1", Values: map[string]interface{}{"x": "2"}})

		res, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{k, "0"},
			Count:   10,
		}).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(1, len(res)); err != nil {
			return err
		}
		return AssertEqual(2, len(res[0].Messages))
	}))

	results = append(results, RunTest("XINFO_STREAM", cat, func() error {
		k := Key(prefix, "stream:info")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, Values: map[string]interface{}{"f": "v"}})

		info, err := rdb.XInfoStream(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(1, info.Length)
	}))

	results = append(results, RunTest("XTRIM_MAXLEN", cat, func() error {
		k := Key(prefix, "stream:trim")
		defer rdb.Del(ctx, k)
		for i := 0; i < 10; i++ {
			rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, Values: map[string]interface{}{"i": i}})
		}
		n, err := rdb.XTrimMaxLen(ctx, k, 5).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(n >= 0, "XTRIM should return non-negative count"); err != nil {
			return err
		}
		length, _ := rdb.XLen(ctx, k).Result()
		return AssertTrue(length <= 5, "stream should have <= 5 entries after trim")
	}))

	results = append(results, RunTest("XDEL", cat, func() error {
		k := Key(prefix, "stream:del")
		defer rdb.Del(ctx, k)
		id, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, Values: map[string]interface{}{"f": "v"}}).Result()
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, Values: map[string]interface{}{"f2": "v2"}})

		n, err := rdb.XDel(ctx, k, id).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}
		length, _ := rdb.XLen(ctx, k).Result()
		return AssertEqualInt64(1, length)
	}))

	return results
}
