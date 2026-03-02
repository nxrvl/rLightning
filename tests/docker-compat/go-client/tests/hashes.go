package tests

import (
	"context"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("hashes", testHashes)
}

func testHashes(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "hashes"

	results = append(results, RunTest("HSET_HGET_single", cat, func() error {
		k := Key(prefix, "hash:single")
		defer rdb.Del(ctx, k)
		if err := rdb.HSet(ctx, k, "field1", "value1").Err(); err != nil {
			return err
		}
		val, err := rdb.HGet(ctx, k, "field1").Result()
		if err != nil {
			return err
		}
		return AssertEqual("value1", val)
	}))

	results = append(results, RunTest("HSET_multiple_fields", cat, func() error {
		k := Key(prefix, "hash:multi")
		defer rdb.Del(ctx, k)
		if err := rdb.HSet(ctx, k, "f1", "v1", "f2", "v2", "f3", "v3").Err(); err != nil {
			return err
		}
		v1, _ := rdb.HGet(ctx, k, "f1").Result()
		v2, _ := rdb.HGet(ctx, k, "f2").Result()
		v3, _ := rdb.HGet(ctx, k, "f3").Result()
		if err := AssertEqual("v1", v1); err != nil {
			return err
		}
		if err := AssertEqual("v2", v2); err != nil {
			return err
		}
		return AssertEqual("v3", v3)
	}))

	results = append(results, RunTest("HMSET_HMGET", cat, func() error {
		k := Key(prefix, "hash:hmset")
		defer rdb.Del(ctx, k)
		if err := rdb.HMSet(ctx, k, map[string]interface{}{"a": "1", "b": "2", "c": "3"}).Err(); err != nil {
			return err
		}
		vals, err := rdb.HMGet(ctx, k, "a", "b", "c", "missing").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(4, len(vals)); err != nil {
			return err
		}
		if err := AssertEqual("1", vals[0]); err != nil {
			return err
		}
		if err := AssertEqual("2", vals[1]); err != nil {
			return err
		}
		if err := AssertEqual("3", vals[2]); err != nil {
			return err
		}
		return AssertTrue(vals[3] == nil, "missing field should be nil")
	}))

	results = append(results, RunTest("HGETALL", cat, func() error {
		k := Key(prefix, "hash:getall")
		defer rdb.Del(ctx, k)
		rdb.HSet(ctx, k, "x", "10", "y", "20")
		m, err := rdb.HGetAll(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(2, len(m)); err != nil {
			return err
		}
		if err := AssertEqual("10", m["x"]); err != nil {
			return err
		}
		return AssertEqual("20", m["y"])
	}))

	results = append(results, RunTest("HDEL_HEXISTS", cat, func() error {
		k := Key(prefix, "hash:del")
		defer rdb.Del(ctx, k)
		rdb.HSet(ctx, k, "field", "val")
		exists, _ := rdb.HExists(ctx, k, "field").Result()
		if err := AssertTrue(exists, "field should exist"); err != nil {
			return err
		}
		n, err := rdb.HDel(ctx, k, "field").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}
		exists, _ = rdb.HExists(ctx, k, "field").Result()
		return AssertTrue(!exists, "field should not exist after HDEL")
	}))

	results = append(results, RunTest("HLEN_HKEYS_HVALS", cat, func() error {
		k := Key(prefix, "hash:lkv")
		defer rdb.Del(ctx, k)
		rdb.HSet(ctx, k, "a", "1", "b", "2", "c", "3")
		hlen, err := rdb.HLen(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(3, hlen); err != nil {
			return err
		}
		keys, err := rdb.HKeys(ctx, k).Result()
		if err != nil {
			return err
		}
		sort.Strings(keys)
		if err := AssertStringSliceEqual([]string{"a", "b", "c"}, keys); err != nil {
			return err
		}
		vals, err := rdb.HVals(ctx, k).Result()
		if err != nil {
			return err
		}
		sort.Strings(vals)
		return AssertStringSliceEqual([]string{"1", "2", "3"}, vals)
	}))

	results = append(results, RunTest("HINCRBY_HINCRBYFLOAT", cat, func() error {
		k := Key(prefix, "hash:incr")
		defer rdb.Del(ctx, k)
		rdb.HSet(ctx, k, "counter", "10")
		val, err := rdb.HIncrBy(ctx, k, "counter", 5).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(15, val); err != nil {
			return err
		}
		rdb.HSet(ctx, k, "float_counter", "10.5")
		fval, err := rdb.HIncrByFloat(ctx, k, "float_counter", 0.1).Result()
		if err != nil {
			return err
		}
		return AssertEqualFloat64(10.6, fval, 0.001)
	}))

	results = append(results, RunTest("HSETNX", cat, func() error {
		k := Key(prefix, "hash:setnx")
		defer rdb.Del(ctx, k)
		ok, err := rdb.HSetNX(ctx, k, "field", "first").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "HSETNX should succeed on new field"); err != nil {
			return err
		}
		ok, err = rdb.HSetNX(ctx, k, "field", "second").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(!ok, "HSETNX should fail on existing field"); err != nil {
			return err
		}
		val, _ := rdb.HGet(ctx, k, "field").Result()
		return AssertEqual("first", val)
	}))

	results = append(results, RunTest("HRANDFIELD", cat, func() error {
		k := Key(prefix, "hash:rand")
		defer rdb.Del(ctx, k)
		rdb.HSet(ctx, k, "a", "1", "b", "2", "c", "3")
		field, err := rdb.HRandField(ctx, k, 1).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(len(field) == 1, "should return 1 field"); err != nil {
			return err
		}
		valid := field[0] == "a" || field[0] == "b" || field[0] == "c"
		return AssertTrue(valid, "returned field should be one of a, b, c")
	}))

	results = append(results, RunTest("HSCAN", cat, func() error {
		k := Key(prefix, "hash:scan")
		defer rdb.Del(ctx, k)
		for i := 0; i < 20; i++ {
			rdb.HSet(ctx, k, "field"+strconv.Itoa(i), "val"+strconv.Itoa(i))
		}
		var allKeys []string
		var cursor uint64
		for {
			keys, next, err := rdb.HScan(ctx, k, cursor, "*", 10).Result()
			if err != nil {
				return err
			}
			// HScan returns key-value pairs alternating
			for i := 0; i < len(keys); i += 2 {
				allKeys = append(allKeys, keys[i])
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return AssertTrue(len(allKeys) == 20, "HSCAN should return all 20 fields, got "+strconv.Itoa(len(allKeys)))
	}))

	return results
}
