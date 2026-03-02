package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("edge_cases", testEdgeCases)
}

func testEdgeCases(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "edge_cases"

	results = append(results, RunTest("wrong_type_error", cat, func() error {
		k := Key(prefix, "edge:wrongtype")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "string_value", 0)
		err := rdb.LPush(ctx, k, "bad").Err()
		if err == nil {
			return fmt.Errorf("expected WRONGTYPE error")
		}
		return AssertTrue(strings.Contains(err.Error(), "WRONGTYPE"), "error should be WRONGTYPE: "+err.Error())
	}))

	results = append(results, RunTest("nonexistent_key_GET", cat, func() error {
		_, err := rdb.Get(ctx, Key(prefix, "edge:nonexist")).Result()
		return AssertTrue(err == redis.Nil, "GET nonexistent key should return redis.Nil")
	}))

	results = append(results, RunTest("nonexistent_key_operations", cat, func() error {
		k := Key(prefix, "edge:nonexist_ops")
		// LLEN on nonexistent key should return 0
		n, err := rdb.LLen(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(0, n); err != nil {
			return err
		}
		// HGETALL on nonexistent key should return empty map
		m, err := rdb.HGetAll(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(0, len(m)); err != nil {
			return err
		}
		// SCARD on nonexistent key should return 0
		n, err = rdb.SCard(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(0, n)
	}))

	results = append(results, RunTest("empty_string_value", cat, func() error {
		k := Key(prefix, "edge:empty")
		defer rdb.Del(ctx, k)
		if err := rdb.Set(ctx, k, "", 0).Err(); err != nil {
			return err
		}
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("", val); err != nil {
			return err
		}
		slen, _ := rdb.StrLen(ctx, k).Result()
		return AssertEqualInt64(0, slen)
	}))

	results = append(results, RunTest("binary_data_null_bytes", cat, func() error {
		k := Key(prefix, "edge:binary")
		defer rdb.Del(ctx, k)
		// Value with null bytes
		binVal := "hello\x00world\x00\xff\xfe"
		if err := rdb.Set(ctx, k, binVal, 0).Err(); err != nil {
			return err
		}
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqual(binVal, val)
	}))

	results = append(results, RunTest("large_value_1MB", cat, func() error {
		k := Key(prefix, "edge:large")
		defer rdb.Del(ctx, k)
		largeVal := strings.Repeat("x", 1024*1024) // 1MB
		if err := rdb.Set(ctx, k, largeVal, 0).Err(); err != nil {
			return err
		}
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqual(len(largeVal), len(val))
	}))

	results = append(results, RunTest("unicode_utf8", cat, func() error {
		k := Key(prefix, "edge:utf8")
		defer rdb.Del(ctx, k)
		unicodeVal := "Hello 世界 🌍 Ñoño café"
		if err := rdb.Set(ctx, k, unicodeVal, 0).Err(); err != nil {
			return err
		}
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqual(unicodeVal, val)
	}))

	results = append(results, RunTest("keys_with_special_chars", cat, func() error {
		keys := []string{
			Key(prefix, "edge:space key"),
			Key(prefix, "edge:newline\nkey"),
			Key(prefix, "edge:colon:key"),
			Key(prefix, "edge:tab\tkey"),
		}
		defer func() {
			for _, k := range keys {
				rdb.Del(ctx, k)
			}
		}()
		for i, k := range keys {
			val := fmt.Sprintf("val_%d", i)
			if err := rdb.Set(ctx, k, val, 0).Err(); err != nil {
				return fmt.Errorf("SET key %q: %v", k, err)
			}
			got, err := rdb.Get(ctx, k).Result()
			if err != nil {
				return fmt.Errorf("GET key %q: %v", k, err)
			}
			if got != val {
				return fmt.Errorf("key %q: expected %s, got %s", k, val, got)
			}
		}
		return nil
	}))

	results = append(results, RunTest("negative_zero_overflow_integers", cat, func() error {
		k := Key(prefix, "edge:intops")
		defer rdb.Del(ctx, k)

		// Negative INCRBY
		rdb.Set(ctx, k, "10", 0)
		val, err := rdb.IncrBy(ctx, k, -20).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(-10, val); err != nil {
			return err
		}

		// INCR on zero
		k2 := Key(prefix, "edge:intzero")
		defer rdb.Del(ctx, k2)
		val, err = rdb.Incr(ctx, k2).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, val); err != nil {
			return err
		}

		// INCR on non-numeric should error
		k3 := Key(prefix, "edge:intnan")
		defer rdb.Del(ctx, k3)
		rdb.Set(ctx, k3, "notanumber", 0)
		err = rdb.Incr(ctx, k3).Err()
		return AssertErr(err)
	}))

	results = append(results, RunTest("concurrent_operations", cat, func() error {
		k := Key(prefix, "edge:concurrent")
		defer rdb.Del(ctx, k)
		// Don't pre-set: INCR on nonexistent key starts at 1
		// So N INCRs = N

		var wg sync.WaitGroup
		var successCount int64
		n := 50
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := rdb.Incr(ctx, k).Err(); err == nil {
					atomic.AddInt64(&successCount, 1)
				}
			}()
		}
		wg.Wait()

		if successCount != int64(n) {
			return fmt.Errorf("%d/%d concurrent INCR operations succeeded", successCount, n)
		}
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqual("50", val)
	}))

	return results
}
