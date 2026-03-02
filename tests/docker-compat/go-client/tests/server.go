package tests

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("server", testServer)
}

func testServer(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "server"

	results = append(results, RunTest("CONFIG_GET", cat, func() error {
		res, err := rdb.ConfigGet(ctx, "maxmemory").Result()
		if err != nil {
			return err
		}
		return AssertTrue(len(res) > 0, "CONFIG GET should return results")
	}))

	results = append(results, RunTest("CONFIG_SET_and_GET", cat, func() error {
		// Save original value to restore later
		origRes, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
		if err != nil {
			return err
		}
		origVal := origRes["maxmemory-policy"]
		defer rdb.ConfigSet(ctx, "maxmemory-policy", origVal)

		// Set maxmemory-policy and read it back
		if err := rdb.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Err(); err != nil {
			return err
		}
		res, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
		if err != nil {
			return err
		}
		return AssertEqual("allkeys-lru", res["maxmemory-policy"])
	}))

	results = append(results, RunTest("FLUSHDB", cat, func() error {
		// Set some keys first
		k1 := Key(prefix, "srv:flush1")
		k2 := Key(prefix, "srv:flush2")
		rdb.Set(ctx, k1, "a", 0)
		rdb.Set(ctx, k2, "b", 0)

		// Use a dedicated DB to avoid flushing test keys
		rdb2 := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
			DB:       2,
		})
		defer rdb2.Close()

		rdb2.Set(ctx, "flush_test", "val", 0)
		if err := rdb2.FlushDB(ctx).Err(); err != nil {
			return err
		}
		size, _ := rdb2.DBSize(ctx).Result()
		if err := AssertEqualInt64(0, size); err != nil {
			return err
		}

		// Clean up our keys from default DB
		rdb.Del(ctx, k1, k2)
		return nil
	}))

	results = append(results, RunTest("TIME", cat, func() error {
		t, err := rdb.Time(ctx).Result()
		if err != nil {
			return err
		}
		return AssertTrue(!t.IsZero(), "TIME should return a non-zero time")
	}))

	results = append(results, RunTest("RANDOMKEY", cat, func() error {
		// Ensure at least one key exists
		k := Key(prefix, "srv:randkey")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "val", 0)

		rk, err := rdb.RandomKey(ctx).Result()
		if err != nil {
			return err
		}
		return AssertTrue(len(rk) > 0, "RANDOMKEY should return a key name")
	}))

	results = append(results, RunTest("COMMAND_COUNT", cat, func() error {
		n, err := rdb.Do(ctx, "COMMAND", "COUNT").Int64()
		if err != nil {
			return err
		}
		return AssertTrue(n > 50, "COMMAND COUNT should be > 50")
	}))

	results = append(results, RunTest("WAIT_with_timeout", cat, func() error {
		// WAIT 0 replicas with 100ms timeout - should return immediately
		n, err := rdb.Wait(ctx, 0, 100*time.Millisecond).Result()
		if err != nil {
			return err
		}
		return AssertTrue(n >= 0, "WAIT should return >= 0 replicas")
	}))

	return results
}
