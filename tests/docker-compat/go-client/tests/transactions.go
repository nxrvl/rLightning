package tests

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("transactions", testTransactions)
}

func testTransactions(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "transactions"

	results = append(results, RunTest("MULTI_EXEC_basic", cat, func() error {
		k1 := Key(prefix, "tx:basic1")
		k2 := Key(prefix, "tx:basic2")
		defer rdb.Del(ctx, k1, k2)

		pipe := rdb.TxPipeline()
		pipe.Set(ctx, k1, "a", 0)
		pipe.Set(ctx, k2, "b", 0)
		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}

		v1, _ := rdb.Get(ctx, k1).Result()
		v2, _ := rdb.Get(ctx, k2).Result()
		if err := AssertEqual("a", v1); err != nil {
			return err
		}
		return AssertEqual("b", v2)
	}))

	results = append(results, RunTest("MULTI_DISCARD", cat, func() error {
		k := Key(prefix, "tx:discard")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "original", 0)

		// Use raw commands for MULTI/DISCARD
		err := rdb.Do(ctx, "MULTI").Err()
		if err != nil {
			return err
		}
		rdb.Do(ctx, "SET", k, "changed")
		err = rdb.Do(ctx, "DISCARD").Err()
		if err != nil {
			return err
		}

		val, _ := rdb.Get(ctx, k).Result()
		return AssertEqual("original", val)
	}))

	results = append(results, RunTest("WATCH_UNWATCH_optimistic_lock", cat, func() error {
		k := Key(prefix, "tx:watch")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "10", 0)

		// WATCH + no modification = EXEC should succeed
		err := rdb.Watch(ctx, func(tx *redis.Tx) error {
			val, err := tx.Get(ctx, k).Result()
			if err != nil {
				return err
			}
			if val != "10" {
				return fmt.Errorf("unexpected val: %s", val)
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, k, "20", 0)
				return nil
			})
			return err
		}, k)
		if err != nil {
			return err
		}

		val, _ := rdb.Get(ctx, k).Result()
		return AssertEqual("20", val)
	}))

	results = append(results, RunTest("WATCH_key_modification_detection", cat, func() error {
		k := Key(prefix, "tx:watchmod")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "original", 0)

		// Use a second client to modify the key between WATCH and EXEC
		rdb2 := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
		})
		defer rdb2.Close()

		err := rdb.Watch(ctx, func(tx *redis.Tx) error {
			// Another client modifies the watched key
			rdb2.Set(ctx, k, "modified_by_other", 0)

			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, k, "should_fail", 0)
				return nil
			})
			return err
		}, k)

		// Should get TxFailedErr because the key was modified
		if err != redis.TxFailedErr {
			return fmt.Errorf("expected TxFailedErr, got: %v", err)
		}

		val, _ := rdb.Get(ctx, k).Result()
		return AssertEqual("modified_by_other", val)
	}))

	results = append(results, RunTest("queued_command_errors", cat, func() error {
		k := Key(prefix, "tx:qerr")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "string_value", 0)

		pipe := rdb.TxPipeline()
		set := pipe.Set(ctx, k, "new_value", 0)
		// This will fail because k is a string, not a list
		lpush := pipe.LPush(ctx, k, "bad")
		_, err := pipe.Exec(ctx)
		// Exec returns error but individual commands may succeed
		_ = err

		// The SET should have succeeded
		if set.Err() != nil {
			return fmt.Errorf("SET in tx should succeed: %v", set.Err())
		}
		// The LPUSH should have failed with WRONGTYPE
		if lpush.Err() == nil {
			return fmt.Errorf("LPUSH should fail with WRONGTYPE")
		}
		return nil
	}))

	return results
}
