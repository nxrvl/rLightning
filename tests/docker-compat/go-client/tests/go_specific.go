package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("go_specific", testGoSpecific)
}

func testGoSpecific(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "go_specific"

	results = append(results, RunTest("context_cancellation", cat, func() error {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // immediately cancel

		err := rdb.Set(cancelCtx, Key(prefix, "gospec:cancel"), "val", 0).Err()
		if err == nil {
			return fmt.Errorf("expected error with cancelled context")
		}
		return nil
	}))

	results = append(results, RunTest("context_timeout", cat, func() error {
		timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
		defer cancel()

		time.Sleep(1 * time.Millisecond) // ensure timeout fires

		err := rdb.Set(timeoutCtx, Key(prefix, "gospec:timeout"), "val", 0).Err()
		if err == nil {
			return fmt.Errorf("expected error with timed out context")
		}
		return nil
	}))

	results = append(results, RunTest("connection_pool_basics", cat, func() error {
		// Create client with specific pool size
		poolClient := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
			PoolSize: 5,
		})
		defer poolClient.Close()

		// Run several commands and check pool stats
		for i := 0; i < 10; i++ {
			k := Key(prefix, fmt.Sprintf("gospec:pool_%d", i))
			defer poolClient.Del(ctx, k)
			if err := poolClient.Set(ctx, k, i, 0).Err(); err != nil {
				return fmt.Errorf("SET %d: %v", i, err)
			}
		}

		stats := poolClient.PoolStats()
		return AssertTrue(stats.TotalConns > 0, fmt.Sprintf("pool should have connections, got %d", stats.TotalConns))
	}))

	results = append(results, RunTest("cmdable_interface_compliance", cat, func() error {
		// Verify that *redis.Client satisfies the Cmdable interface
		var c redis.Cmdable = rdb
		k := Key(prefix, "gospec:cmdable")
		defer rdb.Del(ctx, k)

		if err := c.Set(ctx, k, "cmdable_val", 0).Err(); err != nil {
			return err
		}
		val, err := c.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqual("cmdable_val", val)
	}))

	results = append(results, RunTest("pipeline_vs_txpipeline", cat, func() error {
		k := Key(prefix, "gospec:pipetx")
		defer rdb.Del(ctx, k)

		// Regular pipeline (no MULTI/EXEC)
		pipe := rdb.Pipeline()
		pipe.Set(ctx, k, "pipe_val", 0)
		getCmd := pipe.Get(ctx, k)
		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}
		val, _ := getCmd.Result()
		if err := AssertEqual("pipe_val", val); err != nil {
			return err
		}

		// TxPipeline (wraps in MULTI/EXEC)
		txPipe := rdb.TxPipeline()
		txPipe.Set(ctx, k, "tx_val", 0)
		txGetCmd := txPipe.Get(ctx, k)
		_, err = txPipe.Exec(ctx)
		if err != nil {
			return err
		}
		val, _ = txGetCmd.Result()
		return AssertEqual("tx_val", val)
	}))

	return results
}
