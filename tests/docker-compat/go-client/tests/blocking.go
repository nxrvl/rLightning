package tests

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("blocking", testBlocking)
}

func testBlocking(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "blocking"

	// Helper to create a second connection
	newClient := func() *redis.Client {
		return redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
			DB:       0,
		})
	}

	// BLPOP_with_data: LPUSH then BLPOP, verify immediate return
	results = append(results, RunTest("BLPOP_with_data", cat, func() error {
		k := Key(prefix, "block:blpop_data")
		defer rdb.Del(ctx, k)

		rdb.RPush(ctx, k, "val1")
		vals, err := rdb.BLPop(ctx, time.Second, k).Result()
		if err != nil {
			return fmt.Errorf("BLPOP failed: %v", err)
		}
		if err := AssertEqual(k, vals[0]); err != nil {
			return err
		}
		return AssertEqual("val1", vals[1])
	}))

	// BLPOP_timeout: BLPOP on empty key with 1s timeout, verify timeout return
	results = append(results, RunTest("BLPOP_timeout", cat, func() error {
		k := Key(prefix, "block:blpop_timeout")
		defer rdb.Del(ctx, k)

		start := time.Now()
		_, err := rdb.BLPop(ctx, time.Second, k).Result()
		elapsed := time.Since(start)

		if err != redis.Nil {
			return fmt.Errorf("expected redis.Nil on timeout, got: %v", err)
		}
		// Should have waited approximately 1 second
		if elapsed < 800*time.Millisecond || elapsed > 2500*time.Millisecond {
			return fmt.Errorf("timeout duration out of range: %v", elapsed)
		}
		return nil
	}))

	// BLPOP_multi_key: BLPOP on multiple keys, LPUSH to second key, verify correct key returned
	results = append(results, RunTest("BLPOP_multi_key", cat, func() error {
		k1 := Key(prefix, "block:multi1")
		k2 := Key(prefix, "block:multi2")
		defer rdb.Del(ctx, k1, k2)

		rdb.RPush(ctx, k2, "from_k2")
		vals, err := rdb.BLPop(ctx, time.Second, k1, k2).Result()
		if err != nil {
			return fmt.Errorf("BLPOP multi_key failed: %v", err)
		}
		if err := AssertEqual(k2, vals[0]); err != nil {
			return err
		}
		return AssertEqual("from_k2", vals[1])
	}))

	// BRPOP_basic: Same as BLPOP but from right side
	results = append(results, RunTest("BRPOP_basic", cat, func() error {
		k := Key(prefix, "block:brpop")
		defer rdb.Del(ctx, k)

		rdb.RPush(ctx, k, "first", "second")
		vals, err := rdb.BRPop(ctx, time.Second, k).Result()
		if err != nil {
			return fmt.Errorf("BRPOP failed: %v", err)
		}
		if err := AssertEqual(k, vals[0]); err != nil {
			return err
		}
		return AssertEqual("second", vals[1])
	}))

	// BLMOVE_basic: BLMOVE from source to dest list, verify move
	results = append(results, RunTest("BLMOVE_basic", cat, func() error {
		src := Key(prefix, "block:blmove_src")
		dst := Key(prefix, "block:blmove_dst")
		defer rdb.Del(ctx, src, dst)

		rdb.RPush(ctx, src, "item1")
		val, err := rdb.BLMove(ctx, src, dst, "LEFT", "RIGHT", time.Second).Result()
		if err != nil {
			return fmt.Errorf("BLMOVE failed: %v", err)
		}
		if err := AssertEqual("item1", val); err != nil {
			return err
		}
		// Verify item is in destination
		dstVals, err := rdb.LRange(ctx, dst, 0, -1).Result()
		if err != nil {
			return fmt.Errorf("LRANGE on dest failed: %v", err)
		}
		return AssertStringSliceEqual([]string{"item1"}, dstVals)
	}))

	// BLMPOP_basic: BLMPOP with LEFT/RIGHT direction
	results = append(results, RunTest("BLMPOP_basic", cat, func() error {
		k := Key(prefix, "block:blmpop")
		defer rdb.Del(ctx, k)

		rdb.RPush(ctx, k, "a", "b", "c")
		// BLMPOP timeout numkeys key ... LEFT|RIGHT
		result, err := rdb.Do(ctx, "BLMPOP", "1", "1", k, "LEFT").Result()
		if err != nil {
			return fmt.Errorf("BLMPOP failed: %v", err)
		}
		// Result should be [key, [elements]]
		arr, ok := result.([]interface{})
		if !ok || len(arr) < 2 {
			return fmt.Errorf("unexpected BLMPOP result format: %v", result)
		}
		retKey := fmt.Sprintf("%v", arr[0])
		if err := AssertEqual(k, retKey); err != nil {
			return err
		}
		// Second element should be a list with the popped element
		elemArr, ok := arr[1].([]interface{})
		if !ok || len(elemArr) < 1 {
			return fmt.Errorf("unexpected BLMPOP element format: %v", arr[1])
		}
		retVal := fmt.Sprintf("%v", elemArr[0])
		return AssertEqual("a", retVal)
	}))

	// BZPOPMIN_basic: BZPOPMIN with data and timeout variants
	results = append(results, RunTest("BZPOPMIN_basic", cat, func() error {
		k := Key(prefix, "block:bzpopmin")
		defer rdb.Del(ctx, k)

		rdb.ZAdd(ctx, k, redis.Z{Score: 1.0, Member: "a"}, redis.Z{Score: 2.0, Member: "b"})
		// Use Do() for reliable result parsing
		result, err := rdb.Do(ctx, "BZPOPMIN", k, "1").Result()
		if err != nil {
			return fmt.Errorf("BZPOPMIN failed: %v", err)
		}
		arr, ok := result.([]interface{})
		if !ok || len(arr) < 3 {
			return fmt.Errorf("unexpected BZPOPMIN result: %v", result)
		}
		retKey := fmt.Sprintf("%v", arr[0])
		retMember := fmt.Sprintf("%v", arr[1])
		retScore := fmt.Sprintf("%v", arr[2])
		if err := AssertEqual(k, retKey); err != nil {
			return err
		}
		if err := AssertEqual("a", retMember); err != nil {
			return err
		}
		return AssertEqual("1", retScore)
	}))

	// BZPOPMAX_basic: BZPOPMAX with data and timeout variants
	results = append(results, RunTest("BZPOPMAX_basic", cat, func() error {
		k := Key(prefix, "block:bzpopmax")
		defer rdb.Del(ctx, k)

		rdb.ZAdd(ctx, k, redis.Z{Score: 1.0, Member: "a"}, redis.Z{Score: 2.0, Member: "b"})
		result, err := rdb.Do(ctx, "BZPOPMAX", k, "1").Result()
		if err != nil {
			return fmt.Errorf("BZPOPMAX failed: %v", err)
		}
		arr, ok := result.([]interface{})
		if !ok || len(arr) < 3 {
			return fmt.Errorf("unexpected BZPOPMAX result: %v", result)
		}
		retKey := fmt.Sprintf("%v", arr[0])
		retMember := fmt.Sprintf("%v", arr[1])
		retScore := fmt.Sprintf("%v", arr[2])
		if err := AssertEqual(k, retKey); err != nil {
			return err
		}
		if err := AssertEqual("b", retMember); err != nil {
			return err
		}
		return AssertEqual("2", retScore)
	}))

	// BLOCKING_concurrent: Producer-consumer pattern
	results = append(results, RunTest("BLOCKING_concurrent", cat, func() error {
		k := Key(prefix, "block:concurrent")
		defer rdb.Del(ctx, k)

		client2 := newClient()
		defer client2.Close()

		// Push data after a delay using goroutine
		done := make(chan error, 1)
		go func() {
			time.Sleep(300 * time.Millisecond)
			done <- client2.RPush(ctx, k, "async_val").Err()
		}()

		// BLPOP should block then receive the pushed value
		vals, err := rdb.BLPop(ctx, 5*time.Second, k).Result()
		if err != nil {
			return fmt.Errorf("BLPOP concurrent failed: %v", err)
		}
		if err := AssertEqual("async_val", vals[1]); err != nil {
			return err
		}
		// Wait for producer goroutine to finish
		if pushErr := <-done; pushErr != nil {
			return fmt.Errorf("producer push failed: %v", pushErr)
		}
		return nil
	}))

	// BLOCKING_timeout_accuracy: Verify timeout duration is within acceptable range
	results = append(results, RunTest("BLOCKING_timeout_accuracy", cat, func() error {
		k := Key(prefix, "block:timeout_accuracy")
		defer rdb.Del(ctx, k)

		start := time.Now()
		_, err := rdb.BLPop(ctx, time.Second, k).Result()
		elapsed := time.Since(start)

		if err != redis.Nil {
			return fmt.Errorf("expected timeout, got: %v", err)
		}
		elapsedMs := float64(elapsed.Milliseconds())
		expectedMs := float64(1000)
		diff := math.Abs(elapsedMs - expectedMs)
		// Allow ±500ms tolerance for CI environments
		if diff > 500 {
			return fmt.Errorf("timeout accuracy out of range: expected ~1000ms, got %.0fms (diff: %.0fms)", elapsedMs, diff)
		}
		return nil
	}))

	return results
}
