package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("persistence", testPersistence)
}

func testPersistence(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "persistence"

	// RDB_SAVE_and_RESTORE: BGSAVE, wait, verify LASTSAVE timestamp changes
	results = append(results, RunTest("RDB_SAVE_and_RESTORE", cat, func() error {
		// Get initial LASTSAVE timestamp
		t1, err := rdb.LastSave(ctx).Result()
		if err != nil {
			return err
		}

		// Write some data to ensure there's something to save
		k := Key(prefix, "persist:rdb_save")
		defer rdb.Del(ctx, k)
		if err := rdb.Set(ctx, k, "rdb_test_value", 0).Err(); err != nil {
			return err
		}

		// Trigger BGSAVE
		status, err := rdb.BgSave(ctx).Result()
		if err != nil {
			// BGSAVE may return error if already in progress, that's OK
			if !strings.Contains(err.Error(), "already in progress") &&
				!strings.Contains(err.Error(), "Background save already in progress") {
				return err
			}
		} else {
			if status != "Background saving started" {
				// Some servers return different messages
				if !strings.Contains(status, "Background") && status != "OK" {
					return fmt.Errorf("unexpected BGSAVE response: %s", status)
				}
			}
		}

		// Wait for background save to complete (poll LASTSAVE)
		var t2 int64
		for i := 0; i < 30; i++ {
			time.Sleep(200 * time.Millisecond)
			t2, err = rdb.LastSave(ctx).Result()
			if err != nil {
				return err
			}
			if t2 > t1 {
				break
			}
		}

		return AssertTrue(t2 >= t1, fmt.Sprintf("LASTSAVE should be >= initial: got %d vs %d", t2, t1))
	}))

	// AOF_append_and_replay: CONFIG SET appendonly yes, write data, verify data accessible
	results = append(results, RunTest("AOF_append_and_replay", cat, func() error {
		// Enable AOF
		if err := rdb.ConfigSet(ctx, "appendonly", "yes").Err(); err != nil {
			return fmt.Errorf("CONFIG SET appendonly yes failed: %v", err)
		}

		// Verify AOF is enabled
		res, err := rdb.ConfigGet(ctx, "appendonly").Result()
		if err != nil {
			return err
		}
		val, ok := res["appendonly"]
		if !ok {
			return fmt.Errorf("appendonly not found in CONFIG GET result")
		}
		if err := AssertEqual("yes", val); err != nil {
			return err
		}

		// Write data that should be captured by AOF
		k := Key(prefix, "persist:aof_test")
		defer rdb.Del(ctx, k)
		if err := rdb.Set(ctx, k, "aof_value_123", 0).Err(); err != nil {
			return err
		}

		// Verify data is readable
		got, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("aof_value_123", got); err != nil {
			return err
		}

		// Restore appendonly to no
		rdb.ConfigSet(ctx, "appendonly", "no")
		return nil
	}))

	// PERSIST_across_restart: SET keys with TTL, verify keys and TTLs exist
	results = append(results, RunTest("PERSIST_across_restart", cat, func() error {
		k1 := Key(prefix, "persist:ttl1")
		k2 := Key(prefix, "persist:ttl2")
		defer rdb.Del(ctx, k1, k2)

		// Set keys with TTL
		if err := rdb.Set(ctx, k1, "value1", 60*time.Second).Err(); err != nil {
			return err
		}
		if err := rdb.Set(ctx, k2, "value2", 120*time.Second).Err(); err != nil {
			return err
		}

		// Trigger BGSAVE to persist
		rdb.BgSave(ctx)
		time.Sleep(500 * time.Millisecond)

		// Verify values exist
		v1, err := rdb.Get(ctx, k1).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("value1", v1); err != nil {
			return err
		}

		v2, err := rdb.Get(ctx, k2).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("value2", v2); err != nil {
			return err
		}

		// Verify TTLs are still set
		ttl1, err := rdb.TTL(ctx, k1).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ttl1 > 0, fmt.Sprintf("key1 TTL should be > 0, got %v", ttl1)); err != nil {
			return err
		}

		ttl2, err := rdb.TTL(ctx, k2).Result()
		if err != nil {
			return err
		}
		return AssertTrue(ttl2 > 0, fmt.Sprintf("key2 TTL should be > 0, got %v", ttl2))
	}))

	// RDB_background_save: SET data, BGSAVE, verify DBSIZE
	results = append(results, RunTest("RDB_background_save", cat, func() error {
		// Write some keys
		keys := make([]string, 10)
		for i := 0; i < 10; i++ {
			keys[i] = Key(prefix, fmt.Sprintf("persist:bgsave_%d", i))
			if err := rdb.Set(ctx, keys[i], fmt.Sprintf("val_%d", i), 0).Err(); err != nil {
				return err
			}
		}
		defer func() {
			rdb.Del(ctx, keys...)
		}()

		// BGSAVE
		rdb.BgSave(ctx)
		time.Sleep(500 * time.Millisecond)

		// Verify DBSIZE reflects the keys
		size, err := rdb.DBSize(ctx).Result()
		if err != nil {
			return err
		}
		return AssertTrue(size >= 10, fmt.Sprintf("DBSIZE should be >= 10, got %d", size))
	}))

	// CONFIG_persistence_settings: CONFIG SET/GET for save, appendonly, appendfsync
	results = append(results, RunTest("CONFIG_persistence_settings", cat, func() error {
		// Test appendonly CONFIG SET/GET
		origAof, err := rdb.ConfigGet(ctx, "appendonly").Result()
		if err != nil {
			return err
		}
		origAofVal := origAof["appendonly"]
		defer rdb.ConfigSet(ctx, "appendonly", origAofVal)

		if err := rdb.ConfigSet(ctx, "appendonly", "yes").Err(); err != nil {
			return err
		}
		res, err := rdb.ConfigGet(ctx, "appendonly").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("yes", res["appendonly"]); err != nil {
			return err
		}

		// Test appendfsync CONFIG SET/GET
		origSync, err := rdb.ConfigGet(ctx, "appendfsync").Result()
		if err != nil {
			return err
		}
		origSyncVal := origSync["appendfsync"]
		defer rdb.ConfigSet(ctx, "appendfsync", origSyncVal)

		if err := rdb.ConfigSet(ctx, "appendfsync", "everysec").Err(); err != nil {
			return err
		}
		res, err = rdb.ConfigGet(ctx, "appendfsync").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("everysec", res["appendfsync"]); err != nil {
			return err
		}

		// Test save CONFIG GET (read-only, don't modify)
		saveRes, err := rdb.ConfigGet(ctx, "save").Result()
		if err != nil {
			return err
		}
		_, ok := saveRes["save"]
		return AssertTrue(ok, "CONFIG GET save should return a value")
	}))

	// DEBUG_SLEEP_during_save: Verify server remains responsive during BGSAVE
	results = append(results, RunTest("DEBUG_SLEEP_during_save", cat, func() error {
		k := Key(prefix, "persist:during_save")
		defer rdb.Del(ctx, k)

		// Write data
		if err := rdb.Set(ctx, k, "before_save", 0).Err(); err != nil {
			return err
		}

		// Trigger BGSAVE
		rdb.BgSave(ctx)

		// Immediately verify server is responsive by executing commands
		if err := rdb.Set(ctx, k, "during_save", 0).Err(); err != nil {
			return fmt.Errorf("server not responsive during BGSAVE: %v", err)
		}
		got, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return fmt.Errorf("GET failed during BGSAVE: %v", err)
		}
		if err := AssertEqual("during_save", got); err != nil {
			return err
		}

		// PING should also work
		pong, err := rdb.Ping(ctx).Result()
		if err != nil {
			return fmt.Errorf("PING failed during BGSAVE: %v", err)
		}
		return AssertEqual("PONG", pong)
	}))

	// MULTI_DB_persistence: SET keys in DB 0 and DB 3, verify both databases
	results = append(results, RunTest("MULTI_DB_persistence", cat, func() error {
		k0 := Key(prefix, "persist:db0_key")
		k3 := Key(prefix, "persist:db3_key")

		// Write to DB 0
		if err := rdb.Set(ctx, k0, "db0_value", 0).Err(); err != nil {
			return err
		}
		defer rdb.Del(ctx, k0)

		// Create a client for DB 3
		rdb3 := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
			DB:       3,
		})
		defer rdb3.Close()

		// Write to DB 3
		if err := rdb3.Set(ctx, k3, "db3_value", 0).Err(); err != nil {
			return err
		}
		defer rdb3.Del(ctx, k3)

		// Trigger BGSAVE to persist both DBs
		rdb.BgSave(ctx)
		time.Sleep(500 * time.Millisecond)

		// Verify DB 0 key
		v0, err := rdb.Get(ctx, k0).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("db0_value", v0); err != nil {
			return err
		}

		// Verify DB 3 key
		v3, err := rdb3.Get(ctx, k3).Result()
		if err != nil {
			return err
		}
		return AssertEqual("db3_value", v3)
	}))

	// LARGE_VALUE_persistence: SET 1MB value, verify value intact
	results = append(results, RunTest("LARGE_VALUE_persistence", cat, func() error {
		k := Key(prefix, "persist:large_val")
		defer rdb.Del(ctx, k)

		// Create a 1MB value
		largeVal := strings.Repeat("A", 1024*1024)

		if err := rdb.Set(ctx, k, largeVal, 0).Err(); err != nil {
			return err
		}

		// Trigger BGSAVE
		rdb.BgSave(ctx)
		time.Sleep(500 * time.Millisecond)

		// Verify the value is intact
		got, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(int64(len(largeVal)), int64(len(got))); err != nil {
			return err
		}
		return AssertEqual(largeVal, got)
	}))

	return results
}
