package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("replication", testReplication)
}

func testReplication(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "replication"

	replicaHost := os.Getenv("REDIS_REPLICA_HOST")
	replicaPort := os.Getenv("REDIS_REPLICA_PORT")
	if replicaPort == "" {
		replicaPort = "6379"
	}

	// If replica is not configured, skip all replication tests
	if replicaHost == "" {
		skipReason := "REDIS_REPLICA_HOST not set"
		results = append(results, SkipTest("INFO_replication", cat, skipReason))
		results = append(results, SkipTest("REPLICAOF_basic", cat, skipReason))
		results = append(results, SkipTest("REPLICATION_data_sync", cat, skipReason))
		results = append(results, SkipTest("REPLICATION_multi_commands", cat, skipReason))
		results = append(results, SkipTest("REPLICATION_expiry", cat, skipReason))
		results = append(results, SkipTest("REPLICA_read_only", cat, skipReason))
		results = append(results, SkipTest("REPLICATION_after_reconnect", cat, skipReason))
		results = append(results, SkipTest("WAIT_command", cat, skipReason))
		results = append(results, SkipTest("REPLICATION_SELECT", cat, skipReason))
		return results
	}

	replicaAddr := fmt.Sprintf("%s:%s", replicaHost, replicaPort)
	password := rdb.Options().Password

	newReplicaClient := func(db int) *redis.Client {
		return redis.NewClient(&redis.Options{
			Addr:     replicaAddr,
			Password: password,
			DB:       db,
		})
	}

	// Wait for replica to be connected before running tests
	replica := newReplicaClient(0)
	defer replica.Close()

	// Give replication time to establish
	for i := 0; i < 30; i++ {
		if err := replica.Ping(ctx).Err(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Helper: wait for a key to appear on the replica
	waitForKey := func(r *redis.Client, key string, maxWait time.Duration) (string, error) {
		deadline := time.Now().Add(maxWait)
		for time.Now().Before(deadline) {
			val, err := r.Get(ctx, key).Result()
			if err == nil {
				return val, nil
			}
			if err != redis.Nil {
				return "", err
			}
			time.Sleep(100 * time.Millisecond)
		}
		return "", fmt.Errorf("key %s not found on replica after %v", key, maxWait)
	}

	// INFO_replication: Verify INFO replication section on master
	results = append(results, RunTest("INFO_replication", cat, func() error {
		info, err := rdb.Info(ctx, "replication").Result()
		if err != nil {
			return err
		}

		if !strings.Contains(info, "role:master") {
			return fmt.Errorf("expected role:master in INFO replication, got: %s", info)
		}

		// Verify connected_slaves >= 1
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "connected_slaves:") {
				countStr := strings.TrimPrefix(line, "connected_slaves:")
				var count int
				fmt.Sscanf(countStr, "%d", &count)
				return AssertTrue(count >= 1, fmt.Sprintf("connected_slaves should be >= 1, got %d", count))
			}
		}
		return fmt.Errorf("connected_slaves not found in INFO replication")
	}))

	// REPLICAOF_basic: Verify replica is connected to master
	results = append(results, RunTest("REPLICAOF_basic", cat, func() error {
		info, err := replica.Info(ctx, "replication").Result()
		if err != nil {
			return err
		}

		if !strings.Contains(info, "role:slave") {
			return fmt.Errorf("expected role:slave in replica INFO replication, got: %s", info)
		}

		if !strings.Contains(info, "master_link_status:up") {
			return fmt.Errorf("expected master_link_status:up in replica INFO replication")
		}

		return nil
	}))

	// REPLICATION_data_sync: SET key on master, verify key appears on replica
	results = append(results, RunTest("REPLICATION_data_sync", cat, func() error {
		k := Key(prefix, "repl:data_sync")
		defer rdb.Del(ctx, k)

		if err := rdb.Set(ctx, k, "replicated_value", 0).Err(); err != nil {
			return err
		}

		val, err := waitForKey(replica, k, 5*time.Second)
		if err != nil {
			return err
		}
		return AssertEqual("replicated_value", val)
	}))

	// REPLICATION_multi_commands: SET multiple keys on master, verify all replicated
	results = append(results, RunTest("REPLICATION_multi_commands", cat, func() error {
		keys := make([]string, 10)
		for i := 0; i < 10; i++ {
			keys[i] = Key(prefix, fmt.Sprintf("repl:multi_%d", i))
			if err := rdb.Set(ctx, keys[i], fmt.Sprintf("value_%d", i), 0).Err(); err != nil {
				return err
			}
		}
		defer func() {
			rdb.Del(ctx, keys...)
		}()

		// Wait for last key to appear on replica
		if _, err := waitForKey(replica, keys[9], 5*time.Second); err != nil {
			return err
		}

		// Verify all keys on replica
		for i := 0; i < 10; i++ {
			val, err := replica.Get(ctx, keys[i]).Result()
			if err != nil {
				return fmt.Errorf("key %s not found on replica: %v", keys[i], err)
			}
			if err := AssertEqual(fmt.Sprintf("value_%d", i), val); err != nil {
				return err
			}
		}
		return nil
	}))

	// REPLICATION_expiry: SET key with TTL on master, verify TTL replicated
	results = append(results, RunTest("REPLICATION_expiry", cat, func() error {
		k := Key(prefix, "repl:expiry")
		defer rdb.Del(ctx, k)

		if err := rdb.Set(ctx, k, "ttl_value", 120*time.Second).Err(); err != nil {
			return err
		}

		// Wait for key to appear on replica
		if _, err := waitForKey(replica, k, 5*time.Second); err != nil {
			return err
		}

		// Verify TTL on replica
		ttl, err := replica.TTL(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(ttl > 0, fmt.Sprintf("replica TTL should be > 0, got %v", ttl))
	}))

	// REPLICA_read_only: Verify writes to replica return READONLY error
	results = append(results, RunTest("REPLICA_read_only", cat, func() error {
		k := Key(prefix, "repl:readonly_test")
		err := replica.Set(ctx, k, "should_fail", 0).Err()
		if err == nil {
			// Clean up if it somehow succeeded
			replica.Del(ctx, k)
			return fmt.Errorf("expected READONLY error, but SET succeeded on replica")
		}
		return AssertTrue(
			strings.Contains(err.Error(), "READONLY") || strings.Contains(err.Error(), "readonly"),
			fmt.Sprintf("expected READONLY error, got: %v", err),
		)
	}))

	// REPLICATION_after_reconnect: Disconnect replica, write to master, reconnect, verify sync
	results = append(results, RunTest("REPLICATION_after_reconnect", cat, func() error {
		// Get master host/port from replica's INFO
		info, err := replica.Info(ctx, "replication").Result()
		if err != nil {
			return err
		}

		var masterHost, masterPort string
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "master_host:") {
				masterHost = strings.TrimPrefix(line, "master_host:")
			}
			if strings.HasPrefix(line, "master_port:") {
				masterPort = strings.TrimPrefix(line, "master_port:")
			}
		}
		if masterHost == "" || masterPort == "" {
			return fmt.Errorf("could not determine master host/port from replica INFO")
		}

		// Disconnect replica
		res := replica.Do(ctx, "REPLICAOF", "NO", "ONE")
		if res.Err() != nil {
			return fmt.Errorf("REPLICAOF NO ONE failed: %v", res.Err())
		}

		// Write data on master while replica is disconnected
		k := Key(prefix, "repl:reconnect_test")
		defer rdb.Del(ctx, k)
		if err := rdb.Set(ctx, k, "after_disconnect", 0).Err(); err != nil {
			return err
		}

		// Reconnect replica to master
		res = replica.Do(ctx, "REPLICAOF", masterHost, masterPort)
		if res.Err() != nil {
			return fmt.Errorf("REPLICAOF reconnect failed: %v", res.Err())
		}

		// Wait for sync and key to appear
		val, err := waitForKey(replica, k, 10*time.Second)
		if err != nil {
			return err
		}
		return AssertEqual("after_disconnect", val)
	}))

	// WAIT_command: SET key, WAIT 1 0, verify acknowledged
	results = append(results, RunTest("WAIT_command", cat, func() error {
		k := Key(prefix, "repl:wait_test")
		defer rdb.Del(ctx, k)

		if err := rdb.Set(ctx, k, "wait_value", 0).Err(); err != nil {
			return err
		}

		// WAIT for 1 replica with 5 second timeout
		numReplicas, err := rdb.Wait(ctx, 1, 5*time.Second).Result()
		if err != nil {
			return fmt.Errorf("WAIT failed: %v", err)
		}
		return AssertTrue(numReplicas >= 1, fmt.Sprintf("WAIT should return >= 1, got %d", numReplicas))
	}))

	// REPLICATION_SELECT: Master uses SELECT 3 + SET, verify replica DB 3 has key
	results = append(results, RunTest("REPLICATION_SELECT", cat, func() error {
		k := Key(prefix, "repl:select_db3")

		// Create a client for master DB 3
		masterDB3 := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
			DB:       3,
		})
		defer masterDB3.Close()

		if err := masterDB3.Set(ctx, k, "db3_replicated", 0).Err(); err != nil {
			return err
		}
		defer masterDB3.Del(ctx, k)

		// Create a client for replica DB 3
		replicaDB3 := newReplicaClient(3)
		defer replicaDB3.Close()

		// Wait for key to appear on replica DB 3
		val, err := waitForKey(replicaDB3, k, 5*time.Second)
		if err != nil {
			return err
		}
		return AssertEqual("db3_replicated", val)
	}))

	return results
}
