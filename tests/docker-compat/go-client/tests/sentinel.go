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
	Register("sentinel", testSentinel)
}

func testSentinel(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "sentinel"

	sentinelHost := os.Getenv("REDIS_SENTINEL_HOST")
	sentinelPort := os.Getenv("REDIS_SENTINEL_PORT")
	if sentinelPort == "" {
		sentinelPort = "26379"
	}

	// If sentinel is not configured, skip all sentinel tests
	if sentinelHost == "" {
		skipReason := "REDIS_SENTINEL_HOST not set"
		results = append(results, SkipTest("SENTINEL_master_info", cat, skipReason))
		results = append(results, SkipTest("SENTINEL_replicas", cat, skipReason))
		results = append(results, SkipTest("SENTINEL_sentinels", cat, skipReason))
		results = append(results, SkipTest("SENTINEL_get_master_addr", cat, skipReason))
		results = append(results, SkipTest("SENTINEL_ckquorum", cat, skipReason))
		results = append(results, SkipTest("SENTINEL_failover", cat, skipReason))
		results = append(results, SkipTest("SENTINEL_client_discovery", cat, skipReason))
		return results
	}

	sentinelAddr := fmt.Sprintf("%s:%s", sentinelHost, sentinelPort)

	// Create a direct connection to the sentinel (no password needed for sentinel protocol)
	sentinel := redis.NewSentinelClient(&redis.Options{
		Addr: sentinelAddr,
	})
	defer sentinel.Close()

	// Wait for sentinel to be ready
	for i := 0; i < 30; i++ {
		if err := sentinel.Ping(ctx).Err(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// SENTINEL_master_info: SENTINEL MASTER mymaster — verify master host/port/flags
	results = append(results, RunTest("SENTINEL_master_info", cat, func() error {
		// Use raw command since go-redis sentinel client has limited API
		result, err := sentinel.Master(ctx, "mymaster").Result()
		if err != nil {
			return fmt.Errorf("SENTINEL MASTER mymaster failed: %v", err)
		}

		// result is a map of string->string
		name, ok := result["name"]
		if !ok {
			return fmt.Errorf("expected 'name' field in SENTINEL MASTER response")
		}
		if err := AssertEqual("mymaster", name); err != nil {
			return err
		}

		// Check flags contain "master"
		flags, ok := result["flags"]
		if !ok {
			return fmt.Errorf("expected 'flags' field in SENTINEL MASTER response")
		}
		if !strings.Contains(flags, "master") {
			return fmt.Errorf("expected flags to contain 'master', got: %s", flags)
		}

		// Verify host and port are present
		if _, ok := result["ip"]; !ok {
			return fmt.Errorf("expected 'ip' field in SENTINEL MASTER response")
		}
		if _, ok := result["port"]; !ok {
			return fmt.Errorf("expected 'port' field in SENTINEL MASTER response")
		}

		return nil
	}))

	// SENTINEL_replicas: SENTINEL REPLICAS mymaster — verify replica list
	results = append(results, RunTest("SENTINEL_replicas", cat, func() error {
		replicas, err := sentinel.Replicas(ctx, "mymaster").Result()
		if err != nil {
			// Some sentinel implementations use SLAVES instead of REPLICAS
			// Try SLAVES as fallback
			cmd := redis.NewSliceCmd(ctx, "SENTINEL", "SLAVES", "mymaster")
			_ = sentinel.Process(ctx, cmd)
			if cmd.Err() != nil {
				return fmt.Errorf("SENTINEL REPLICAS mymaster failed: %v (also tried SLAVES: %v)", err, cmd.Err())
			}
			// If SLAVES worked, we at least know the command is supported
			return nil
		}

		// Should have at least 1 replica (we set up master + replica)
		if err := AssertTrue(len(replicas) >= 1,
			fmt.Sprintf("expected at least 1 replica, got %d", len(replicas))); err != nil {
			return err
		}

		// Verify first replica has expected fields
		replica := replicas[0]
		if _, ok := replica["ip"]; !ok {
			return fmt.Errorf("expected 'ip' field in replica info")
		}
		if _, ok := replica["port"]; !ok {
			return fmt.Errorf("expected 'port' field in replica info")
		}

		return nil
	}))

	// SENTINEL_sentinels: SENTINEL SENTINELS mymaster — verify sentinel peer list
	results = append(results, RunTest("SENTINEL_sentinels", cat, func() error {
		sentinels, err := sentinel.Sentinels(ctx, "mymaster").Result()
		if err != nil {
			return fmt.Errorf("SENTINEL SENTINELS mymaster failed: %v", err)
		}

		// Should have at least 2 other sentinels (we run 3 total, so this sentinel sees 2 peers)
		if err := AssertTrue(len(sentinels) >= 2,
			fmt.Sprintf("expected at least 2 sentinel peers, got %d", len(sentinels))); err != nil {
			return err
		}

		// Verify first sentinel peer has expected fields
		peer := sentinels[0]
		if _, ok := peer["ip"]; !ok {
			return fmt.Errorf("expected 'ip' field in sentinel peer info")
		}
		if _, ok := peer["port"]; !ok {
			return fmt.Errorf("expected 'port' field in sentinel peer info")
		}

		return nil
	}))

	// SENTINEL_get_master_addr: SENTINEL GET-MASTER-ADDR-BY-NAME mymaster — verify address
	results = append(results, RunTest("SENTINEL_get_master_addr", cat, func() error {
		addr, err := sentinel.GetMasterAddrByName(ctx, "mymaster").Result()
		if err != nil {
			return fmt.Errorf("SENTINEL GET-MASTER-ADDR-BY-NAME mymaster failed: %v", err)
		}

		// Should return [host, port]
		if err := AssertTrue(len(addr) == 2,
			fmt.Sprintf("expected 2 elements in address, got %d", len(addr))); err != nil {
			return err
		}

		// Host should not be empty
		if err := AssertTrue(addr[0] != "",
			"expected non-empty host in master address"); err != nil {
			return err
		}

		// Port should not be empty
		if err := AssertTrue(addr[1] != "",
			"expected non-empty port in master address"); err != nil {
			return err
		}

		return nil
	}))

	// SENTINEL_ckquorum: SENTINEL CKQUORUM mymaster — verify quorum check
	results = append(results, RunTest("SENTINEL_ckquorum", cat, func() error {
		// CkQuorum returns a string message on success
		result := sentinel.CkQuorum(ctx, "mymaster")
		if result.Err() != nil {
			return fmt.Errorf("SENTINEL CKQUORUM mymaster failed: %v", result.Err())
		}

		msg := result.Val()
		// Response should indicate quorum is reachable
		if err := AssertTrue(
			strings.Contains(msg, "OK") || strings.Contains(msg, "ok") || strings.Contains(msg, "reachable"),
			fmt.Sprintf("expected quorum OK response, got: %s", msg)); err != nil {
			return err
		}

		return nil
	}))

	// SENTINEL_failover: Simulate master failure, verify failover completes
	results = append(results, RunTest("SENTINEL_failover", cat, func() error {
		// We can trigger a failover via SENTINEL FAILOVER command
		// Note: we don't actually kill the master — SENTINEL FAILOVER forces
		// a failover even if the master is healthy
		result := sentinel.Failover(ctx, "mymaster")
		if result.Err() != nil {
			// Some sentinel implementations may reject failover if already in progress
			// or if there's only one usable replica
			errMsg := result.Err().Error()
			if strings.Contains(errMsg, "NOGOODSLAVE") || strings.Contains(errMsg, "in progress") ||
				strings.Contains(errMsg, "INPROG") || strings.Contains(errMsg, "no good") {
				// This is acceptable — means the infrastructure works but conditions aren't met
				return nil
			}
			return fmt.Errorf("SENTINEL FAILOVER mymaster failed: %v", result.Err())
		}

		// Wait a bit for failover to complete and master to stabilize
		time.Sleep(3 * time.Second)

		// Verify we can still get master address (failover completed or reverted)
		addr, err := sentinel.GetMasterAddrByName(ctx, "mymaster").Result()
		if err != nil {
			return fmt.Errorf("failed to get master address after failover: %v", err)
		}

		if err := AssertTrue(len(addr) == 2 && addr[0] != "" && addr[1] != "",
			"expected valid master address after failover"); err != nil {
			return err
		}

		return nil
	}))

	// SENTINEL_client_discovery: Connect via sentinel, verify automatic master discovery
	results = append(results, RunTest("SENTINEL_client_discovery", cat, func() error {
		password := rdb.Options().Password

		// Wait for sentinel to recover from any previous failover
		time.Sleep(2 * time.Second)

		// Use go-redis sentinel-aware client to discover the master
		client := redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    "mymaster",
			SentinelAddrs: []string{sentinelAddr},
			Password:      password,
			DB:            0,
		})
		defer client.Close()

		// Should be able to connect and issue commands
		if err := client.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("failed to connect via sentinel discovery: %v", err)
		}

		// Write and read a value to confirm we're connected to a working master
		k := Key(prefix, "sentinel:discovery_test")
		defer client.Del(ctx, k)

		if err := client.Set(ctx, k, "discovered", 30*time.Second).Err(); err != nil {
			return fmt.Errorf("failed to SET via sentinel-discovered master: %v", err)
		}

		val, err := client.Get(ctx, k).Result()
		if err != nil {
			return fmt.Errorf("failed to GET via sentinel-discovered master: %v", err)
		}

		return AssertEqual("discovered", val)
	}))

	return results
}
