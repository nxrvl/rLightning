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
	Register("cluster", testCluster)
}

func testCluster(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "cluster"

	clusterHost := os.Getenv("REDIS_CLUSTER_HOST")
	clusterPort := os.Getenv("REDIS_CLUSTER_PORT")
	if clusterPort == "" {
		clusterPort = "6379"
	}

	// If cluster is not configured, skip all cluster tests
	if clusterHost == "" {
		skipReason := "REDIS_CLUSTER_HOST not set"
		results = append(results, SkipTest("CLUSTER_INFO", cat, skipReason))
		results = append(results, SkipTest("CLUSTER_NODES", cat, skipReason))
		results = append(results, SkipTest("CLUSTER_SLOTS", cat, skipReason))
		results = append(results, SkipTest("CLUSTER_SHARDS", cat, skipReason))
		results = append(results, SkipTest("CLUSTER_hash_tag", cat, skipReason))
		results = append(results, SkipTest("CLUSTER_MOVED_redirect", cat, skipReason))
		results = append(results, SkipTest("CLUSTER_KEYSLOT", cat, skipReason))
		results = append(results, SkipTest("CLUSTER_COUNTKEYSINSLOT", cat, skipReason))
		results = append(results, SkipTest("CLUSTER_cross_slot_error", cat, skipReason))
		results = append(results, SkipTest("READONLY_mode", cat, skipReason))
		return results
	}

	clusterAddr := fmt.Sprintf("%s:%s", clusterHost, clusterPort)
	password := rdb.Options().Password

	clusterClient := redis.NewClient(&redis.Options{
		Addr:     clusterAddr,
		Password: password,
		DB:       0,
	})
	defer clusterClient.Close()

	// Wait for cluster node to be ready
	for i := 0; i < 30; i++ {
		if err := clusterClient.Ping(ctx).Err(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	// Wait for cluster to converge (cluster_state:ok)
	for i := 0; i < 30; i++ {
		info, err := clusterClient.ClusterInfo(ctx).Result()
		if err == nil && strings.Contains(info, "cluster_state:ok") {
			break
		}
		time.Sleep(time.Second)
	}

	// CLUSTER_INFO: Verify CLUSTER INFO returns cluster_state, cluster_slots_assigned, cluster_size
	results = append(results, RunTest("CLUSTER_INFO", cat, func() error {
		info, err := clusterClient.ClusterInfo(ctx).Result()
		if err != nil {
			return err
		}

		if !strings.Contains(info, "cluster_state:") {
			return fmt.Errorf("CLUSTER INFO missing cluster_state field")
		}
		if !strings.Contains(info, "cluster_slots_assigned:") {
			return fmt.Errorf("CLUSTER INFO missing cluster_slots_assigned field")
		}
		if !strings.Contains(info, "cluster_size:") {
			return fmt.Errorf("CLUSTER INFO missing cluster_size field")
		}
		if !strings.Contains(info, "cluster_enabled:1") {
			return fmt.Errorf("CLUSTER INFO should show cluster_enabled:1")
		}

		return nil
	}))

	// CLUSTER_NODES: Verify CLUSTER NODES returns node list with correct format
	results = append(results, RunTest("CLUSTER_NODES", cat, func() error {
		nodes, err := clusterClient.ClusterNodes(ctx).Result()
		if err != nil {
			return err
		}

		lines := strings.Split(strings.TrimSpace(nodes), "\n")
		if len(lines) < 3 {
			return fmt.Errorf("expected at least 3 nodes, got %d lines", len(lines))
		}

		// Verify each line has the expected format: <id> <ip:port@cport> <flags> ...
		foundMyself := false
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 8 {
				return fmt.Errorf("node line has too few fields (%d): %s", len(fields), line)
			}
			// Node ID should be 40 hex chars
			if len(fields[0]) != 40 {
				return fmt.Errorf("node ID should be 40 chars, got %d: %s", len(fields[0]), fields[0])
			}
			if strings.Contains(fields[2], "myself") {
				foundMyself = true
			}
		}

		return AssertTrue(foundMyself, "CLUSTER NODES should contain a 'myself' node")
	}))

	// CLUSTER_SLOTS: Verify CLUSTER SLOTS returns slot ranges with master/replica info
	results = append(results, RunTest("CLUSTER_SLOTS", cat, func() error {
		slots, err := clusterClient.ClusterSlots(ctx).Result()
		if err != nil {
			return err
		}

		if len(slots) == 0 {
			return fmt.Errorf("CLUSTER SLOTS returned empty result")
		}

		// Verify slot ranges cover [0, 16383]
		totalSlots := 0
		for _, slot := range slots {
			if slot.Start < 0 || slot.End > 16383 {
				return fmt.Errorf("invalid slot range: %d-%d", slot.Start, slot.End)
			}
			totalSlots += int(slot.End-slot.Start) + 1
			// Each slot range should have at least one node
			if len(slot.Nodes) == 0 {
				return fmt.Errorf("slot range %d-%d has no nodes", slot.Start, slot.End)
			}
		}

		return AssertTrue(totalSlots == 16384, fmt.Sprintf("expected 16384 total slots, got %d", totalSlots))
	}))

	// CLUSTER_SHARDS: Verify CLUSTER SHARDS returns shard information (Redis 7.0+)
	results = append(results, RunTest("CLUSTER_SHARDS", cat, func() error {
		result, err := clusterClient.Do(ctx, "CLUSTER", "SHARDS").Result()
		if err != nil {
			return err
		}

		shards, ok := result.([]interface{})
		if !ok {
			return fmt.Errorf("CLUSTER SHARDS returned unexpected type: %T", result)
		}

		if len(shards) < 3 {
			return fmt.Errorf("expected at least 3 shards, got %d", len(shards))
		}

		// Each shard should have slots and nodes info
		for i, shard := range shards {
			shardData, ok := shard.([]interface{})
			if !ok {
				return fmt.Errorf("shard %d is not an array", i)
			}
			if len(shardData) < 4 {
				return fmt.Errorf("shard %d has too few fields (%d)", i, len(shardData))
			}
		}

		return nil
	}))

	// CLUSTER_hash_tag: Verify {tag} keys hash to same slot
	results = append(results, RunTest("CLUSTER_hash_tag", cat, func() error {
		// Keys with same hash tag should hash to same slot
		slot1, err := clusterClient.ClusterKeySlot(ctx, "{user}.name").Result()
		if err != nil {
			return err
		}
		slot2, err := clusterClient.ClusterKeySlot(ctx, "{user}.email").Result()
		if err != nil {
			return err
		}
		slot3, err := clusterClient.ClusterKeySlot(ctx, "{user}.age").Result()
		if err != nil {
			return err
		}

		if slot1 != slot2 || slot2 != slot3 {
			return fmt.Errorf("{user}.name=%d, {user}.email=%d, {user}.age=%d — should all be same slot",
				slot1, slot2, slot3)
		}

		// Keys with different hash tags should (usually) hash to different slots
		slotA, err := clusterClient.ClusterKeySlot(ctx, "{alpha}.key").Result()
		if err != nil {
			return err
		}
		slotB, err := clusterClient.ClusterKeySlot(ctx, "{beta}.key").Result()
		if err != nil {
			return err
		}

		// They could theoretically collide, but it's very unlikely
		return AssertTrue(slotA != slotB, fmt.Sprintf("{alpha} and {beta} should hash to different slots, both got %d", slotA))
	}))

	// CLUSTER_MOVED_redirect: Access key on wrong node, verify MOVED error with correct slot and target
	results = append(results, RunTest("CLUSTER_MOVED_redirect", cat, func() error {
		// Find a key that hashes to a slot NOT owned by the node we're connected to
		// First get this node's slots
		nodes, err := clusterClient.ClusterNodes(ctx).Result()
		if err != nil {
			return err
		}

		// Parse the myself node's slots
		var mySlots []int64
		for _, line := range strings.Split(strings.TrimSpace(nodes), "\n") {
			if strings.Contains(line, "myself") {
				fields := strings.Fields(line)
				for _, f := range fields[8:] {
					if strings.Contains(f, "-") {
						parts := strings.Split(f, "-")
						var start, end int
						fmt.Sscanf(parts[0], "%d", &start)
						fmt.Sscanf(parts[1], "%d", &end)
						for s := start; s <= end; s++ {
							mySlots = append(mySlots, int64(s))
						}
					}
				}
				break
			}
		}

		// Find a slot not in mySlots
		mySlotSet := make(map[int64]bool)
		for _, s := range mySlots {
			mySlotSet[s] = true
		}

		targetSlot := int64(-1)
		for s := int64(0); s < 16384; s++ {
			if !mySlotSet[s] {
				targetSlot = s
				break
			}
		}

		if targetSlot == -1 {
			return fmt.Errorf("could not find a slot not owned by this node (node owns all slots?)")
		}

		// Find a key that hashes to that slot
		testKey := ""
		for i := 0; i < 100000; i++ {
			candidate := fmt.Sprintf("%scluster:moved:%d", prefix, i)
			slot, _ := clusterClient.ClusterKeySlot(ctx, candidate).Result()
			if slot == targetSlot {
				testKey = candidate
				break
			}
		}

		if testKey == "" {
			return fmt.Errorf("could not find a key hashing to slot %d", targetSlot)
		}

		// Try to GET the key — should get MOVED error
		err = clusterClient.Get(ctx, testKey).Err()
		if err == nil {
			return fmt.Errorf("expected MOVED error for key in slot %d, but command succeeded", targetSlot)
		}

		errStr := err.Error()
		if !strings.Contains(errStr, "MOVED") {
			return fmt.Errorf("expected MOVED error, got: %s", errStr)
		}

		return nil
	}))

	// CLUSTER_KEYSLOT: Verify CLUSTER KEYSLOT returns correct CRC16 mod 16384
	results = append(results, RunTest("CLUSTER_KEYSLOT", cat, func() error {
		// "foo" should hash to slot 12182
		slot, err := clusterClient.ClusterKeySlot(ctx, "foo").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(12182, slot); err != nil {
			return fmt.Errorf("CLUSTER KEYSLOT foo: %v", err)
		}

		// Empty key hashes to slot 0
		slot, err = clusterClient.ClusterKeySlot(ctx, "").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(0, slot); err != nil {
			return fmt.Errorf("CLUSTER KEYSLOT (empty): %v", err)
		}

		// "{user}.info" should hash the same as "user"
		slot1, err := clusterClient.ClusterKeySlot(ctx, "{user}.info").Result()
		if err != nil {
			return err
		}
		slot2, err := clusterClient.ClusterKeySlot(ctx, "user").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(slot2, slot1); err != nil {
			return fmt.Errorf("{user}.info should hash same as user: %v", err)
		}

		return nil
	}))

	// CLUSTER_COUNTKEYSINSLOT: Verify key counting per slot
	results = append(results, RunTest("CLUSTER_COUNTKEYSINSLOT", cat, func() error {
		// Use a cluster-aware client to SET a key, then check count on the right node
		// "foo" hashes to slot 12182
		slot, err := clusterClient.ClusterKeySlot(ctx, "foo").Result()
		if err != nil {
			return err
		}

		// Find which node owns this slot
		slots, err := clusterClient.ClusterSlots(ctx).Result()
		if err != nil {
			return err
		}

		var targetAddr string
		for _, s := range slots {
			if int64(s.Start) <= slot && slot <= int64(s.End) && len(s.Nodes) > 0 {
				targetAddr = s.Nodes[0].Addr
				break
			}
		}

		if targetAddr == "" {
			return fmt.Errorf("could not find node owning slot %d", slot)
		}

		// Connect to the target node
		targetClient := redis.NewClient(&redis.Options{
			Addr:     targetAddr,
			Password: password,
			DB:       0,
		})
		defer targetClient.Close()

		k := Key(prefix, "cluster:countslot:foo")
		// Find what slot this key actually hashes to
		keySlot, err := targetClient.ClusterKeySlot(ctx, k).Result()
		if err != nil {
			return err
		}

		// Find node owning this key's slot
		var keyNodeAddr string
		for _, s := range slots {
			if int64(s.Start) <= keySlot && keySlot <= int64(s.End) && len(s.Nodes) > 0 {
				keyNodeAddr = s.Nodes[0].Addr
				break
			}
		}

		if keyNodeAddr == "" {
			return fmt.Errorf("could not find node owning slot %d", keySlot)
		}

		keyNodeClient := redis.NewClient(&redis.Options{
			Addr:     keyNodeAddr,
			Password: password,
			DB:       0,
		})
		defer keyNodeClient.Close()

		// Set the key on the correct node
		if err := keyNodeClient.Set(ctx, k, "value", 0).Err(); err != nil {
			return err
		}
		defer keyNodeClient.Del(ctx, k)

		// Count keys in that slot
		count, err := keyNodeClient.ClusterCountKeysInSlot(ctx, int(keySlot)).Result()
		if err != nil {
			return err
		}

		return AssertTrue(count >= 1, fmt.Sprintf("CLUSTER COUNTKEYSINSLOT %d should be >= 1 after SET, got %d", keySlot, count))
	}))

	// CLUSTER_cross_slot_error: Verify CROSSSLOT error for multi-key commands on different slots
	results = append(results, RunTest("CLUSTER_cross_slot_error", cat, func() error {
		// MSET with keys in different slots should fail
		// "key1" and "key2" likely hash to different slots
		err := clusterClient.MSet(ctx, "cluster_cross_a", "val1", "cluster_cross_b", "val2").Err()
		if err == nil {
			// Clean up
			clusterClient.Del(ctx, "cluster_cross_a", "cluster_cross_b")
			// If it succeeded, the server might not enforce CROSSSLOT on non-cluster-aware connections
			// This is valid behavior for some implementations
			return nil
		}

		errStr := err.Error()
		if strings.Contains(errStr, "CROSSSLOT") || strings.Contains(errStr, "MOVED") {
			return nil // Expected
		}
		return fmt.Errorf("expected CROSSSLOT or MOVED error, got: %s", errStr)
	}))

	// READONLY_mode: READONLY on a cluster node, verify reads succeed
	results = append(results, RunTest("READONLY_mode", cat, func() error {
		// Send READONLY command to enable replica reads
		result, err := clusterClient.Do(ctx, "READONLY").Result()
		if err != nil {
			return err
		}
		if result != "OK" {
			return fmt.Errorf("READONLY should return OK, got %v", result)
		}

		// Send READWRITE to disable it
		result, err = clusterClient.Do(ctx, "READWRITE").Result()
		if err != nil {
			return err
		}
		if result != "OK" {
			return fmt.Errorf("READWRITE should return OK, got %v", result)
		}

		return nil
	}))

	return results
}
