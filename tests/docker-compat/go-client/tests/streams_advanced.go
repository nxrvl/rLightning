package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("streams_advanced", testStreamsAdvanced)
}

func testStreamsAdvanced(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "streams_advanced"

	// XGROUP_CREATE: Create consumer group, verify with XINFO GROUPS
	results = append(results, RunTest("XGROUP_CREATE", cat, func() error {
		k := Key(prefix, "sa:xgroup")
		defer rdb.Del(ctx, k)
		// Create stream with an entry first
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"f": "v"}})
		err := rdb.XGroupCreate(ctx, k, "mygroup", "0").Err()
		if err != nil {
			return err
		}
		groups, err := rdb.XInfoGroups(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(1, len(groups)); err != nil {
			return err
		}
		return AssertEqual("mygroup", groups[0].Name)
	}))

	// XREADGROUP_basic: Create group, XREADGROUP, verify message delivered
	results = append(results, RunTest("XREADGROUP_basic", cat, func() error {
		k := Key(prefix, "sa:xreadgroup")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"msg": "hello"}})
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "grp1",
			Consumer: "consumer1",
			Streams:  []string{k, ">"},
			Count:    10,
		}).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(1, len(res)); err != nil {
			return err
		}
		if err := AssertEqual(1, len(res[0].Messages)); err != nil {
			return err
		}
		return AssertEqual("hello", res[0].Messages[0].Values["msg"])
	}))

	// XREADGROUP_pending: XREADGROUP without ACK, verify message in pending list
	results = append(results, RunTest("XREADGROUP_pending", cat, func() error {
		k := Key(prefix, "sa:xreadpend")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"f": "v"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "2-1", Values: map[string]interface{}{"f": "v2"}})
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		// Read without ACK
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "grp1",
			Consumer: "c1",
			Streams:  []string{k, ">"},
			Count:    10,
		})

		pending, err := rdb.XPending(ctx, k, "grp1").Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(2, pending.Count)
	}))

	// XACK_basic: Read message, XACK, verify removed from pending list
	results = append(results, RunTest("XACK_basic", cat, func() error {
		k := Key(prefix, "sa:xack")
		defer rdb.Del(ctx, k)
		id, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, Values: map[string]interface{}{"f": "v"}}).Result()
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "grp1",
			Consumer: "c1",
			Streams:  []string{k, ">"},
			Count:    10,
		})

		// Verify pending before ACK
		pending, _ := rdb.XPending(ctx, k, "grp1").Result()
		if err := AssertEqualInt64(1, pending.Count); err != nil {
			return err
		}

		// ACK
		n, err := rdb.XAck(ctx, k, "grp1", id).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}

		// Verify pending after ACK
		pending, _ = rdb.XPending(ctx, k, "grp1").Result()
		return AssertEqualInt64(0, pending.Count)
	}))

	// XCLAIM_basic: Read on consumer1, XCLAIM to consumer2
	results = append(results, RunTest("XCLAIM_basic", cat, func() error {
		k := Key(prefix, "sa:xclaim")
		defer rdb.Del(ctx, k)
		id, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, Values: map[string]interface{}{"f": "v"}}).Result()
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		// Consumer1 reads
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "grp1",
			Consumer: "consumer1",
			Streams:  []string{k, ">"},
			Count:    10,
		})

		// Claim to consumer2 with 0 min-idle-time
		msgs, err := rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   k,
			Group:    "grp1",
			Consumer: "consumer2",
			MinIdle:  0,
			Messages: []string{id},
		}).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(1, len(msgs)); err != nil {
			return err
		}

		// Verify consumer2 has it in pending
		detail, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: k,
			Group:  "grp1",
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(1, len(detail)); err != nil {
			return err
		}
		return AssertEqual("consumer2", detail[0].Consumer)
	}))

	// XAUTOCLAIM_basic: Read message, wait, XAUTOCLAIM with min-idle
	results = append(results, RunTest("XAUTOCLAIM_basic", cat, func() error {
		k := Key(prefix, "sa:xautoclaim")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"f": "v"}})
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		// Consumer1 reads
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "grp1",
			Consumer: "consumer1",
			Streams:  []string{k, ">"},
			Count:    10,
		})

		// Small wait to ensure idle time > 0
		time.Sleep(50 * time.Millisecond)

		// XAUTOCLAIM to consumer2 with very low min-idle
		msgs, _, err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   k,
			Group:    "grp1",
			Consumer: "consumer2",
			MinIdle:  time.Millisecond, // 1ms min idle
			Start:    "0-0",
			Count:    10,
		}).Result()
		if err != nil {
			return err
		}
		return AssertTrue(len(msgs) >= 1, fmt.Sprintf("expected at least 1 autoclaimed message, got %d", len(msgs)))
	}))

	// XPENDING_summary: Check pending summary
	results = append(results, RunTest("XPENDING_summary", cat, func() error {
		k := Key(prefix, "sa:xpendsumm")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"f": "v1"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "2-1", Values: map[string]interface{}{"f": "v2"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "3-1", Values: map[string]interface{}{"f": "v3"}})
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		// Read 2 messages with consumer1, 1 with consumer2
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: "grp1", Consumer: "c1",
			Streams: []string{k, ">"}, Count: 2,
		})
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: "grp1", Consumer: "c2",
			Streams: []string{k, ">"}, Count: 1,
		})

		pending, err := rdb.XPending(ctx, k, "grp1").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(3, pending.Count); err != nil {
			return err
		}
		if err := AssertEqual("1-1", pending.Lower); err != nil {
			return err
		}
		if err := AssertEqual("3-1", pending.Higher); err != nil {
			return err
		}
		// Should have 2 consumers
		return AssertEqual(2, len(pending.Consumers))
	}))

	// XPENDING_detail: Check per-consumer pending detail
	results = append(results, RunTest("XPENDING_detail", cat, func() error {
		k := Key(prefix, "sa:xpenddet")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"f": "v1"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "2-1", Values: map[string]interface{}{"f": "v2"}})
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: "grp1", Consumer: "c1",
			Streams: []string{k, ">"}, Count: 10,
		})

		detail, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: k,
			Group:  "grp1",
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(2, len(detail)); err != nil {
			return err
		}
		if err := AssertEqual("c1", detail[0].Consumer); err != nil {
			return err
		}
		return AssertEqual("1-1", detail[0].ID)
	}))

	// XINFO_STREAM: Verify full stream info including length, groups, first/last entry
	results = append(results, RunTest("XINFO_STREAM", cat, func() error {
		k := Key(prefix, "sa:xinfost")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"f": "v1"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "2-1", Values: map[string]interface{}{"f": "v2"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "3-1", Values: map[string]interface{}{"f": "v3"}})
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		info, err := rdb.XInfoStream(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(3, info.Length); err != nil {
			return err
		}
		if err := AssertEqualInt64(1, int64(info.Groups)); err != nil {
			return err
		}
		if err := AssertEqual("1-1", info.FirstEntry.ID); err != nil {
			return err
		}
		return AssertEqual("3-1", info.LastEntry.ID)
	}))

	// XINFO_CONSUMERS: Verify consumer info
	results = append(results, RunTest("XINFO_CONSUMERS", cat, func() error {
		k := Key(prefix, "sa:xinfocons")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"f": "v"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "2-1", Values: map[string]interface{}{"f": "v2"}})
		rdb.XGroupCreate(ctx, k, "grp1", "0")

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: "grp1", Consumer: "consumer1",
			Streams: []string{k, ">"}, Count: 10,
		})

		consumers, err := rdb.XInfoConsumers(ctx, k, "grp1").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(1, len(consumers)); err != nil {
			return err
		}
		if err := AssertEqual("consumer1", consumers[0].Name); err != nil {
			return err
		}
		return AssertEqualInt64(2, consumers[0].Pending)
	}))

	// XTRIM_MAXLEN: Add entries, XTRIM MAXLEN 5, verify only 5 remain
	results = append(results, RunTest("XTRIM_MAXLEN", cat, func() error {
		k := Key(prefix, "sa:xtrimmax")
		defer rdb.Del(ctx, k)
		for i := 0; i < 10; i++ {
			rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, Values: map[string]interface{}{"i": i}})
		}
		_, err := rdb.XTrimMaxLen(ctx, k, 5).Result()
		if err != nil {
			return err
		}
		length, _ := rdb.XLen(ctx, k).Result()
		return AssertEqualInt64(5, length)
	}))

	// XTRIM_MINID: Add entries, XTRIM MINID, verify entries before ID removed
	results = append(results, RunTest("XTRIM_MINID", cat, func() error {
		k := Key(prefix, "sa:xtrimmin")
		defer rdb.Del(ctx, k)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "1-1", Values: map[string]interface{}{"f": "v1"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "2-1", Values: map[string]interface{}{"f": "v2"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "3-1", Values: map[string]interface{}{"f": "v3"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "4-1", Values: map[string]interface{}{"f": "v4"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: k, ID: "5-1", Values: map[string]interface{}{"f": "v5"}})

		// Trim entries with ID < 3-1
		_, err := rdb.XTrimMinID(ctx, k, "3-1").Result()
		if err != nil {
			return err
		}
		length, _ := rdb.XLen(ctx, k).Result()
		if err := AssertEqualInt64(3, length); err != nil {
			return err
		}
		// Verify first entry is now 3-1
		msgs, _ := rdb.XRange(ctx, k, "-", "+").Result()
		return AssertEqual("3-1", msgs[0].ID)
	}))

	return results
}
