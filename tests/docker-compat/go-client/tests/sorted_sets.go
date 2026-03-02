package tests

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("sorted_sets", testSortedSets)
}

func testSortedSets(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "sorted_sets"

	results = append(results, RunTest("ZADD_single_and_multiple", cat, func() error {
		k := Key(prefix, "zset:add")
		defer rdb.Del(ctx, k)
		n, err := rdb.ZAdd(ctx, k, redis.Z{Score: 1, Member: "a"}).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}
		n, err = rdb.ZAdd(ctx, k, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(2, n)
	}))

	results = append(results, RunTest("ZADD_NX_XX_GT_LT", cat, func() error {
		k := Key(prefix, "zset:flags")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k, redis.Z{Score: 5, Member: "a"})

		// NX: only add new
		n, _ := rdb.ZAddNX(ctx, k, redis.Z{Score: 10, Member: "a"}).Result()
		if err := AssertEqualInt64(0, n); err != nil {
			return err
		}
		score, _ := rdb.ZScore(ctx, k, "a").Result()
		if err := AssertEqualFloat64(5, score, 0.001); err != nil {
			return err
		}

		// XX: only update existing
		n, _ = rdb.ZAddXX(ctx, k, redis.Z{Score: 10, Member: "a"}).Result()
		if err := AssertEqualInt64(0, n); err != nil { // 0 because no new members added
			return err
		}
		score, _ = rdb.ZScore(ctx, k, "a").Result()
		if err := AssertEqualFloat64(10, score, 0.001); err != nil {
			return err
		}

		// GT: only update if new score > current
		rdb.ZAddGT(ctx, k, redis.Z{Score: 5, Member: "a"})
		score, _ = rdb.ZScore(ctx, k, "a").Result()
		if err := AssertEqualFloat64(10, score, 0.001); err != nil {
			return err
		}

		// LT: only update if new score < current
		rdb.ZAddLT(ctx, k, redis.Z{Score: 3, Member: "a"})
		score, _ = rdb.ZScore(ctx, k, "a").Result()
		return AssertEqualFloat64(3, score, 0.001)
	}))

	results = append(results, RunTest("ZSCORE_ZMSCORE", cat, func() error {
		k := Key(prefix, "zset:score")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k, redis.Z{Score: 1.5, Member: "a"}, redis.Z{Score: 2.5, Member: "b"})
		score, err := rdb.ZScore(ctx, k, "a").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualFloat64(1.5, score, 0.001); err != nil {
			return err
		}
		scores, err := rdb.ZMScore(ctx, k, "a", "b", "nonexist").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualFloat64(1.5, scores[0], 0.001); err != nil {
			return err
		}
		return AssertEqualFloat64(2.5, scores[1], 0.001)
	}))

	results = append(results, RunTest("ZRANGE_basic", cat, func() error {
		k := Key(prefix, "zset:range")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k, redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		vals, err := rdb.ZRange(ctx, k, 0, -1).Result()
		if err != nil {
			return err
		}
		return AssertStringSliceEqual([]string{"a", "b", "c"}, vals)
	}))

	results = append(results, RunTest("ZRANGEBYSCORE", cat, func() error {
		k := Key(prefix, "zset:rbyscore")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k,
			redis.Z{Score: 1, Member: "a"},
			redis.Z{Score: 2, Member: "b"},
			redis.Z{Score: 3, Member: "c"},
			redis.Z{Score: 4, Member: "d"},
		)
		vals, err := rdb.ZRangeByScore(ctx, k, &redis.ZRangeBy{Min: "2", Max: "3"}).Result()
		if err != nil {
			return err
		}
		return AssertStringSliceEqual([]string{"b", "c"}, vals)
	}))

	results = append(results, RunTest("ZREVRANGEBYSCORE", cat, func() error {
		k := Key(prefix, "zset:rrbyscore")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k,
			redis.Z{Score: 1, Member: "a"},
			redis.Z{Score: 2, Member: "b"},
			redis.Z{Score: 3, Member: "c"},
		)
		vals, err := rdb.ZRevRangeByScore(ctx, k, &redis.ZRangeBy{Min: "1", Max: "3"}).Result()
		if err != nil {
			return err
		}
		return AssertStringSliceEqual([]string{"c", "b", "a"}, vals)
	}))

	results = append(results, RunTest("ZRANK_ZREVRANK", cat, func() error {
		k := Key(prefix, "zset:rank")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k, redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		rank, err := rdb.ZRank(ctx, k, "b").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, rank); err != nil {
			return err
		}
		rrank, err := rdb.ZRevRank(ctx, k, "b").Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(1, rrank)
	}))

	results = append(results, RunTest("ZREM_ZREMRANGEBYSCORE_ZREMRANGEBYRANK", cat, func() error {
		k := Key(prefix, "zset:rem")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k,
			redis.Z{Score: 1, Member: "a"},
			redis.Z{Score: 2, Member: "b"},
			redis.Z{Score: 3, Member: "c"},
			redis.Z{Score: 4, Member: "d"},
			redis.Z{Score: 5, Member: "e"},
		)
		n, err := rdb.ZRem(ctx, k, "a").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}

		n, err = rdb.ZRemRangeByScore(ctx, k, "2", "3").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(2, n); err != nil {
			return err
		}

		n, err = rdb.ZRemRangeByRank(ctx, k, 0, 0).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(1, n) // removes "d"
	}))

	results = append(results, RunTest("ZCARD_ZCOUNT", cat, func() error {
		k := Key(prefix, "zset:card")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k, redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		card, err := rdb.ZCard(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(3, card); err != nil {
			return err
		}
		count, err := rdb.ZCount(ctx, k, "1", "2").Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(2, count)
	}))

	results = append(results, RunTest("ZINCRBY", cat, func() error {
		k := Key(prefix, "zset:incr")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k, redis.Z{Score: 10, Member: "a"})
		newScore, err := rdb.ZIncrBy(ctx, k, 5, "a").Result()
		if err != nil {
			return err
		}
		return AssertEqualFloat64(15, newScore, 0.001)
	}))

	results = append(results, RunTest("ZUNIONSTORE_ZINTERSTORE", cat, func() error {
		k1 := Key(prefix, "zset:u1")
		k2 := Key(prefix, "zset:u2")
		dest1 := Key(prefix, "zset:union")
		dest2 := Key(prefix, "zset:inter")
		defer rdb.Del(ctx, k1, k2, dest1, dest2)
		rdb.ZAdd(ctx, k1, redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		rdb.ZAdd(ctx, k2, redis.Z{Score: 3, Member: "b"}, redis.Z{Score: 4, Member: "c"})

		n, err := rdb.ZUnionStore(ctx, dest1, &redis.ZStore{Keys: []string{k1, k2}}).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(3, n); err != nil {
			return err
		}
		// b should have score 2+3=5
		score, _ := rdb.ZScore(ctx, dest1, "b").Result()
		if err := AssertEqualFloat64(5, score, 0.001); err != nil {
			return err
		}

		n, err = rdb.ZInterStore(ctx, dest2, &redis.ZStore{Keys: []string{k1, k2}}).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(1, n) // only "b"
	}))

	results = append(results, RunTest("ZRANDMEMBER", cat, func() error {
		k := Key(prefix, "zset:rand")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k, redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		members, err := rdb.ZRandMember(ctx, k, 1).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(len(members) == 1, "should return 1 member"); err != nil {
			return err
		}
		valid := members[0] == "a" || members[0] == "b" || members[0] == "c"
		return AssertTrue(valid, "member should be a, b, or c")
	}))

	results = append(results, RunTest("ZSCAN", cat, func() error {
		k := Key(prefix, "zset:scan")
		defer rdb.Del(ctx, k)
		for i := 0; i < 20; i++ {
			rdb.ZAdd(ctx, k, redis.Z{Score: float64(i), Member: "m" + strconv.Itoa(i)})
		}
		var all []string
		var cursor uint64
		for {
			members, next, err := rdb.ZScan(ctx, k, cursor, "*", 10).Result()
			if err != nil {
				return err
			}
			// ZScan returns member-score pairs
			for i := 0; i < len(members); i += 2 {
				all = append(all, members[i])
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return AssertTrue(len(all) == 20, "ZSCAN should return 20 members, got "+strconv.Itoa(len(all)))
	}))

	results = append(results, RunTest("ZPOPMIN_ZPOPMAX", cat, func() error {
		k := Key(prefix, "zset:pop")
		defer rdb.Del(ctx, k)
		rdb.ZAdd(ctx, k,
			redis.Z{Score: 1, Member: "a"},
			redis.Z{Score: 2, Member: "b"},
			redis.Z{Score: 3, Member: "c"},
		)
		min, err := rdb.ZPopMin(ctx, k, 1).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("a", min[0].Member); err != nil {
			return err
		}
		max, err := rdb.ZPopMax(ctx, k, 1).Result()
		if err != nil {
			return err
		}
		return AssertEqual("c", max[0].Member)
	}))

	return results
}
