package tests

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("sets", testSets)
}

func testSets(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "sets"

	results = append(results, RunTest("SADD_single_and_multiple", cat, func() error {
		k := Key(prefix, "set:add")
		defer rdb.Del(ctx, k)
		n, err := rdb.SAdd(ctx, k, "a").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}
		n, err = rdb.SAdd(ctx, k, "b", "c", "d").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(3, n); err != nil {
			return err
		}
		// Adding duplicate
		n, _ = rdb.SAdd(ctx, k, "a").Result()
		return AssertEqualInt64(0, n)
	}))

	results = append(results, RunTest("SMEMBERS_SISMEMBER_SMISMEMBER", cat, func() error {
		k := Key(prefix, "set:members")
		defer rdb.Del(ctx, k)
		rdb.SAdd(ctx, k, "a", "b", "c")
		members, err := rdb.SMembers(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertStringSliceEqualUnordered([]string{"a", "b", "c"}, members); err != nil {
			return err
		}
		is, err := rdb.SIsMember(ctx, k, "a").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(is, "a should be a member"); err != nil {
			return err
		}
		is, _ = rdb.SIsMember(ctx, k, "z").Result()
		if err := AssertTrue(!is, "z should not be a member"); err != nil {
			return err
		}
		mis, err := rdb.SMIsMember(ctx, k, "a", "z", "c").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(mis[0], "a should be member"); err != nil {
			return err
		}
		if err := AssertTrue(!mis[1], "z should not be member"); err != nil {
			return err
		}
		return AssertTrue(mis[2], "c should be member")
	}))

	results = append(results, RunTest("SREM_SPOP_SRANDMEMBER", cat, func() error {
		k := Key(prefix, "set:rem")
		defer rdb.Del(ctx, k)
		rdb.SAdd(ctx, k, "a", "b", "c", "d", "e")
		n, err := rdb.SRem(ctx, k, "a", "b").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(2, n); err != nil {
			return err
		}
		popped, err := rdb.SPop(ctx, k).Result()
		if err != nil {
			return err
		}
		valid := popped == "c" || popped == "d" || popped == "e"
		if err := AssertTrue(valid, "popped should be c, d, or e"); err != nil {
			return err
		}
		rand, err := rdb.SRandMember(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(len(rand) > 0, "SRANDMEMBER should return a member")
	}))

	results = append(results, RunTest("SCARD", cat, func() error {
		k := Key(prefix, "set:card")
		defer rdb.Del(ctx, k)
		rdb.SAdd(ctx, k, "a", "b", "c")
		n, err := rdb.SCard(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(3, n)
	}))

	results = append(results, RunTest("SUNION_SINTER_SDIFF", cat, func() error {
		k1 := Key(prefix, "set:op1")
		k2 := Key(prefix, "set:op2")
		defer rdb.Del(ctx, k1, k2)
		rdb.SAdd(ctx, k1, "a", "b", "c")
		rdb.SAdd(ctx, k2, "b", "c", "d")

		union, err := rdb.SUnion(ctx, k1, k2).Result()
		if err != nil {
			return err
		}
		if err := AssertStringSliceEqualUnordered([]string{"a", "b", "c", "d"}, union); err != nil {
			return err
		}

		inter, err := rdb.SInter(ctx, k1, k2).Result()
		if err != nil {
			return err
		}
		if err := AssertStringSliceEqualUnordered([]string{"b", "c"}, inter); err != nil {
			return err
		}

		diff, err := rdb.SDiff(ctx, k1, k2).Result()
		if err != nil {
			return err
		}
		return AssertStringSliceEqualUnordered([]string{"a"}, diff)
	}))

	results = append(results, RunTest("SUNIONSTORE_SINTERSTORE_SDIFFSTORE", cat, func() error {
		k1 := Key(prefix, "set:store1")
		k2 := Key(prefix, "set:store2")
		dest1 := Key(prefix, "set:union_dest")
		dest2 := Key(prefix, "set:inter_dest")
		dest3 := Key(prefix, "set:diff_dest")
		defer rdb.Del(ctx, k1, k2, dest1, dest2, dest3)
		rdb.SAdd(ctx, k1, "a", "b", "c")
		rdb.SAdd(ctx, k2, "b", "c", "d")

		n, err := rdb.SUnionStore(ctx, dest1, k1, k2).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(4, n); err != nil {
			return err
		}

		n, err = rdb.SInterStore(ctx, dest2, k1, k2).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(2, n); err != nil {
			return err
		}

		n, err = rdb.SDiffStore(ctx, dest3, k1, k2).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(1, n)
	}))

	results = append(results, RunTest("SMOVE", cat, func() error {
		src := Key(prefix, "set:move_src")
		dst := Key(prefix, "set:move_dst")
		defer rdb.Del(ctx, src, dst)
		rdb.SAdd(ctx, src, "a", "b")
		rdb.SAdd(ctx, dst, "c")
		ok, err := rdb.SMove(ctx, src, dst, "a").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "SMOVE should succeed"); err != nil {
			return err
		}
		srcMembers, _ := rdb.SMembers(ctx, src).Result()
		dstMembers, _ := rdb.SMembers(ctx, dst).Result()
		if err := AssertStringSliceEqualUnordered([]string{"b"}, srcMembers); err != nil {
			return err
		}
		return AssertStringSliceEqualUnordered([]string{"a", "c"}, dstMembers)
	}))

	results = append(results, RunTest("SSCAN", cat, func() error {
		k := Key(prefix, "set:scan")
		defer rdb.Del(ctx, k)
		for i := 0; i < 20; i++ {
			rdb.SAdd(ctx, k, "member"+strconv.Itoa(i))
		}
		var all []string
		var cursor uint64
		for {
			members, next, err := rdb.SScan(ctx, k, cursor, "*", 10).Result()
			if err != nil {
				return err
			}
			all = append(all, members...)
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return AssertTrue(len(all) == 20, "SSCAN should return all 20 members, got "+strconv.Itoa(len(all)))
	}))

	results = append(results, RunTest("SINTERCARD", cat, func() error {
		k1 := Key(prefix, "set:ic1")
		k2 := Key(prefix, "set:ic2")
		defer rdb.Del(ctx, k1, k2)
		rdb.SAdd(ctx, k1, "a", "b", "c")
		rdb.SAdd(ctx, k2, "b", "c", "d")
		n, err := rdb.SInterCard(ctx, 0, k1, k2).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(2, n)
	}))

	return results
}
