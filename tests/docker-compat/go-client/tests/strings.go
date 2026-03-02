package tests

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("strings", testStrings)
}

func testStrings(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "strings"

	results = append(results, RunTest("SET_GET_basic", cat, func() error {
		k := Key(prefix, "str:basic")
		defer rdb.Del(ctx, k)
		if err := rdb.Set(ctx, k, "hello", 0).Err(); err != nil {
			return err
		}
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqual("hello", val)
	}))

	results = append(results, RunTest("SET_NX_flag", cat, func() error {
		k := Key(prefix, "str:setnx")
		defer rdb.Del(ctx, k)
		ok, err := rdb.SetNX(ctx, k, "first", 0).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "first SetNX should succeed"); err != nil {
			return err
		}
		ok, err = rdb.SetNX(ctx, k, "second", 0).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(!ok, "second SetNX should fail"); err != nil {
			return err
		}
		val, _ := rdb.Get(ctx, k).Result()
		return AssertEqual("first", val)
	}))

	results = append(results, RunTest("SET_XX_flag", cat, func() error {
		k := Key(prefix, "str:setxx")
		defer rdb.Del(ctx, k)
		ok, err := rdb.SetXX(ctx, k, "nope", 0).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(!ok, "SetXX on non-existent key should fail"); err != nil {
			return err
		}
		rdb.Set(ctx, k, "exists", 0)
		ok, err = rdb.SetXX(ctx, k, "updated", 0).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "SetXX on existing key should succeed"); err != nil {
			return err
		}
		val, _ := rdb.Get(ctx, k).Result()
		return AssertEqual("updated", val)
	}))

	results = append(results, RunTest("SET_with_EX", cat, func() error {
		k := Key(prefix, "str:setex")
		defer rdb.Del(ctx, k)
		if err := rdb.Set(ctx, k, "ttlval", 10*time.Second).Err(); err != nil {
			return err
		}
		ttl, err := rdb.TTL(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(ttl > 0 && ttl <= 10*time.Second, "TTL should be between 0 and 10s")
	}))

	results = append(results, RunTest("SET_with_PX", cat, func() error {
		k := Key(prefix, "str:setpx")
		defer rdb.Del(ctx, k)
		if err := rdb.Set(ctx, k, "pxval", 10000*time.Millisecond).Err(); err != nil {
			return err
		}
		pttl, err := rdb.PTTL(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(pttl > 0, "PTTL should be positive")
	}))

	results = append(results, RunTest("MSET_MGET", cat, func() error {
		k1 := Key(prefix, "str:m1")
		k2 := Key(prefix, "str:m2")
		k3 := Key(prefix, "str:m3")
		defer rdb.Del(ctx, k1, k2, k3)
		if err := rdb.MSet(ctx, k1, "a", k2, "b", k3, "c").Err(); err != nil {
			return err
		}
		vals, err := rdb.MGet(ctx, k1, k2, k3).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(3, len(vals)); err != nil {
			return err
		}
		if err := AssertEqual("a", vals[0]); err != nil {
			return err
		}
		if err := AssertEqual("b", vals[1]); err != nil {
			return err
		}
		return AssertEqual("c", vals[2])
	}))

	results = append(results, RunTest("INCR_DECR", cat, func() error {
		k := Key(prefix, "str:incr")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "10", 0)
		val, err := rdb.Incr(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(11, val); err != nil {
			return err
		}
		val, err = rdb.Decr(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(10, val)
	}))

	results = append(results, RunTest("INCRBY_DECRBY", cat, func() error {
		k := Key(prefix, "str:incrby")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "100", 0)
		val, err := rdb.IncrBy(ctx, k, 25).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(125, val); err != nil {
			return err
		}
		val, err = rdb.DecrBy(ctx, k, 50).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(75, val)
	}))

	results = append(results, RunTest("INCRBYFLOAT", cat, func() error {
		k := Key(prefix, "str:incrf")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "10.5", 0)
		val, err := rdb.IncrByFloat(ctx, k, 0.1).Result()
		if err != nil {
			return err
		}
		return AssertEqualFloat64(10.6, val, 0.001)
	}))

	results = append(results, RunTest("APPEND_STRLEN", cat, func() error {
		k := Key(prefix, "str:append")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "hello", 0)
		newLen, err := rdb.Append(ctx, k, " world").Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(11, newLen); err != nil {
			return err
		}
		slen, err := rdb.StrLen(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(11, slen)
	}))

	results = append(results, RunTest("GETRANGE_SETRANGE", cat, func() error {
		k := Key(prefix, "str:range")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "Hello, World!", 0)
		sub, err := rdb.GetRange(ctx, k, 7, 11).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("World", sub); err != nil {
			return err
		}
		_, err = rdb.SetRange(ctx, k, 7, "Redis").Result()
		if err != nil {
			return err
		}
		val, _ := rdb.Get(ctx, k).Result()
		return AssertEqual("Hello, Redis!", val)
	}))

	results = append(results, RunTest("GETSET_deprecated", cat, func() error {
		k := Key(prefix, "str:getset")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "old", 0)
		// GetSet is deprecated but still works
		old, err := rdb.GetSet(ctx, k, "new").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("old", old); err != nil {
			return err
		}
		val, _ := rdb.Get(ctx, k).Result()
		return AssertEqual("new", val)
	}))

	results = append(results, RunTest("GETDEL", cat, func() error {
		k := Key(prefix, "str:getdel")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "deleteme", 0)
		val, err := rdb.GetDel(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("deleteme", val); err != nil {
			return err
		}
		_, err = rdb.Get(ctx, k).Result()
		return AssertErr(err) // key should be gone
	}))

	results = append(results, RunTest("SETNX_command", cat, func() error {
		k := Key(prefix, "str:setnxcmd")
		defer rdb.Del(ctx, k)
		ok, err := rdb.SetNX(ctx, k, "val", 0).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "SETNX should succeed on new key"); err != nil {
			return err
		}
		ok, _ = rdb.SetNX(ctx, k, "val2", 0).Result()
		return AssertTrue(!ok, "SETNX should fail on existing key")
	}))

	results = append(results, RunTest("SETEX_command", cat, func() error {
		k := Key(prefix, "str:setexcmd")
		defer rdb.Del(ctx, k)
		if err := rdb.SetEx(ctx, k, "val", 10*time.Second).Err(); err != nil {
			return err
		}
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("val", val); err != nil {
			return err
		}
		ttl, _ := rdb.TTL(ctx, k).Result()
		return AssertTrue(ttl > 0, "TTL should be positive after SETEX")
	}))

	results = append(results, RunTest("PSETEX_command", cat, func() error {
		k := Key(prefix, "str:psetex")
		defer rdb.Del(ctx, k)
		if err := rdb.Do(ctx, "PSETEX", k, 10000, "val").Err(); err != nil {
			return err
		}
		pttl, err := rdb.PTTL(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(pttl > 0, "PTTL should be positive after PSETEX")
	}))

	results = append(results, RunTest("SET_with_EXAT", cat, func() error {
		k := Key(prefix, "str:exat")
		defer rdb.Del(ctx, k)
		expireAt := time.Now().Add(60 * time.Second)
		if err := rdb.SetArgs(ctx, k, "val", redis.SetArgs{ExpireAt: expireAt}).Err(); err != nil {
			return err
		}
		ttl, err := rdb.TTL(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(ttl > 50*time.Second && ttl <= 60*time.Second, "TTL should be ~60s")
	}))

	results = append(results, RunTest("MGET_with_missing_keys", cat, func() error {
		k1 := Key(prefix, "str:mget_exists")
		defer rdb.Del(ctx, k1)
		rdb.Set(ctx, k1, "val", 0)
		vals, err := rdb.MGet(ctx, k1, Key(prefix, "str:mget_nonexist")).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(2, len(vals)); err != nil {
			return err
		}
		if err := AssertEqual("val", vals[0]); err != nil {
			return err
		}
		return AssertTrue(vals[1] == nil, "missing key should return nil")
	}))

	return results
}
