package tests

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("keys", testKeys)
}

func testKeys(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "keys"

	results = append(results, RunTest("DEL_single_and_multiple", cat, func() error {
		k1 := Key(prefix, "key:del1")
		k2 := Key(prefix, "key:del2")
		k3 := Key(prefix, "key:del3")
		rdb.Set(ctx, k1, "a", 0)
		rdb.Set(ctx, k2, "b", 0)
		rdb.Set(ctx, k3, "c", 0)
		n, err := rdb.Del(ctx, k1).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}
		n, err = rdb.Del(ctx, k2, k3).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(2, n)
	}))

	results = append(results, RunTest("EXISTS_single_and_multiple", cat, func() error {
		k1 := Key(prefix, "key:ex1")
		k2 := Key(prefix, "key:ex2")
		defer rdb.Del(ctx, k1, k2)
		rdb.Set(ctx, k1, "a", 0)
		rdb.Set(ctx, k2, "b", 0)
		n, err := rdb.Exists(ctx, k1).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}
		n, err = rdb.Exists(ctx, k1, k2, Key(prefix, "key:nonexist")).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(2, n)
	}))

	results = append(results, RunTest("EXPIRE_TTL", cat, func() error {
		k := Key(prefix, "key:expire")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "val", 0)
		ok, err := rdb.Expire(ctx, k, 10*time.Second).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "EXPIRE should succeed"); err != nil {
			return err
		}
		ttl, err := rdb.TTL(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(ttl > 0 && ttl <= 10*time.Second, "TTL should be between 0 and 10s")
	}))

	results = append(results, RunTest("PEXPIRE_PTTL", cat, func() error {
		k := Key(prefix, "key:pexpire")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "val", 0)
		ok, err := rdb.PExpire(ctx, k, 10000*time.Millisecond).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "PEXPIRE should succeed"); err != nil {
			return err
		}
		pttl, err := rdb.PTTL(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(pttl > 0, "PTTL should be positive")
	}))

	results = append(results, RunTest("EXPIREAT_PEXPIREAT", cat, func() error {
		k := Key(prefix, "key:expireat")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "val", 0)
		future := time.Now().Add(60 * time.Second)
		ok, err := rdb.ExpireAt(ctx, k, future).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "EXPIREAT should succeed"); err != nil {
			return err
		}
		ttl, _ := rdb.TTL(ctx, k).Result()
		if err := AssertTrue(ttl > 50*time.Second, "TTL should be ~60s"); err != nil {
			return err
		}

		k2 := Key(prefix, "key:pexpireat")
		defer rdb.Del(ctx, k2)
		rdb.Set(ctx, k2, "val", 0)
		ok, err = rdb.PExpireAt(ctx, k2, future).Result()
		if err != nil {
			return err
		}
		return AssertTrue(ok, "PEXPIREAT should succeed")
	}))

	results = append(results, RunTest("PERSIST", cat, func() error {
		k := Key(prefix, "key:persist")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "val", 10*time.Second)
		ok, err := rdb.Persist(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(ok, "PERSIST should succeed"); err != nil {
			return err
		}
		ttl, _ := rdb.TTL(ctx, k).Result()
		// go-redis returns -1s for keys without TTL, -2s for non-existent keys
		return AssertTrue(ttl < 0, "TTL should be negative after PERSIST (no expiration)")
	}))

	results = append(results, RunTest("TYPE", cat, func() error {
		ks := Key(prefix, "key:type_s")
		kl := Key(prefix, "key:type_l")
		kh := Key(prefix, "key:type_h")
		kz := Key(prefix, "key:type_z")
		kset := Key(prefix, "key:type_set")
		defer rdb.Del(ctx, ks, kl, kh, kz, kset)
		rdb.Set(ctx, ks, "val", 0)
		rdb.LPush(ctx, kl, "val")
		rdb.HSet(ctx, kh, "f", "v")
		rdb.ZAdd(ctx, kz, redis.Z{Score: 1, Member: "a"})
		rdb.SAdd(ctx, kset, "a")

		t, _ := rdb.Type(ctx, ks).Result()
		if err := AssertEqual("string", t); err != nil {
			return err
		}
		t, _ = rdb.Type(ctx, kl).Result()
		if err := AssertEqual("list", t); err != nil {
			return err
		}
		t, _ = rdb.Type(ctx, kh).Result()
		if err := AssertEqual("hash", t); err != nil {
			return err
		}
		t, _ = rdb.Type(ctx, kz).Result()
		if err := AssertEqual("zset", t); err != nil {
			return err
		}
		t, _ = rdb.Type(ctx, kset).Result()
		return AssertEqual("set", t)
	}))

	results = append(results, RunTest("RENAME_RENAMENX", cat, func() error {
		k1 := Key(prefix, "key:rename_src")
		k2 := Key(prefix, "key:rename_dst")
		k3 := Key(prefix, "key:renamenx_dst")
		defer rdb.Del(ctx, k1, k2, k3)
		rdb.Set(ctx, k1, "val", 0)
		if err := rdb.Rename(ctx, k1, k2).Err(); err != nil {
			return err
		}
		val, _ := rdb.Get(ctx, k2).Result()
		if err := AssertEqual("val", val); err != nil {
			return err
		}

		// RENAMENX
		rdb.Set(ctx, k1, "new", 0)
		rdb.Set(ctx, k3, "exists", 0)
		ok, _ := rdb.RenameNX(ctx, k1, k3).Result()
		if err := AssertTrue(!ok, "RENAMENX should fail when dest exists"); err != nil {
			return err
		}
		rdb.Del(ctx, k3)
		ok, _ = rdb.RenameNX(ctx, k1, k3).Result()
		return AssertTrue(ok, "RENAMENX should succeed when dest doesn't exist")
	}))

	results = append(results, RunTest("KEYS_pattern", cat, func() error {
		k1 := Key(prefix, "key:pat_a")
		k2 := Key(prefix, "key:pat_b")
		k3 := Key(prefix, "key:pat_c")
		defer rdb.Del(ctx, k1, k2, k3)
		rdb.Set(ctx, k1, "1", 0)
		rdb.Set(ctx, k2, "2", 0)
		rdb.Set(ctx, k3, "3", 0)
		keys, err := rdb.Keys(ctx, Key(prefix, "key:pat_*")).Result()
		if err != nil {
			return err
		}
		return AssertTrue(len(keys) >= 3, "KEYS should return at least 3 keys")
	}))

	results = append(results, RunTest("SCAN_with_pattern", cat, func() error {
		for i := 0; i < 10; i++ {
			rdb.Set(ctx, Key(prefix, "key:scan_"+strconv.Itoa(i)), strconv.Itoa(i), 0)
		}
		defer func() {
			for i := 0; i < 10; i++ {
				rdb.Del(ctx, Key(prefix, "key:scan_"+strconv.Itoa(i)))
			}
		}()
		var all []string
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(ctx, cursor, Key(prefix, "key:scan_*"), 5).Result()
			if err != nil {
				return err
			}
			all = append(all, keys...)
			cursor = next
			if cursor == 0 {
				break
			}
		}
		return AssertTrue(len(all) >= 10, "SCAN should find at least 10 keys, got "+strconv.Itoa(len(all)))
	}))

	results = append(results, RunTest("UNLINK", cat, func() error {
		k := Key(prefix, "key:unlink")
		rdb.Set(ctx, k, "val", 0)
		n, err := rdb.Unlink(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, n); err != nil {
			return err
		}
		exists, _ := rdb.Exists(ctx, k).Result()
		return AssertEqualInt64(0, exists)
	}))

	results = append(results, RunTest("OBJECT_ENCODING", cat, func() error {
		k := Key(prefix, "key:obj_enc")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "12345", 0)
		enc, err := rdb.ObjectEncoding(ctx, k).Result()
		if err != nil {
			return err
		}
		// Should be "int" or "embstr" depending on value
		return AssertTrue(len(enc) > 0, "OBJECT ENCODING should return a value")
	}))

	results = append(results, RunTest("COPY", cat, func() error {
		src := Key(prefix, "key:copy_src")
		dst := Key(prefix, "key:copy_dst")
		defer rdb.Del(ctx, src, dst)
		rdb.Set(ctx, src, "copyval", 0)
		res, err := rdb.Copy(ctx, src, dst, 0, false).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, res); err != nil {
			return err
		}
		val, _ := rdb.Get(ctx, dst).Result()
		return AssertEqual("copyval", val)
	}))

	results = append(results, RunTest("DUMP_RESTORE", cat, func() error {
		src := Key(prefix, "key:dump")
		dst := Key(prefix, "key:restore")
		defer rdb.Del(ctx, src, dst)
		rdb.Set(ctx, src, "dumpval", 0)
		dump, err := rdb.Dump(ctx, src).Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(len(dump) > 0, "DUMP should return data"); err != nil {
			return err
		}
		err = rdb.Restore(ctx, dst, 0, dump).Err()
		if err != nil {
			return err
		}
		val, _ := rdb.Get(ctx, dst).Result()
		return AssertEqual("dumpval", val)
	}))

	return results
}
