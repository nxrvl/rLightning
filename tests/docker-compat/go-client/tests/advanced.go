package tests

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("advanced_types", testAdvancedTypes)
}

func testAdvancedTypes(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "advanced_types"

	// --- Bitmap ---

	results = append(results, RunTest("SETBIT_GETBIT", cat, func() error {
		k := Key(prefix, "bm:sg")
		defer rdb.Del(ctx, k)
		old, err := rdb.SetBit(ctx, k, 7, 1).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(0, old); err != nil {
			return err
		}
		val, err := rdb.GetBit(ctx, k, 7).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(1, val); err != nil {
			return err
		}
		val, _ = rdb.GetBit(ctx, k, 0).Result()
		return AssertEqualInt64(0, val)
	}))

	results = append(results, RunTest("BITCOUNT", cat, func() error {
		k := Key(prefix, "bm:count")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, "foobar", 0)
		n, err := rdb.BitCount(ctx, k, nil).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(26, n)
	}))

	results = append(results, RunTest("BITOP", cat, func() error {
		k1 := Key(prefix, "bm:op1")
		k2 := Key(prefix, "bm:op2")
		dest := Key(prefix, "bm:opdest")
		defer rdb.Del(ctx, k1, k2, dest)
		rdb.Set(ctx, k1, "abc", 0)
		rdb.Set(ctx, k2, "def", 0)
		n, err := rdb.BitOpAnd(ctx, dest, k1, k2).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(3, n)
	}))

	results = append(results, RunTest("BITPOS", cat, func() error {
		k := Key(prefix, "bm:pos")
		defer rdb.Del(ctx, k)
		rdb.SetBit(ctx, k, 0, 0)
		rdb.SetBit(ctx, k, 1, 0)
		rdb.SetBit(ctx, k, 2, 1)
		pos, err := rdb.BitPos(ctx, k, 1).Result()
		if err != nil {
			return err
		}
		return AssertEqualInt64(2, pos)
	}))

	// --- HyperLogLog ---

	results = append(results, RunTest("PFADD_PFCOUNT", cat, func() error {
		k := Key(prefix, "hll:basic")
		defer rdb.Del(ctx, k)
		n, err := rdb.PFAdd(ctx, k, "a", "b", "c", "d").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(n > 0, "PFADD should return 1 for new elements"); err != nil {
			return err
		}
		count, err := rdb.PFCount(ctx, k).Result()
		if err != nil {
			return err
		}
		return AssertTrue(count >= 3 && count <= 5, "PFCOUNT should be ~4")
	}))

	results = append(results, RunTest("PFMERGE", cat, func() error {
		k1 := Key(prefix, "hll:m1")
		k2 := Key(prefix, "hll:m2")
		dest := Key(prefix, "hll:merged")
		defer rdb.Del(ctx, k1, k2, dest)
		rdb.PFAdd(ctx, k1, "a", "b", "c")
		rdb.PFAdd(ctx, k2, "c", "d", "e")
		if err := rdb.PFMerge(ctx, dest, k1, k2).Err(); err != nil {
			return err
		}
		count, err := rdb.PFCount(ctx, dest).Result()
		if err != nil {
			return err
		}
		return AssertTrue(count >= 4 && count <= 6, "merged count should be ~5")
	}))

	// --- Geo ---

	results = append(results, RunTest("GEOADD_GEOPOS", cat, func() error {
		k := Key(prefix, "geo:cities")
		defer rdb.Del(ctx, k)
		n, err := rdb.GeoAdd(ctx, k,
			&redis.GeoLocation{Name: "Paris", Longitude: 2.3522, Latitude: 48.8566},
			&redis.GeoLocation{Name: "London", Longitude: -0.1278, Latitude: 51.5074},
			&redis.GeoLocation{Name: "Berlin", Longitude: 13.4050, Latitude: 52.5200},
		).Result()
		if err != nil {
			return err
		}
		if err := AssertEqualInt64(3, n); err != nil {
			return err
		}
		positions, err := rdb.GeoPos(ctx, k, "Paris", "London").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(2, len(positions)); err != nil {
			return err
		}
		// Paris coords should be close to 2.35, 48.86
		return AssertTrue(positions[0] != nil, "Paris position should not be nil")
	}))

	results = append(results, RunTest("GEODIST", cat, func() error {
		k := Key(prefix, "geo:dist")
		defer rdb.Del(ctx, k)
		rdb.GeoAdd(ctx, k,
			&redis.GeoLocation{Name: "Paris", Longitude: 2.3522, Latitude: 48.8566},
			&redis.GeoLocation{Name: "London", Longitude: -0.1278, Latitude: 51.5074},
		)
		dist, err := rdb.GeoDist(ctx, k, "Paris", "London", "km").Result()
		if err != nil {
			return err
		}
		// Distance is ~340km
		return AssertTrue(dist > 300 && dist < 400, "Paris-London distance should be ~340km")
	}))

	results = append(results, RunTest("GEOSEARCH", cat, func() error {
		k := Key(prefix, "geo:search")
		defer rdb.Del(ctx, k)
		rdb.GeoAdd(ctx, k,
			&redis.GeoLocation{Name: "Paris", Longitude: 2.3522, Latitude: 48.8566},
			&redis.GeoLocation{Name: "London", Longitude: -0.1278, Latitude: 51.5074},
			&redis.GeoLocation{Name: "Berlin", Longitude: 13.4050, Latitude: 52.5200},
			&redis.GeoLocation{Name: "Madrid", Longitude: -3.7038, Latitude: 40.4168},
		)
		results, err := rdb.GeoSearch(ctx, k, &redis.GeoSearchQuery{
			Longitude:  2.3522,
			Latitude:   48.8566,
			Radius:     500,
			RadiusUnit: "km",
			Sort:       "ASC",
		}).Result()
		if err != nil {
			return err
		}
		// Paris and London should be within 500km
		if err := AssertTrue(len(results) >= 1, "should find at least Paris"); err != nil {
			return err
		}
		return AssertEqual("Paris", results[0])
	}))

	results = append(results, RunTest("GEOHASH", cat, func() error {
		k := Key(prefix, "geo:hash")
		defer rdb.Del(ctx, k)
		rdb.GeoAdd(ctx, k, &redis.GeoLocation{Name: "Paris", Longitude: 2.3522, Latitude: 48.8566})
		hashes, err := rdb.GeoHash(ctx, k, "Paris").Result()
		if err != nil {
			return err
		}
		if err := AssertEqual(1, len(hashes)); err != nil {
			return err
		}
		// Geohash should be a non-empty string
		return AssertTrue(len(hashes[0]) > 0, "geohash should not be empty")
	}))

	// Check if the geohash starts with expected prefix for Paris area
	results = append(results, RunTest("GEOHASH_accuracy", cat, func() error {
		k := Key(prefix, "geo:hash_acc")
		defer rdb.Del(ctx, k)
		rdb.GeoAdd(ctx, k, &redis.GeoLocation{Name: "Paris", Longitude: 2.3522, Latitude: 48.8566})
		hashes, _ := rdb.GeoHash(ctx, k, "Paris").Result()
		// Paris geohash starts with "u09" typically
		return AssertTrue(strings.HasPrefix(hashes[0], "u0"), "Paris geohash should start with 'u0'")
	}))

	return results
}
