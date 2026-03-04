"""Category 13: Advanced Data Types tests (Bitmap, HyperLogLog, Geo)."""

from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int, assert_equal_float,
)


def test_advanced(r, prefix):
    results = []
    cat = "advanced_types"

    # --- Bitmap ---

    def t_setbit_getbit():
        k = key(prefix, "bm:sg")
        try:
            old = r.setbit(k, 7, 1)
            assert_equal_int(0, old)
            val = r.getbit(k, 7)
            assert_equal_int(1, val)
            val = r.getbit(k, 0)
            assert_equal_int(0, val)
        finally:
            r.delete(k)
    results.append(run_test("SETBIT_GETBIT", cat, t_setbit_getbit))

    def t_bitcount():
        k = key(prefix, "bm:count")
        try:
            r.set(k, "foobar")
            n = r.bitcount(k)
            assert_equal_int(26, n)
        finally:
            r.delete(k)
    results.append(run_test("BITCOUNT", cat, t_bitcount))

    def t_bitop():
        k1 = key(prefix, "bm:op1")
        k2 = key(prefix, "bm:op2")
        dest = key(prefix, "bm:opdest")
        try:
            r.set(k1, "abc")
            r.set(k2, "def")
            n = r.bitop("AND", dest, k1, k2)
            assert_equal_int(3, n)
        finally:
            r.delete(k1, k2, dest)
    results.append(run_test("BITOP", cat, t_bitop))

    def t_bitpos():
        k = key(prefix, "bm:pos")
        try:
            r.setbit(k, 0, 0)
            r.setbit(k, 1, 0)
            r.setbit(k, 2, 1)
            pos = r.bitpos(k, 1)
            assert_equal_int(2, pos)
        finally:
            r.delete(k)
    results.append(run_test("BITPOS", cat, t_bitpos))

    # --- HyperLogLog ---

    def t_pfadd_pfcount():
        k = key(prefix, "hll:basic")
        try:
            n = r.pfadd(k, "a", "b", "c", "d")
            assert_true(n > 0, "PFADD should return 1 for new elements")
            count = r.pfcount(k)
            assert_true(3 <= count <= 5, "PFCOUNT should be ~4")
        finally:
            r.delete(k)
    results.append(run_test("PFADD_PFCOUNT", cat, t_pfadd_pfcount))

    def t_pfmerge():
        k1 = key(prefix, "hll:m1")
        k2 = key(prefix, "hll:m2")
        dest = key(prefix, "hll:merged")
        try:
            r.pfadd(k1, "a", "b", "c")
            r.pfadd(k2, "c", "d", "e")
            r.pfmerge(dest, k1, k2)
            count = r.pfcount(dest)
            assert_true(4 <= count <= 6, "merged count should be ~5")
        finally:
            r.delete(k1, k2, dest)
    results.append(run_test("PFMERGE", cat, t_pfmerge))

    # --- Geo ---

    def t_geoadd_geopos():
        k = key(prefix, "geo:cities")
        try:
            n = r.geoadd(k, [2.3522, 48.8566, "Paris", -.1278, 51.5074, "London", 13.4050, 52.5200, "Berlin"])
            assert_equal_int(3, n)
            positions = r.geopos(k, "Paris", "London")
            assert_equal(2, len(positions))
            assert_true(positions[0] is not None, "Paris position should not be None")
        finally:
            r.delete(k)
    results.append(run_test("GEOADD_GEOPOS", cat, t_geoadd_geopos))

    def t_geodist():
        k = key(prefix, "geo:dist")
        try:
            r.geoadd(k, [2.3522, 48.8566, "Paris", -.1278, 51.5074, "London"])
            dist = r.geodist(k, "Paris", "London", unit="km")
            assert_true(300 < float(dist) < 400, "Paris-London distance should be ~340km")
        finally:
            r.delete(k)
    results.append(run_test("GEODIST", cat, t_geodist))

    def t_geosearch():
        k = key(prefix, "geo:search")
        try:
            r.geoadd(k, [
                2.3522, 48.8566, "Paris",
                -.1278, 51.5074, "London",
                13.4050, 52.5200, "Berlin",
                -3.7038, 40.4168, "Madrid",
            ])
            results_geo = r.geosearch(k, longitude=2.3522, latitude=48.8566, radius=500, unit="km", sort="ASC")
            assert_true(len(results_geo) >= 1, "should find at least Paris")
            assert_equal("Paris", results_geo[0])
        finally:
            r.delete(k)
    results.append(run_test("GEOSEARCH", cat, t_geosearch))

    def t_geohash():
        k = key(prefix, "geo:hash")
        try:
            r.geoadd(k, [2.3522, 48.8566, "Paris"])
            hashes = r.geohash(k, "Paris")
            assert_equal(1, len(hashes))
            assert_true(len(hashes[0]) > 0, "geohash should not be empty")
        finally:
            r.delete(k)
    results.append(run_test("GEOHASH", cat, t_geohash))

    def t_geohash_accuracy():
        k = key(prefix, "geo:hash_acc")
        try:
            r.geoadd(k, [2.3522, 48.8566, "Paris"])
            hashes = r.geohash(k, "Paris")
            assert_true(hashes[0].startswith("u0"), "Paris geohash should start with 'u0'")
        finally:
            r.delete(k)
    results.append(run_test("GEOHASH_accuracy", cat, t_geohash_accuracy))

    return results


register("advanced_types", test_advanced)
