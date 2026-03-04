const { register, runTest, key, assertEqual, assertTrue, assertEqualInt, assertEqualFloat } = require('./framework');

register('advanced_types', async (redis, prefix) => {
  const results = [];
  const cat = 'advanced_types';

  // --- Bitmap ---

  results.push(await runTest('SETBIT_GETBIT', cat, async () => {
    const k = key(prefix, 'bm:sg');
    const old = await redis.setbit(k, 7, 1);
    assertEqualInt(0, old);
    const val = await redis.getbit(k, 7);
    assertEqualInt(1, val);
    const val0 = await redis.getbit(k, 0);
    assertEqualInt(0, val0);
    await redis.del(k);
  }));

  results.push(await runTest('BITCOUNT', cat, async () => {
    const k = key(prefix, 'bm:count');
    await redis.set(k, 'foobar');
    const n = await redis.bitcount(k);
    assertEqualInt(26, n);
    await redis.del(k);
  }));

  results.push(await runTest('BITOP', cat, async () => {
    const k1 = key(prefix, 'bm:op1');
    const k2 = key(prefix, 'bm:op2');
    const dest = key(prefix, 'bm:opdest');
    await redis.set(k1, 'abc');
    await redis.set(k2, 'def');
    const n = await redis.bitop('AND', dest, k1, k2);
    assertEqualInt(3, n);
    await redis.del(k1, k2, dest);
  }));

  results.push(await runTest('BITPOS', cat, async () => {
    const k = key(prefix, 'bm:pos');
    await redis.setbit(k, 0, 0);
    await redis.setbit(k, 1, 0);
    await redis.setbit(k, 2, 1);
    const pos = await redis.bitpos(k, 1);
    assertEqualInt(2, pos);
    await redis.del(k);
  }));

  // --- HyperLogLog ---

  results.push(await runTest('PFADD_PFCOUNT', cat, async () => {
    const k = key(prefix, 'hll:basic');
    const n = await redis.pfadd(k, 'a', 'b', 'c', 'd');
    assertTrue(n > 0, 'PFADD should return 1 for new elements');
    const count = await redis.pfcount(k);
    assertTrue(count >= 3 && count <= 5, 'PFCOUNT should be ~4');
    await redis.del(k);
  }));

  results.push(await runTest('PFMERGE', cat, async () => {
    const k1 = key(prefix, 'hll:m1');
    const k2 = key(prefix, 'hll:m2');
    const dest = key(prefix, 'hll:merged');
    await redis.pfadd(k1, 'a', 'b', 'c');
    await redis.pfadd(k2, 'c', 'd', 'e');
    await redis.pfmerge(dest, k1, k2);
    const count = await redis.pfcount(dest);
    assertTrue(count >= 4 && count <= 6, 'merged count should be ~5');
    await redis.del(k1, k2, dest);
  }));

  // --- Geo ---

  results.push(await runTest('GEOADD_GEOPOS', cat, async () => {
    const k = key(prefix, 'geo:cities');
    const n = await redis.geoadd(k, 2.3522, 48.8566, 'Paris', -0.1278, 51.5074, 'London', 13.4050, 52.5200, 'Berlin');
    assertEqualInt(3, n);
    const positions = await redis.geopos(k, 'Paris', 'London');
    assertEqualInt(2, positions.length);
    assertTrue(positions[0] !== null, 'Paris position should not be null');
    await redis.del(k);
  }));

  results.push(await runTest('GEODIST', cat, async () => {
    const k = key(prefix, 'geo:dist');
    await redis.geoadd(k, 2.3522, 48.8566, 'Paris', -0.1278, 51.5074, 'London');
    const dist = await redis.geodist(k, 'Paris', 'London', 'km');
    const d = parseFloat(dist);
    assertTrue(d > 300 && d < 400, 'Paris-London distance should be ~340km');
    await redis.del(k);
  }));

  results.push(await runTest('GEOSEARCH', cat, async () => {
    const k = key(prefix, 'geo:search');
    await redis.geoadd(k, 2.3522, 48.8566, 'Paris', -0.1278, 51.5074, 'London', 13.4050, 52.5200, 'Berlin', -3.7038, 40.4168, 'Madrid');
    const members = await redis.call('GEOSEARCH', k, 'FROMLONLAT', 2.3522, 48.8566, 'BYRADIUS', 500, 'km', 'ASC');
    assertTrue(members.length >= 1, 'should find at least Paris');
    assertEqual('Paris', members[0]);
    await redis.del(k);
  }));

  results.push(await runTest('GEOHASH', cat, async () => {
    const k = key(prefix, 'geo:hash');
    await redis.geoadd(k, 2.3522, 48.8566, 'Paris');
    const hashes = await redis.geohash(k, 'Paris');
    assertEqualInt(1, hashes.length);
    assertTrue(hashes[0].length > 0, 'geohash should not be empty');
    await redis.del(k);
  }));

  results.push(await runTest('GEOHASH_accuracy', cat, async () => {
    const k = key(prefix, 'geo:hash_acc');
    await redis.geoadd(k, 2.3522, 48.8566, 'Paris');
    const hashes = await redis.geohash(k, 'Paris');
    assertTrue(hashes[0].startsWith('u0'), "Paris geohash should start with 'u0'");
    await redis.del(k);
  }));

  return results;
});
