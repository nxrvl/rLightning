const { register, runTest, key, assertEqual, assertTrue, assertEqualInt, assertEqualFloat, assertArrayEqual } = require('./framework');

register('sorted_sets', async (redis, prefix) => {
  const results = [];
  const cat = 'sorted_sets';

  results.push(await runTest('ZADD_single_and_multiple', cat, async () => {
    const k = key(prefix, 'zset:add');
    const n1 = await redis.zadd(k, 1, 'a');
    assertEqualInt(1, n1);
    const n2 = await redis.zadd(k, 2, 'b', 3, 'c');
    assertEqualInt(2, n2);
    await redis.del(k);
  }));

  results.push(await runTest('ZADD_NX_XX_GT_LT', cat, async () => {
    const k = key(prefix, 'zset:flags');
    await redis.zadd(k, 5, 'a');

    // NX: only add new
    const n = await redis.zadd(k, 'NX', 10, 'a');
    assertEqualInt(0, n);
    const score1 = await redis.zscore(k, 'a');
    assertEqualFloat(5, parseFloat(score1), 0.001);

    // XX: only update existing
    await redis.zadd(k, 'XX', 10, 'a');
    const score2 = await redis.zscore(k, 'a');
    assertEqualFloat(10, parseFloat(score2), 0.001);

    // GT: only update if new score > current
    await redis.zadd(k, 'GT', 5, 'a');
    const score3 = await redis.zscore(k, 'a');
    assertEqualFloat(10, parseFloat(score3), 0.001);

    // LT: only update if new score < current
    await redis.zadd(k, 'LT', 3, 'a');
    const score4 = await redis.zscore(k, 'a');
    assertEqualFloat(3, parseFloat(score4), 0.001);

    await redis.del(k);
  }));

  results.push(await runTest('ZSCORE_ZMSCORE', cat, async () => {
    const k = key(prefix, 'zset:score');
    await redis.zadd(k, 1.5, 'a', 2.5, 'b');
    const score = await redis.zscore(k, 'a');
    assertEqualFloat(1.5, parseFloat(score), 0.001);
    const scores = await redis.call('ZMSCORE', k, 'a', 'b', 'nonexist');
    assertEqualFloat(1.5, parseFloat(scores[0]), 0.001);
    assertEqualFloat(2.5, parseFloat(scores[1]), 0.001);
    await redis.del(k);
  }));

  results.push(await runTest('ZRANGE_basic', cat, async () => {
    const k = key(prefix, 'zset:range');
    await redis.zadd(k, 1, 'a', 2, 'b', 3, 'c');
    const vals = await redis.zrange(k, 0, -1);
    assertArrayEqual(['a', 'b', 'c'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('ZRANGEBYSCORE', cat, async () => {
    const k = key(prefix, 'zset:rbyscore');
    await redis.zadd(k, 1, 'a', 2, 'b', 3, 'c', 4, 'd');
    const vals = await redis.zrangebyscore(k, 2, 3);
    assertArrayEqual(['b', 'c'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('ZREVRANGEBYSCORE', cat, async () => {
    const k = key(prefix, 'zset:rrbyscore');
    await redis.zadd(k, 1, 'a', 2, 'b', 3, 'c');
    const vals = await redis.zrevrangebyscore(k, 3, 1);
    assertArrayEqual(['c', 'b', 'a'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('ZRANK_ZREVRANK', cat, async () => {
    const k = key(prefix, 'zset:rank');
    await redis.zadd(k, 1, 'a', 2, 'b', 3, 'c');
    const rank = await redis.zrank(k, 'b');
    assertEqualInt(1, rank);
    const rrank = await redis.zrevrank(k, 'b');
    assertEqualInt(1, rrank);
    await redis.del(k);
  }));

  results.push(await runTest('ZREM_ZREMRANGEBYSCORE_ZREMRANGEBYRANK', cat, async () => {
    const k = key(prefix, 'zset:rem');
    await redis.zadd(k, 1, 'a', 2, 'b', 3, 'c', 4, 'd', 5, 'e');
    const n1 = await redis.zrem(k, 'a');
    assertEqualInt(1, n1);
    const n2 = await redis.zremrangebyscore(k, 2, 3);
    assertEqualInt(2, n2);
    const n3 = await redis.zremrangebyrank(k, 0, 0);
    assertEqualInt(1, n3); // removes "d"
    await redis.del(k);
  }));

  results.push(await runTest('ZCARD_ZCOUNT', cat, async () => {
    const k = key(prefix, 'zset:card');
    await redis.zadd(k, 1, 'a', 2, 'b', 3, 'c');
    const card = await redis.zcard(k);
    assertEqualInt(3, card);
    const count = await redis.zcount(k, 1, 2);
    assertEqualInt(2, count);
    await redis.del(k);
  }));

  results.push(await runTest('ZINCRBY', cat, async () => {
    const k = key(prefix, 'zset:incr');
    await redis.zadd(k, 10, 'a');
    const newScore = await redis.zincrby(k, 5, 'a');
    assertEqualFloat(15, parseFloat(newScore), 0.001);
    await redis.del(k);
  }));

  results.push(await runTest('ZUNIONSTORE_ZINTERSTORE', cat, async () => {
    const k1 = key(prefix, 'zset:u1');
    const k2 = key(prefix, 'zset:u2');
    const dest1 = key(prefix, 'zset:union');
    const dest2 = key(prefix, 'zset:inter');
    await redis.zadd(k1, 1, 'a', 2, 'b');
    await redis.zadd(k2, 3, 'b', 4, 'c');

    const n1 = await redis.zunionstore(dest1, 2, k1, k2);
    assertEqualInt(3, n1);
    const score = await redis.zscore(dest1, 'b');
    assertEqualFloat(5, parseFloat(score), 0.001);

    const n2 = await redis.zinterstore(dest2, 2, k1, k2);
    assertEqualInt(1, n2);

    await redis.del(k1, k2, dest1, dest2);
  }));

  results.push(await runTest('ZRANDMEMBER', cat, async () => {
    const k = key(prefix, 'zset:rand');
    await redis.zadd(k, 1, 'a', 2, 'b', 3, 'c');
    const members = await redis.call('ZRANDMEMBER', k, 1);
    const member = Array.isArray(members) ? members[0] : members;
    const valid = member === 'a' || member === 'b' || member === 'c';
    assertTrue(valid, 'member should be a, b, or c');
    await redis.del(k);
  }));

  results.push(await runTest('ZSCAN', cat, async () => {
    const k = key(prefix, 'zset:scan');
    for (let i = 0; i < 20; i++) {
      await redis.zadd(k, i, `m${i}`);
    }
    const all = [];
    let cursor = '0';
    do {
      const [nextCursor, data] = await redis.zscan(k, cursor, 'MATCH', '*', 'COUNT', 10);
      cursor = nextCursor;
      for (let i = 0; i < data.length; i += 2) {
        all.push(data[i]);
      }
    } while (cursor !== '0');
    assertEqualInt(20, all.length);
    await redis.del(k);
  }));

  results.push(await runTest('ZPOPMIN_ZPOPMAX', cat, async () => {
    const k = key(prefix, 'zset:pop');
    await redis.zadd(k, 1, 'a', 2, 'b', 3, 'c');
    const min = await redis.zpopmin(k, 1);
    assertEqual('a', min[0]);
    const max = await redis.zpopmax(k, 1);
    assertEqual('c', max[0]);
    await redis.del(k);
  }));

  return results;
});
