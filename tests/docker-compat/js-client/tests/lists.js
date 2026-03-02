const { register, runTest, key, assertEqual, assertEqualInt, assertArrayEqual } = require('./framework');

register('lists', async (redis, prefix) => {
  const results = [];
  const cat = 'lists';

  results.push(await runTest('LPUSH_RPUSH_single_and_multiple', cat, async () => {
    const k = key(prefix, 'list:push');
    await redis.lpush(k, 'a');
    await redis.rpush(k, 'b');
    await redis.lpush(k, 'c', 'd');  // d, c, a, b
    await redis.rpush(k, 'e', 'f');  // d, c, a, b, e, f
    const vals = await redis.lrange(k, 0, -1);
    assertArrayEqual(['d', 'c', 'a', 'b', 'e', 'f'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('LPOP_RPOP', cat, async () => {
    const k = key(prefix, 'list:pop');
    await redis.rpush(k, 'a', 'b', 'c');
    const left = await redis.lpop(k);
    assertEqual('a', left);
    const right = await redis.rpop(k);
    assertEqual('c', right);
    await redis.del(k);
  }));

  results.push(await runTest('LRANGE', cat, async () => {
    const k = key(prefix, 'list:range');
    await redis.rpush(k, 'a', 'b', 'c', 'd', 'e');
    const vals = await redis.lrange(k, 1, 3);
    assertArrayEqual(['b', 'c', 'd'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('LLEN', cat, async () => {
    const k = key(prefix, 'list:len');
    await redis.rpush(k, 'a', 'b', 'c');
    const n = await redis.llen(k);
    assertEqualInt(3, n);
    await redis.del(k);
  }));

  results.push(await runTest('LINDEX', cat, async () => {
    const k = key(prefix, 'list:idx');
    await redis.rpush(k, 'a', 'b', 'c');
    const val1 = await redis.lindex(k, 1);
    assertEqual('b', val1);
    const val2 = await redis.lindex(k, -1);
    assertEqual('c', val2);
    await redis.del(k);
  }));

  results.push(await runTest('LSET', cat, async () => {
    const k = key(prefix, 'list:set');
    await redis.rpush(k, 'a', 'b', 'c');
    await redis.lset(k, 1, 'B');
    const val = await redis.lindex(k, 1);
    assertEqual('B', val);
    await redis.del(k);
  }));

  results.push(await runTest('LINSERT_BEFORE_AFTER', cat, async () => {
    const k = key(prefix, 'list:ins');
    await redis.rpush(k, 'a', 'c');
    await redis.linsert(k, 'BEFORE', 'c', 'b');  // a, b, c
    await redis.linsert(k, 'AFTER', 'c', 'd');   // a, b, c, d
    const vals = await redis.lrange(k, 0, -1);
    assertArrayEqual(['a', 'b', 'c', 'd'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('LREM', cat, async () => {
    const k = key(prefix, 'list:rem');
    await redis.rpush(k, 'a', 'b', 'a', 'c', 'a');
    const n = await redis.lrem(k, 2, 'a');
    assertEqualInt(2, n);
    const vals = await redis.lrange(k, 0, -1);
    assertArrayEqual(['b', 'c', 'a'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('LTRIM', cat, async () => {
    const k = key(prefix, 'list:trim');
    await redis.rpush(k, 'a', 'b', 'c', 'd', 'e');
    await redis.ltrim(k, 1, 3);
    const vals = await redis.lrange(k, 0, -1);
    assertArrayEqual(['b', 'c', 'd'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('LPOS', cat, async () => {
    const k = key(prefix, 'list:pos');
    await redis.rpush(k, 'a', 'b', 'c', 'b', 'd');
    const pos = await redis.call('LPOS', k, 'b');
    assertEqualInt(1, pos);
    await redis.del(k);
  }));

  results.push(await runTest('LMPOP', cat, async () => {
    const k = key(prefix, 'list:mpop');
    await redis.rpush(k, 'a', 'b', 'c', 'd');
    const result = await redis.call('LMPOP', 1, k, 'LEFT', 'COUNT', 2);
    assertEqual(k, result[0]);
    assertArrayEqual(['a', 'b'], result[1]);
    await redis.del(k);
  }));

  return results;
});
