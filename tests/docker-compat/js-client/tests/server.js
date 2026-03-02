const Redis = require('ioredis');
const { register, runTest, key, assertEqual, assertTrue, assertEqualInt } = require('./framework');

register('server', async (redis, prefix) => {
  const results = [];
  const cat = 'server';

  results.push(await runTest('CONFIG_GET', cat, async () => {
    const res = await redis.config('GET', 'maxmemory');
    assertTrue(res.length > 0, 'CONFIG GET should return results');
  }));

  results.push(await runTest('CONFIG_SET_and_GET', cat, async () => {
    await redis.config('SET', 'maxmemory-policy', 'allkeys-lru');
    const res = await redis.config('GET', 'maxmemory-policy');
    // ioredis returns flat array: ['maxmemory-policy', 'allkeys-lru']
    assertEqual('allkeys-lru', res[1]);
  }));

  results.push(await runTest('FLUSHDB', cat, async () => {
    const k1 = key(prefix, 'srv:flush1');
    const k2 = key(prefix, 'srv:flush2');
    await redis.set(k1, 'a');
    await redis.set(k2, 'b');

    // Use a dedicated DB to avoid flushing test keys
    const rdb2 = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      db: 2,
      lazyConnect: true,
    });
    await rdb2.connect();
    try {
      await rdb2.set('flush_test', 'val');
      await rdb2.flushdb();
      const size = await rdb2.dbsize();
      assertEqualInt(0, size);
    } finally {
      rdb2.disconnect();
    }
    await redis.del(k1, k2);
  }));

  results.push(await runTest('TIME', cat, async () => {
    const t = await redis.time();
    assertTrue(Array.isArray(t) && t.length === 2, 'TIME should return [seconds, microseconds]');
    assertTrue(Number(t[0]) > 0, 'seconds should be positive');
  }));

  results.push(await runTest('RANDOMKEY', cat, async () => {
    const k = key(prefix, 'srv:randkey');
    await redis.set(k, 'val');
    const rk = await redis.randomkey();
    assertTrue(rk !== null && rk.length > 0, 'RANDOMKEY should return a key name');
    await redis.del(k);
  }));

  results.push(await runTest('COMMAND_COUNT', cat, async () => {
    const n = await redis.call('COMMAND', 'COUNT');
    assertTrue(Number(n) > 50, 'COMMAND COUNT should be > 50');
  }));

  results.push(await runTest('WAIT_with_timeout', cat, async () => {
    const n = await redis.call('WAIT', 0, 100);
    assertTrue(Number(n) >= 0, 'WAIT should return >= 0 replicas');
  }));

  return results;
});
