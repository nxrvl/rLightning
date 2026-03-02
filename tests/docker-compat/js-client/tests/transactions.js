const Redis = require('ioredis');
const { register, runTest, key, assertEqual, assertTrue } = require('./framework');

register('transactions', async (redis, prefix) => {
  const results = [];
  const cat = 'transactions';

  results.push(await runTest('MULTI_EXEC_basic', cat, async () => {
    const k1 = key(prefix, 'tx:basic1');
    const k2 = key(prefix, 'tx:basic2');
    const result = await redis.multi()
      .set(k1, 'a')
      .set(k2, 'b')
      .exec();
    // exec returns [[null, 'OK'], [null, 'OK']]
    assertTrue(result !== null, 'EXEC should return results');
    const v1 = await redis.get(k1);
    const v2 = await redis.get(k2);
    assertEqual('a', v1);
    assertEqual('b', v2);
    await redis.del(k1, k2);
  }));

  results.push(await runTest('MULTI_DISCARD', cat, async () => {
    const k = key(prefix, 'tx:discard');
    await redis.set(k, 'original');
    // Use a dedicated connection to ensure MULTI/DISCARD run on the same connection
    const conn = redis.duplicate();
    try {
      await conn.connect();
      await conn.call('MULTI');
      await conn.call('SET', k, 'changed');
      await conn.call('DISCARD');
    } finally {
      conn.disconnect();
    }
    const val = await redis.get(k);
    assertEqual('original', val);
    await redis.del(k);
  }));

  results.push(await runTest('WATCH_UNWATCH_optimistic_lock', cat, async () => {
    const k = key(prefix, 'tx:watch');
    await redis.set(k, '10');

    // WATCH + no modification = EXEC should succeed
    await redis.watch(k);
    const val = await redis.get(k);
    assertEqual('10', val);
    const result = await redis.multi()
      .set(k, '20')
      .exec();
    assertTrue(result !== null, 'EXEC should succeed when watched key not modified');
    const newVal = await redis.get(k);
    assertEqual('20', newVal);
    await redis.del(k);
  }));

  results.push(await runTest('WATCH_key_modification_detection', cat, async () => {
    const k = key(prefix, 'tx:watchmod');
    await redis.set(k, 'original');

    const rdb2 = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await rdb2.connect();

    try {
      await redis.watch(k);
      // Another client modifies the watched key
      await rdb2.set(k, 'modified_by_other');
      const result = await redis.multi()
        .set(k, 'should_fail')
        .exec();
      // When WATCH detects modification, exec() returns null
      assertTrue(result === null, 'EXEC should return null when watched key was modified');
      const val = await redis.get(k);
      assertEqual('modified_by_other', val);
    } finally {
      rdb2.disconnect();
      await redis.del(k);
    }
  }));

  results.push(await runTest('queued_command_errors', cat, async () => {
    const k = key(prefix, 'tx:qerr');
    await redis.set(k, 'string_value');
    const result = await redis.multi()
      .set(k, 'new_value')
      .lpush(k, 'bad')  // This will fail because k is a string
      .exec();
    // In ioredis, exec returns array with [err, result] for each command
    assertTrue(result !== null, 'EXEC should return results');
    assertTrue(result[0][0] === null, 'SET should succeed');
    assertTrue(result[1][0] !== null, 'LPUSH should fail with WRONGTYPE');
    await redis.del(k);
  }));

  return results;
});
