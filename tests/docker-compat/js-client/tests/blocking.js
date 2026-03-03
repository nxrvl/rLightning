const Redis = require('ioredis');
const { register, runTest, key, assertEqual, assertTrue, sleep } = require('./framework');

register('blocking', async (redis, prefix) => {
  const results = [];
  const cat = 'blocking';

  // Helper to create a second connection
  const createClient = () => {
    return new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
      retryStrategy: () => null,
      maxRetriesPerRequest: 3,
    });
  };

  // BLPOP_with_data: LPUSH then BLPOP, verify immediate return
  results.push(await runTest('BLPOP_with_data', cat, async () => {
    const k = key(prefix, 'block:blpop_data');
    try {
      await redis.rpush(k, 'val1');
      const result = await redis.blpop(k, 1);
      assertEqual(k, result[0]);
      assertEqual('val1', result[1]);
    } finally {
      await redis.del(k);
    }
  }));

  // BLPOP_timeout: BLPOP on empty key with 1s timeout, verify timeout return
  results.push(await runTest('BLPOP_timeout', cat, async () => {
    const k = key(prefix, 'block:blpop_timeout');
    try {
      const start = Date.now();
      const result = await redis.blpop(k, 1);
      const elapsed = Date.now() - start;

      assertTrue(result === null, 'BLPOP should return null on timeout');
      assertTrue(elapsed >= 800 && elapsed <= 2500,
        `Timeout duration out of range: ${elapsed}ms`);
    } finally {
      await redis.del(k);
    }
  }));

  // BLPOP_multi_key: BLPOP on multiple keys, LPUSH to second key, verify correct key returned
  results.push(await runTest('BLPOP_multi_key', cat, async () => {
    const k1 = key(prefix, 'block:multi1');
    const k2 = key(prefix, 'block:multi2');
    try {
      await redis.rpush(k2, 'from_k2');
      const result = await redis.blpop(k1, k2, 1);
      assertEqual(k2, result[0]);
      assertEqual('from_k2', result[1]);
    } finally {
      await redis.del(k1, k2);
    }
  }));

  // BRPOP_basic: Same as BLPOP but from right side
  results.push(await runTest('BRPOP_basic', cat, async () => {
    const k = key(prefix, 'block:brpop');
    try {
      await redis.rpush(k, 'first', 'second');
      const result = await redis.brpop(k, 1);
      assertEqual(k, result[0]);
      assertEqual('second', result[1]);
    } finally {
      await redis.del(k);
    }
  }));

  // BLMOVE_basic: BLMOVE from source to dest list, verify move
  results.push(await runTest('BLMOVE_basic', cat, async () => {
    const src = key(prefix, 'block:blmove_src');
    const dst = key(prefix, 'block:blmove_dst');
    try {
      await redis.rpush(src, 'item1');
      const val = await redis.blmove(src, dst, 'LEFT', 'RIGHT', 1);
      assertEqual('item1', val);

      // Verify item is in destination
      const dstVals = await redis.lrange(dst, 0, -1);
      assertEqual(1, dstVals.length);
      assertEqual('item1', dstVals[0]);
    } finally {
      await redis.del(src, dst);
    }
  }));

  // BLMPOP_basic: BLMPOP with LEFT/RIGHT direction
  results.push(await runTest('BLMPOP_basic', cat, async () => {
    const k = key(prefix, 'block:blmpop');
    try {
      await redis.rpush(k, 'a', 'b', 'c');
      // BLMPOP timeout numkeys key ... LEFT|RIGHT
      const result = await redis.call('BLMPOP', 1, 1, k, 'LEFT');
      assertTrue(Array.isArray(result), 'BLMPOP should return an array');
      assertEqual(k, result[0]);
      assertTrue(Array.isArray(result[1]), 'BLMPOP second element should be an array');
      assertEqual('a', result[1][0]);
    } finally {
      await redis.del(k);
    }
  }));

  // BZPOPMIN_basic: BZPOPMIN with data and timeout variants
  results.push(await runTest('BZPOPMIN_basic', cat, async () => {
    const k = key(prefix, 'block:bzpopmin');
    try {
      await redis.zadd(k, 1, 'a', 2, 'b');
      const result = await redis.bzpopmin(k, 1);
      assertTrue(Array.isArray(result), 'BZPOPMIN should return an array');
      assertEqual(k, result[0]);
      assertEqual('a', result[1]);
      assertEqual('1', result[2]);
    } finally {
      await redis.del(k);
    }
  }));

  // BZPOPMAX_basic: BZPOPMAX with data and timeout variants
  results.push(await runTest('BZPOPMAX_basic', cat, async () => {
    const k = key(prefix, 'block:bzpopmax');
    try {
      await redis.zadd(k, 1, 'a', 2, 'b');
      const result = await redis.bzpopmax(k, 1);
      assertTrue(Array.isArray(result), 'BZPOPMAX should return an array');
      assertEqual(k, result[0]);
      assertEqual('b', result[1]);
      assertEqual('2', result[2]);
    } finally {
      await redis.del(k);
    }
  }));

  // BLOCKING_concurrent: Producer-consumer pattern
  results.push(await runTest('BLOCKING_concurrent', cat, async () => {
    const k = key(prefix, 'block:concurrent');
    const client2 = createClient();
    await client2.connect();

    try {
      // Schedule push after delay on second connection
      const pushPromise = new Promise((resolve, reject) => {
        setTimeout(async () => {
          try {
            await client2.rpush(k, 'async_val');
            resolve();
          } catch (e) {
            reject(e);
          }
        }, 300);
      });

      // BLPOP should block then receive pushed value
      const result = await redis.blpop(k, 5);
      assertEqual(k, result[0]);
      assertEqual('async_val', result[1]);

      await pushPromise;
    } finally {
      client2.disconnect();
      await redis.del(k);
    }
  }));

  // BLOCKING_timeout_accuracy: Verify timeout duration is within acceptable range
  results.push(await runTest('BLOCKING_timeout_accuracy', cat, async () => {
    const k = key(prefix, 'block:timeout_accuracy');
    try {
      const start = Date.now();
      const result = await redis.blpop(k, 1);
      const elapsed = Date.now() - start;

      assertTrue(result === null, 'BLPOP should return null on timeout');
      const diff = Math.abs(elapsed - 1000);
      assertTrue(diff <= 500,
        `Timeout accuracy out of range: expected ~1000ms, got ${elapsed}ms (diff: ${diff}ms)`);
    } finally {
      await redis.del(k);
    }
  }));

  return results;
});
