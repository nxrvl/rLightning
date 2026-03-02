const { register, runTest, key, assertEqual, assertTrue, assertEqualInt, assertArrayEqual } = require('./framework');

register('pipeline', async (redis, prefix) => {
  const results = [];
  const cat = 'pipeline';

  results.push(await runTest('pipeline_100_SETs', cat, async () => {
    const pipe = redis.pipeline();
    const keys = [];
    for (let i = 0; i < 100; i++) {
      const k = key(prefix, `pipe:set_${i}`);
      keys.push(k);
      pipe.set(k, String(i));
    }
    const result = await pipe.exec();
    assertEqualInt(100, result.length);
    // Cleanup
    for (const k of keys) {
      await redis.del(k);
    }
  }));

  results.push(await runTest('pipeline_100_GETs', cat, async () => {
    const keys = [];
    for (let i = 0; i < 100; i++) {
      const k = key(prefix, `pipe:get_${i}`);
      keys.push(k);
      await redis.set(k, String(i));
    }

    const pipe = redis.pipeline();
    for (const k of keys) {
      pipe.get(k);
    }
    const result = await pipe.exec();

    for (let i = 0; i < 100; i++) {
      assertTrue(result[i][0] === null, `GET ${i} should not error`);
      assertEqual(String(i), result[i][1]);
    }

    for (const k of keys) {
      await redis.del(k);
    }
  }));

  results.push(await runTest('pipeline_mixed_commands', cat, async () => {
    const k1 = key(prefix, 'pipe:mix1');
    const k2 = key(prefix, 'pipe:mix2');
    const k3 = key(prefix, 'pipe:mix3');

    const result = await redis.pipeline()
      .set(k1, 'hello')
      .incr(k2)
      .lpush(k3, 'a', 'b')
      .get(k1)
      .lrange(k3, 0, -1)
      .exec();

    assertTrue(result[0][0] === null, 'SET should succeed');
    assertEqualInt(1, result[1][1]); // INCR result
    assertEqualInt(2, result[2][1]); // LPUSH result
    assertEqual('hello', result[3][1]); // GET result
    assertArrayEqual(['b', 'a'], result[4][1]); // LRANGE result

    await redis.del(k1, k2, k3);
  }));

  results.push(await runTest('pipeline_with_errors', cat, async () => {
    const k = key(prefix, 'pipe:err');
    await redis.set(k, 'string_val');

    const result = await redis.pipeline()
      .get(k)
      .lpush(k, 'bad')  // WRONGTYPE error
      .exec();

    // GET should succeed
    assertTrue(result[0][0] === null, 'GET should succeed');
    assertEqual('string_val', result[0][1]);

    // LPUSH should fail with WRONGTYPE
    assertTrue(result[1][0] !== null, 'LPUSH should fail with WRONGTYPE');

    await redis.del(k);
  }));

  results.push(await runTest('pipeline_result_ordering', cat, async () => {
    const keys = [];
    for (let i = 0; i < 10; i++) {
      const k = key(prefix, `pipe:order_${i}`);
      keys.push(k);
      await redis.set(k, `val_${i}`);
    }

    const pipe = redis.pipeline();
    for (const k of keys) {
      pipe.get(k);
    }
    const result = await pipe.exec();

    for (let i = 0; i < 10; i++) {
      assertTrue(result[i][0] === null, `cmd ${i} should not error`);
      assertEqual(`val_${i}`, result[i][1]);
    }

    for (const k of keys) {
      await redis.del(k);
    }
  }));

  return results;
});
