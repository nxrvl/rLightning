const Redis = require('ioredis');
const { register, runTest, key, assertEqual, assertTrue, assertEqualInt, sleep } = require('./framework');

register('js_specific', async (redis, prefix) => {
  const results = [];
  const cat = 'js_specific';

  results.push(await runTest('ioredis_auto_reconnect', cat, async () => {
    // Create client with auto-reconnect enabled (default)
    const client = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
      retryStrategy: (times) => {
        if (times > 3) return null;
        return 100;
      },
    });
    await client.connect();
    try {
      const k = key(prefix, 'jsspec:reconnect');
      await client.set(k, 'reconnect_test');
      const val = await client.get(k);
      assertEqual('reconnect_test', val);
      await client.del(k);
    } finally {
      client.disconnect();
    }
  }));

  results.push(await runTest('buffer_binary_data', cat, async () => {
    const k = key(prefix, 'jsspec:buffer');
    const buf = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd]);
    await redis.set(k, buf);
    const result = await redis.getBuffer(k);
    assertTrue(Buffer.isBuffer(result), 'getBuffer should return a Buffer');
    assertTrue(Buffer.compare(buf, result) === 0, 'binary data should round-trip through Buffers');
    await redis.del(k);
  }));

  results.push(await runTest('lazyConnect_behavior', cat, async () => {
    const client = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    // Before connect, status should be 'wait'
    assertEqual('wait', client.status);
    await client.connect();
    assertEqual('ready', client.status);
    const k = key(prefix, 'jsspec:lazy');
    await client.set(k, 'lazy_val');
    const val = await client.get(k);
    assertEqual('lazy_val', val);
    await client.del(k);
    client.disconnect();
  }));

  results.push(await runTest('transformer_result_handling', cat, async () => {
    // Test that ioredis correctly transforms command results
    const k = key(prefix, 'jsspec:transform');
    await redis.hset(k, 'a', '1', 'b', '2');

    // hgetall returns an object (ioredis transforms flat array to object)
    const obj = await redis.hgetall(k);
    assertTrue(typeof obj === 'object', 'hgetall should return object');
    assertEqual('1', obj.a);
    assertEqual('2', obj.b);

    // smembers returns array
    const sk = key(prefix, 'jsspec:transform_set');
    await redis.sadd(sk, 'x', 'y', 'z');
    const members = await redis.smembers(sk);
    assertTrue(Array.isArray(members), 'smembers should return array');
    assertEqualInt(3, members.length);

    await redis.del(k, sk);
  }));

  results.push(await runTest('pipeline_chaining', cat, async () => {
    const k = key(prefix, 'jsspec:pipe_chain');
    // ioredis supports method chaining on pipelines
    const result = await redis.pipeline()
      .set(k, 'chain_val')
      .get(k)
      .strlen(k)
      .del(k)
      .exec();

    assertTrue(result[0][0] === null, 'SET should succeed');
    assertEqual('chain_val', result[1][1]);
    assertEqualInt(9, result[2][1]); // 'chain_val'.length
    assertEqualInt(1, result[3][1]); // DEL returns 1
  }));

  return results;
});
