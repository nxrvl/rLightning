const { register, runTest, key, assertEqual, assertTrue, sleep } = require('./framework');

register('memory', async (redis, prefix) => {
  const results = [];
  const cat = 'memory';

  // MEMORY_USAGE_basic: SET key, verify MEMORY USAGE returns reasonable byte count
  results.push(await runTest('MEMORY_USAGE_basic', cat, async () => {
    const k = key(prefix, 'mem:usage_basic');
    try {
      await redis.set(k, 'hello world');
      const usage = await redis.call('MEMORY', 'USAGE', k);
      assertTrue(usage > 0, `MEMORY USAGE should be > 0, got ${usage}`);
      assertTrue(usage < 1024, `MEMORY USAGE should be < 1024 bytes for small string, got ${usage}`);
    } finally {
      await redis.del(k);
    }
  }));

  // MEMORY_USAGE_types: Test MEMORY USAGE for string, hash, list, set, zset
  results.push(await runTest('MEMORY_USAGE_types', cat, async () => {
    const kStr = key(prefix, 'mem:type_str');
    const kHash = key(prefix, 'mem:type_hash');
    const kList = key(prefix, 'mem:type_list');
    const kSet = key(prefix, 'mem:type_set');
    const kZset = key(prefix, 'mem:type_zset');
    try {
      await redis.set(kStr, 'value');
      await redis.hset(kHash, 'field1', 'val1', 'field2', 'val2');
      await redis.rpush(kList, 'a', 'b', 'c');
      await redis.sadd(kSet, 'm1', 'm2', 'm3');
      await redis.zadd(kZset, 1.0, 'x', 2.0, 'y');

      const types = { [kStr]: 'string', [kHash]: 'hash', [kList]: 'list', [kSet]: 'set', [kZset]: 'zset' };
      for (const [k, typeName] of Object.entries(types)) {
        const usage = await redis.call('MEMORY', 'USAGE', k);
        assertTrue(usage > 0, `MEMORY USAGE for ${typeName} should be > 0, got ${usage}`);
      }
    } finally {
      await redis.del(kStr, kHash, kList, kSet, kZset);
    }
  }));

  // MEMORY_DOCTOR: Run MEMORY DOCTOR, verify response format
  results.push(await runTest('MEMORY_DOCTOR', cat, async () => {
    const result = await redis.call('MEMORY', 'DOCTOR');
    assertTrue(typeof result === 'string', `MEMORY DOCTOR should return string, got ${typeof result}`);
    assertTrue(result.length > 0, 'MEMORY DOCTOR response should not be empty');
  }));

  // INFO_memory_section: Verify INFO memory section contains used_memory, used_memory_peak
  results.push(await runTest('INFO_memory_section', cat, async () => {
    // Ensure some data exists so used_memory > 0 on all implementations
    const kInfo = key(prefix, 'mem:info_data');
    await redis.set(kInfo, 'x'.repeat(100));
    try {
    const info = await redis.info('memory');
    assertTrue(info.includes('used_memory:'), 'INFO memory missing used_memory');
    assertTrue(info.includes('used_memory_peak:'), 'INFO memory missing used_memory_peak');

    // Verify used_memory is a positive number
    const match = info.match(/used_memory:(\d+)/);
    assertTrue(match !== null, 'Could not parse used_memory from INFO');
    const usedMemory = parseInt(match[1], 10);
    assertTrue(usedMemory > 0, `used_memory should be > 0, got ${usedMemory}`);
    } finally {
      await redis.del(kInfo);
    }
  }));

  // CONFIG_maxmemory: CONFIG SET maxmemory, verify it takes effect
  results.push(await runTest('CONFIG_maxmemory', cat, async () => {
    // Save original maxmemory
    const origConfig = await redis.config('GET', 'maxmemory');
    const origMaxmem = origConfig[1] || '0';
    try {
      await redis.config('SET', 'maxmemory', '104857600');
      const vals = await redis.config('GET', 'maxmemory');
      assertEqual('104857600', vals[1]);
    } finally {
      await redis.config('SET', 'maxmemory', origMaxmem);
    }
  }));

  // CONFIG_maxmemory_policy: Test maxmemory-policy configuration
  results.push(await runTest('CONFIG_maxmemory_policy', cat, async () => {
    const origConfig = await redis.config('GET', 'maxmemory-policy');
    const origPolicy = origConfig[1] || 'noeviction';
    try {
      const policies = ['noeviction', 'allkeys-lru', 'volatile-lru', 'allkeys-random'];
      for (const policy of policies) {
        await redis.config('SET', 'maxmemory-policy', policy);
        const vals = await redis.config('GET', 'maxmemory-policy');
        assertEqual(policy, vals[1]);
      }
    } finally {
      await redis.config('SET', 'maxmemory-policy', origPolicy);
    }
  }));

  // OBJECT_ENCODING: Verify encoding changes based on data size
  results.push(await runTest('OBJECT_ENCODING', cat, async () => {
    const kInt = key(prefix, 'mem:enc_int');
    const kStr = key(prefix, 'mem:enc_str');
    const kEmbstr = key(prefix, 'mem:enc_embstr');
    const kList = key(prefix, 'mem:enc_list');
    const kSet = key(prefix, 'mem:enc_set');
    const kHash = key(prefix, 'mem:enc_hash');
    const kZset = key(prefix, 'mem:enc_zset');
    try {
      // Integer encoding
      await redis.set(kInt, '12345');
      let enc = await redis.object('ENCODING', kInt);
      assertEqual('int', enc);

      // Embstr encoding for short strings
      await redis.set(kEmbstr, 'hello');
      enc = await redis.object('ENCODING', kEmbstr);
      assertEqual('embstr', enc);

      // Raw encoding for long strings
      await redis.set(kStr, 'x'.repeat(100));
      enc = await redis.object('ENCODING', kStr);
      assertEqual('raw', enc);

      // Small list -> listpack
      await redis.rpush(kList, 'a', 'b', 'c');
      enc = await redis.object('ENCODING', kList);
      assertTrue(enc === 'listpack' || enc === 'ziplist',
        `small list should be listpack or ziplist, got ${enc}`);

      // Small set -> listpack
      await redis.sadd(kSet, 'm1', 'm2', 'm3');
      enc = await redis.object('ENCODING', kSet);
      assertTrue(enc === 'listpack' || enc === 'ziplist',
        `small set should be listpack or ziplist, got ${enc}`);

      // Small hash -> listpack
      await redis.hset(kHash, 'f1', 'v1', 'f2', 'v2');
      enc = await redis.object('ENCODING', kHash);
      assertTrue(enc === 'listpack' || enc === 'ziplist',
        `small hash should be listpack or ziplist, got ${enc}`);

      // Small zset -> listpack
      await redis.zadd(kZset, 1.0, 'a', 2.0, 'b');
      enc = await redis.object('ENCODING', kZset);
      assertTrue(enc === 'listpack' || enc === 'ziplist',
        `small zset should be listpack or ziplist, got ${enc}`);
    } finally {
      await redis.del(kInt, kStr, kEmbstr, kList, kSet, kHash, kZset);
    }
  }));

  // OBJECT_REFCOUNT: Verify OBJECT REFCOUNT returns integer
  results.push(await runTest('OBJECT_REFCOUNT', cat, async () => {
    const k = key(prefix, 'mem:refcount');
    try {
      await redis.set(k, 'testvalue');
      const refcount = await redis.object('REFCOUNT', k);
      assertTrue(Number.isInteger(Number(refcount)), `OBJECT REFCOUNT should return integer, got ${refcount}`);
      assertTrue(Number(refcount) >= 1, `OBJECT REFCOUNT should be >= 1, got ${refcount}`);
    } finally {
      await redis.del(k);
    }
  }));

  // OBJECT_IDLETIME: SET key, verify OBJECT IDLETIME reflects elapsed time
  results.push(await runTest('OBJECT_IDLETIME', cat, async () => {
    const k = key(prefix, 'mem:idletime');
    try {
      await redis.set(k, 'testvalue');
      const idletime = await redis.object('IDLETIME', k);
      assertEqualInt(0, Number(idletime));
    } finally {
      await redis.del(k);
    }
  }));

  // OBJECT_FREQ: Verify OBJECT FREQ returns LFU frequency counter
  results.push(await runTest('OBJECT_FREQ', cat, async () => {
    const k = key(prefix, 'mem:freq');
    // Save and set LFU policy
    const origConfig = await redis.config('GET', 'maxmemory-policy');
    const origPolicy = origConfig[1] || 'noeviction';
    try {
      await redis.config('SET', 'maxmemory-policy', 'allkeys-lfu');
      await redis.set(k, 'testvalue');
      // Access the key a few times
      for (let i = 0; i < 5; i++) {
        await redis.get(k);
      }
      await sleep(50);

      const freq = await redis.object('FREQ', k);
      assertTrue(Number(freq) > 0, `OBJECT FREQ should be > 0 after access, got ${freq}`);
    } finally {
      await redis.config('SET', 'maxmemory-policy', origPolicy);
      await redis.del(k);
    }
  }));

  return results;
});
