const { register, runTest, key, assertEqual, assertTrue, assertEqualInt } = require('./framework');

register('keys', async (redis, prefix) => {
  const results = [];
  const cat = 'keys';

  results.push(await runTest('DEL_single_and_multiple', cat, async () => {
    const k1 = key(prefix, 'key:del1');
    const k2 = key(prefix, 'key:del2');
    const k3 = key(prefix, 'key:del3');
    await redis.set(k1, 'a');
    await redis.set(k2, 'b');
    await redis.set(k3, 'c');
    const n1 = await redis.del(k1);
    assertEqualInt(1, n1);
    const n2 = await redis.del(k2, k3);
    assertEqualInt(2, n2);
  }));

  results.push(await runTest('EXISTS_single_and_multiple', cat, async () => {
    const k1 = key(prefix, 'key:ex1');
    const k2 = key(prefix, 'key:ex2');
    await redis.set(k1, 'a');
    await redis.set(k2, 'b');
    const n1 = await redis.exists(k1);
    assertEqualInt(1, n1);
    const n2 = await redis.exists(k1, k2, key(prefix, 'key:nonexist'));
    assertEqualInt(2, n2);
    await redis.del(k1, k2);
  }));

  results.push(await runTest('EXPIRE_TTL', cat, async () => {
    const k = key(prefix, 'key:expire');
    await redis.set(k, 'val');
    const ok = await redis.expire(k, 10);
    assertEqualInt(1, ok);
    const ttl = await redis.ttl(k);
    assertTrue(ttl > 0 && ttl <= 10, 'TTL should be between 0 and 10');
    await redis.del(k);
  }));

  results.push(await runTest('PEXPIRE_PTTL', cat, async () => {
    const k = key(prefix, 'key:pexpire');
    await redis.set(k, 'val');
    const ok = await redis.pexpire(k, 10000);
    assertEqualInt(1, ok);
    const pttl = await redis.pttl(k);
    assertTrue(pttl > 0, 'PTTL should be positive');
    await redis.del(k);
  }));

  results.push(await runTest('EXPIREAT_PEXPIREAT', cat, async () => {
    const k = key(prefix, 'key:expireat');
    await redis.set(k, 'val');
    const future = Math.floor(Date.now() / 1000) + 60;
    const ok1 = await redis.expireat(k, future);
    assertEqualInt(1, ok1);
    const ttl = await redis.ttl(k);
    assertTrue(ttl > 50, 'TTL should be ~60s');

    const k2 = key(prefix, 'key:pexpireat');
    await redis.set(k2, 'val');
    const futureMs = Date.now() + 60000;
    const ok2 = await redis.pexpireat(k2, futureMs);
    assertEqualInt(1, ok2);
    await redis.del(k, k2);
  }));

  results.push(await runTest('PERSIST', cat, async () => {
    const k = key(prefix, 'key:persist');
    await redis.set(k, 'val', 'EX', 10);
    const ok = await redis.persist(k);
    assertEqualInt(1, ok);
    const ttl = await redis.ttl(k);
    // -1 means no expiry, -2 means key doesn't exist
    assertTrue(ttl < 0, 'TTL should be negative after PERSIST (no expiration)');
    await redis.del(k);
  }));

  results.push(await runTest('TYPE', cat, async () => {
    const ks = key(prefix, 'key:type_s');
    const kl = key(prefix, 'key:type_l');
    const kh = key(prefix, 'key:type_h');
    const kz = key(prefix, 'key:type_z');
    const kset = key(prefix, 'key:type_set');
    await redis.set(ks, 'val');
    await redis.lpush(kl, 'val');
    await redis.hset(kh, 'f', 'v');
    await redis.zadd(kz, 1, 'a');
    await redis.sadd(kset, 'a');

    assertEqual('string', await redis.type(ks));
    assertEqual('list', await redis.type(kl));
    assertEqual('hash', await redis.type(kh));
    assertEqual('zset', await redis.type(kz));
    assertEqual('set', await redis.type(kset));

    await redis.del(ks, kl, kh, kz, kset);
  }));

  results.push(await runTest('RENAME_RENAMENX', cat, async () => {
    const k1 = key(prefix, 'key:rename_src');
    const k2 = key(prefix, 'key:rename_dst');
    const k3 = key(prefix, 'key:renamenx_dst');
    await redis.set(k1, 'val');
    await redis.rename(k1, k2);
    const val = await redis.get(k2);
    assertEqual('val', val);

    // RENAMENX
    await redis.set(k1, 'new');
    await redis.set(k3, 'exists');
    const ok1 = await redis.renamenx(k1, k3);
    assertEqualInt(0, ok1);
    await redis.del(k3);
    const ok2 = await redis.renamenx(k1, k3);
    assertEqualInt(1, ok2);

    await redis.del(k1, k2, k3);
  }));

  results.push(await runTest('KEYS_pattern', cat, async () => {
    const k1 = key(prefix, 'key:pat_a');
    const k2 = key(prefix, 'key:pat_b');
    const k3 = key(prefix, 'key:pat_c');
    await redis.set(k1, '1');
    await redis.set(k2, '2');
    await redis.set(k3, '3');
    const keys = await redis.keys(key(prefix, 'key:pat_*'));
    assertTrue(keys.length >= 3, 'KEYS should return at least 3 keys');
    await redis.del(k1, k2, k3);
  }));

  results.push(await runTest('SCAN_with_pattern', cat, async () => {
    for (let i = 0; i < 10; i++) {
      await redis.set(key(prefix, `key:scan_${i}`), String(i));
    }
    const all = [];
    let cursor = '0';
    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', key(prefix, 'key:scan_*'), 'COUNT', 5);
      cursor = nextCursor;
      all.push(...keys);
    } while (cursor !== '0');
    assertTrue(all.length >= 10, `SCAN should find at least 10 keys, got ${all.length}`);
    for (let i = 0; i < 10; i++) {
      await redis.del(key(prefix, `key:scan_${i}`));
    }
  }));

  results.push(await runTest('UNLINK', cat, async () => {
    const k = key(prefix, 'key:unlink');
    await redis.set(k, 'val');
    const n = await redis.unlink(k);
    assertEqualInt(1, n);
    const exists = await redis.exists(k);
    assertEqualInt(0, exists);
  }));

  results.push(await runTest('OBJECT_ENCODING', cat, async () => {
    const k = key(prefix, 'key:obj_enc');
    await redis.set(k, '12345');
    const enc = await redis.object('ENCODING', k);
    assertTrue(enc !== null && enc.length > 0, 'OBJECT ENCODING should return a value');
    await redis.del(k);
  }));

  results.push(await runTest('COPY', cat, async () => {
    const src = key(prefix, 'key:copy_src');
    const dst = key(prefix, 'key:copy_dst');
    await redis.set(src, 'copyval');
    const res = await redis.call('COPY', src, dst);
    assertEqualInt(1, res);
    const val = await redis.get(dst);
    assertEqual('copyval', val);
    await redis.del(src, dst);
  }));

  results.push(await runTest('DUMP_RESTORE', cat, async () => {
    const src = key(prefix, 'key:dump');
    const dst = key(prefix, 'key:restore');
    await redis.set(src, 'dumpval');
    const dump = await redis.dumpBuffer(src);
    assertTrue(dump !== null && dump.length > 0, 'DUMP should return data');
    await redis.restore(dst, 0, dump);
    const val = await redis.get(dst);
    assertEqual('dumpval', val);
    await redis.del(src, dst);
  }));

  return results;
});
