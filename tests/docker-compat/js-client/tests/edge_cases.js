const { register, runTest, key, assertEqual, assertTrue, assertEqualInt } = require('./framework');

register('edge_cases', async (redis, prefix) => {
  const results = [];
  const cat = 'edge_cases';

  results.push(await runTest('wrong_type_error', cat, async () => {
    const k = key(prefix, 'edge:wrongtype');
    await redis.set(k, 'string_value');
    let errored = false;
    try {
      await redis.lpush(k, 'bad');
    } catch (err) {
      errored = true;
      assertTrue(err.message.includes('WRONGTYPE'), 'error should be WRONGTYPE: ' + err.message);
    }
    assertTrue(errored, 'expected WRONGTYPE error');
    await redis.del(k);
  }));

  results.push(await runTest('nonexistent_key_GET', cat, async () => {
    const val = await redis.get(key(prefix, 'edge:nonexist'));
    assertTrue(val === null, 'GET nonexistent key should return null');
  }));

  results.push(await runTest('nonexistent_key_operations', cat, async () => {
    const k = key(prefix, 'edge:nonexist_ops');
    const n = await redis.llen(k);
    assertEqualInt(0, n);
    const m = await redis.hgetall(k);
    assertEqualInt(0, Object.keys(m).length);
    const sc = await redis.scard(k);
    assertEqualInt(0, sc);
  }));

  results.push(await runTest('empty_string_value', cat, async () => {
    const k = key(prefix, 'edge:empty');
    await redis.set(k, '');
    const val = await redis.get(k);
    assertEqual('', val);
    const slen = await redis.strlen(k);
    assertEqualInt(0, slen);
    await redis.del(k);
  }));

  results.push(await runTest('binary_data_null_bytes', cat, async () => {
    const k = key(prefix, 'edge:binary');
    const binVal = Buffer.from('hello\x00world\x00\xff\xfe');
    await redis.set(k, binVal);
    const valBuf = await redis.getBuffer(k);
    assertTrue(Buffer.compare(binVal, valBuf) === 0, 'binary data should round-trip correctly');
    await redis.del(k);
  }));

  results.push(await runTest('large_value_1MB', cat, async () => {
    const k = key(prefix, 'edge:large');
    const largeVal = 'x'.repeat(1024 * 1024);
    await redis.set(k, largeVal);
    const val = await redis.get(k);
    assertEqualInt(largeVal.length, val.length);
    await redis.del(k);
  }));

  results.push(await runTest('unicode_utf8', cat, async () => {
    const k = key(prefix, 'edge:utf8');
    const unicodeVal = 'Hello \u4e16\u754c \ud83c\udf0d \u00d1o\u00f1o caf\u00e9';
    await redis.set(k, unicodeVal);
    const val = await redis.get(k);
    assertEqual(unicodeVal, val);
    await redis.del(k);
  }));

  results.push(await runTest('keys_with_special_chars', cat, async () => {
    const keys = [
      key(prefix, 'edge:space key'),
      key(prefix, 'edge:newline\nkey'),
      key(prefix, 'edge:colon:key'),
      key(prefix, 'edge:tab\tkey'),
    ];
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i];
      const val = `val_${i}`;
      await redis.set(k, val);
      const got = await redis.get(k);
      assertEqual(val, got);
    }
    for (const k of keys) {
      await redis.del(k);
    }
  }));

  results.push(await runTest('negative_zero_overflow_integers', cat, async () => {
    const k = key(prefix, 'edge:intops');
    await redis.set(k, '10');
    const val1 = await redis.incrby(k, -20);
    assertEqualInt(-10, val1);

    const k2 = key(prefix, 'edge:intzero');
    const val2 = await redis.incr(k2);
    assertEqualInt(1, val2);

    const k3 = key(prefix, 'edge:intnan');
    await redis.set(k3, 'notanumber');
    let errored = false;
    try {
      await redis.incr(k3);
    } catch {
      errored = true;
    }
    assertTrue(errored, 'INCR on non-numeric should error');

    await redis.del(k, k2, k3);
  }));

  results.push(await runTest('concurrent_operations', cat, async () => {
    const k = key(prefix, 'edge:concurrent');
    const n = 50;
    const promises = [];
    for (let i = 0; i < n; i++) {
      promises.push(redis.incr(k));
    }
    const results = await Promise.all(promises);
    // All should succeed
    assertEqualInt(n, results.length);
    const val = await redis.get(k);
    assertEqual('50', val);
    await redis.del(k);
  }));

  return results;
});
