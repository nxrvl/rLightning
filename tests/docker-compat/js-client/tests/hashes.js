const { register, runTest, key, assertEqual, assertTrue, assertEqualInt, assertEqualFloat, assertArrayEqual, assertArrayEqualUnordered } = require('./framework');

register('hashes', async (redis, prefix) => {
  const results = [];
  const cat = 'hashes';

  results.push(await runTest('HSET_HGET_single', cat, async () => {
    const k = key(prefix, 'hash:single');
    await redis.hset(k, 'field1', 'value1');
    const val = await redis.hget(k, 'field1');
    assertEqual('value1', val);
    await redis.del(k);
  }));

  results.push(await runTest('HSET_multiple_fields', cat, async () => {
    const k = key(prefix, 'hash:multi');
    await redis.hset(k, 'f1', 'v1', 'f2', 'v2', 'f3', 'v3');
    const v1 = await redis.hget(k, 'f1');
    const v2 = await redis.hget(k, 'f2');
    const v3 = await redis.hget(k, 'f3');
    assertEqual('v1', v1);
    assertEqual('v2', v2);
    assertEqual('v3', v3);
    await redis.del(k);
  }));

  results.push(await runTest('HMSET_HMGET', cat, async () => {
    const k = key(prefix, 'hash:hmset');
    await redis.hmset(k, { a: '1', b: '2', c: '3' });
    const vals = await redis.hmget(k, 'a', 'b', 'c', 'missing');
    assertEqualInt(4, vals.length);
    assertEqual('1', vals[0]);
    assertEqual('2', vals[1]);
    assertEqual('3', vals[2]);
    assertTrue(vals[3] === null, 'missing field should be null');
    await redis.del(k);
  }));

  results.push(await runTest('HGETALL', cat, async () => {
    const k = key(prefix, 'hash:getall');
    await redis.hset(k, 'x', '10', 'y', '20');
    const m = await redis.hgetall(k);
    assertEqual('10', m.x);
    assertEqual('20', m.y);
    assertEqualInt(2, Object.keys(m).length);
    await redis.del(k);
  }));

  results.push(await runTest('HDEL_HEXISTS', cat, async () => {
    const k = key(prefix, 'hash:del');
    await redis.hset(k, 'field', 'val');
    const exists1 = await redis.hexists(k, 'field');
    assertEqualInt(1, exists1);
    const n = await redis.hdel(k, 'field');
    assertEqualInt(1, n);
    const exists2 = await redis.hexists(k, 'field');
    assertEqualInt(0, exists2);
    await redis.del(k);
  }));

  results.push(await runTest('HLEN_HKEYS_HVALS', cat, async () => {
    const k = key(prefix, 'hash:lkv');
    await redis.hset(k, 'a', '1', 'b', '2', 'c', '3');
    const hlen = await redis.hlen(k);
    assertEqualInt(3, hlen);
    const keys = await redis.hkeys(k);
    assertArrayEqualUnordered(['a', 'b', 'c'], keys);
    const vals = await redis.hvals(k);
    assertArrayEqualUnordered(['1', '2', '3'], vals);
    await redis.del(k);
  }));

  results.push(await runTest('HINCRBY_HINCRBYFLOAT', cat, async () => {
    const k = key(prefix, 'hash:incr');
    await redis.hset(k, 'counter', '10');
    const val = await redis.hincrby(k, 'counter', 5);
    assertEqualInt(15, val);
    await redis.hset(k, 'float_counter', '10.5');
    const fval = await redis.hincrbyfloat(k, 'float_counter', 0.1);
    assertEqualFloat(10.6, parseFloat(fval), 0.001);
    await redis.del(k);
  }));

  results.push(await runTest('HSETNX', cat, async () => {
    const k = key(prefix, 'hash:setnx');
    const ok1 = await redis.hsetnx(k, 'field', 'first');
    assertEqualInt(1, ok1);
    const ok2 = await redis.hsetnx(k, 'field', 'second');
    assertEqualInt(0, ok2);
    const val = await redis.hget(k, 'field');
    assertEqual('first', val);
    await redis.del(k);
  }));

  results.push(await runTest('HRANDFIELD', cat, async () => {
    const k = key(prefix, 'hash:rand');
    await redis.hset(k, 'a', '1', 'b', '2', 'c', '3');
    const fields = await redis.call('HRANDFIELD', k, 1);
    assertTrue(Array.isArray(fields) ? fields.length === 1 : typeof fields === 'string', 'should return 1 field');
    const field = Array.isArray(fields) ? fields[0] : fields;
    const valid = field === 'a' || field === 'b' || field === 'c';
    assertTrue(valid, 'returned field should be one of a, b, c');
    await redis.del(k);
  }));

  results.push(await runTest('HSCAN', cat, async () => {
    const k = key(prefix, 'hash:scan');
    for (let i = 0; i < 20; i++) {
      await redis.hset(k, `field${i}`, `val${i}`);
    }
    const allKeys = [];
    let cursor = '0';
    do {
      const [nextCursor, data] = await redis.hscan(k, cursor, 'MATCH', '*', 'COUNT', 10);
      cursor = nextCursor;
      for (let i = 0; i < data.length; i += 2) {
        allKeys.push(data[i]);
      }
    } while (cursor !== '0');
    assertEqualInt(20, allKeys.length);
    await redis.del(k);
  }));

  return results;
});
