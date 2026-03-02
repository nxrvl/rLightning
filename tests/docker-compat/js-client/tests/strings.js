const { register, runTest, key, assertEqual, assertTrue, assertEqualInt, assertEqualFloat } = require('./framework');

register('strings', async (redis, prefix) => {
  const results = [];
  const cat = 'strings';

  results.push(await runTest('SET_GET_basic', cat, async () => {
    const k = key(prefix, 'str:basic');
    await redis.set(k, 'hello');
    const val = await redis.get(k);
    assertEqual('hello', val);
    await redis.del(k);
  }));

  results.push(await runTest('SET_NX_flag', cat, async () => {
    const k = key(prefix, 'str:setnx');
    const ok1 = await redis.set(k, 'first', 'NX');
    assertEqual('OK', ok1);
    const ok2 = await redis.set(k, 'second', 'NX');
    assertTrue(ok2 === null, 'second SET NX should return null');
    const val = await redis.get(k);
    assertEqual('first', val);
    await redis.del(k);
  }));

  results.push(await runTest('SET_XX_flag', cat, async () => {
    const k = key(prefix, 'str:setxx');
    const ok1 = await redis.set(k, 'nope', 'XX');
    assertTrue(ok1 === null, 'SET XX on non-existent should return null');
    await redis.set(k, 'exists');
    const ok2 = await redis.set(k, 'updated', 'XX');
    assertEqual('OK', ok2);
    const val = await redis.get(k);
    assertEqual('updated', val);
    await redis.del(k);
  }));

  results.push(await runTest('SET_with_EX', cat, async () => {
    const k = key(prefix, 'str:setex');
    await redis.set(k, 'ttlval', 'EX', 10);
    const ttl = await redis.ttl(k);
    assertTrue(ttl > 0 && ttl <= 10, 'TTL should be between 0 and 10');
    await redis.del(k);
  }));

  results.push(await runTest('SET_with_PX', cat, async () => {
    const k = key(prefix, 'str:setpx');
    await redis.set(k, 'pxval', 'PX', 10000);
    const pttl = await redis.pttl(k);
    assertTrue(pttl > 0, 'PTTL should be positive');
    await redis.del(k);
  }));

  results.push(await runTest('MSET_MGET', cat, async () => {
    const k1 = key(prefix, 'str:m1');
    const k2 = key(prefix, 'str:m2');
    const k3 = key(prefix, 'str:m3');
    await redis.mset(k1, 'a', k2, 'b', k3, 'c');
    const vals = await redis.mget(k1, k2, k3);
    assertEqualInt(3, vals.length);
    assertEqual('a', vals[0]);
    assertEqual('b', vals[1]);
    assertEqual('c', vals[2]);
    await redis.del(k1, k2, k3);
  }));

  results.push(await runTest('INCR_DECR', cat, async () => {
    const k = key(prefix, 'str:incr');
    await redis.set(k, '10');
    const val1 = await redis.incr(k);
    assertEqualInt(11, val1);
    const val2 = await redis.decr(k);
    assertEqualInt(10, val2);
    await redis.del(k);
  }));

  results.push(await runTest('INCRBY_DECRBY', cat, async () => {
    const k = key(prefix, 'str:incrby');
    await redis.set(k, '100');
    const val1 = await redis.incrby(k, 25);
    assertEqualInt(125, val1);
    const val2 = await redis.decrby(k, 50);
    assertEqualInt(75, val2);
    await redis.del(k);
  }));

  results.push(await runTest('INCRBYFLOAT', cat, async () => {
    const k = key(prefix, 'str:incrf');
    await redis.set(k, '10.5');
    const val = await redis.incrbyfloat(k, 0.1);
    assertEqualFloat(10.6, parseFloat(val), 0.001);
    await redis.del(k);
  }));

  results.push(await runTest('APPEND_STRLEN', cat, async () => {
    const k = key(prefix, 'str:append');
    await redis.set(k, 'hello');
    const newLen = await redis.append(k, ' world');
    assertEqualInt(11, newLen);
    const slen = await redis.strlen(k);
    assertEqualInt(11, slen);
    await redis.del(k);
  }));

  results.push(await runTest('GETRANGE_SETRANGE', cat, async () => {
    const k = key(prefix, 'str:range');
    await redis.set(k, 'Hello, World!');
    const sub = await redis.getrange(k, 7, 11);
    assertEqual('World', sub);
    await redis.setrange(k, 7, 'Redis');
    const val = await redis.get(k);
    assertEqual('Hello, Redis!', val);
    await redis.del(k);
  }));

  results.push(await runTest('GETSET_deprecated', cat, async () => {
    const k = key(prefix, 'str:getset');
    await redis.set(k, 'old');
    const old = await redis.getset(k, 'new');
    assertEqual('old', old);
    const val = await redis.get(k);
    assertEqual('new', val);
    await redis.del(k);
  }));

  results.push(await runTest('GETDEL', cat, async () => {
    const k = key(prefix, 'str:getdel');
    await redis.set(k, 'deleteme');
    const val = await redis.getdel(k);
    assertEqual('deleteme', val);
    const gone = await redis.get(k);
    assertTrue(gone === null, 'key should be gone after GETDEL');
  }));

  results.push(await runTest('SETNX_command', cat, async () => {
    const k = key(prefix, 'str:setnxcmd');
    const ok1 = await redis.setnx(k, 'val');
    assertEqualInt(1, ok1);
    const ok2 = await redis.setnx(k, 'val2');
    assertEqualInt(0, ok2);
    await redis.del(k);
  }));

  results.push(await runTest('SETEX_command', cat, async () => {
    const k = key(prefix, 'str:setexcmd');
    await redis.setex(k, 10, 'val');
    const val = await redis.get(k);
    assertEqual('val', val);
    const ttl = await redis.ttl(k);
    assertTrue(ttl > 0, 'TTL should be positive after SETEX');
    await redis.del(k);
  }));

  results.push(await runTest('PSETEX_command', cat, async () => {
    const k = key(prefix, 'str:psetex');
    await redis.psetex(k, 10000, 'val');
    const pttl = await redis.pttl(k);
    assertTrue(pttl > 0, 'PTTL should be positive after PSETEX');
    await redis.del(k);
  }));

  results.push(await runTest('SET_with_EXAT', cat, async () => {
    const k = key(prefix, 'str:exat');
    const expireAt = Math.floor(Date.now() / 1000) + 60;
    await redis.set(k, 'val', 'EXAT', expireAt);
    const ttl = await redis.ttl(k);
    assertTrue(ttl > 50 && ttl <= 60, 'TTL should be ~60s');
    await redis.del(k);
  }));

  results.push(await runTest('MGET_with_missing_keys', cat, async () => {
    const k1 = key(prefix, 'str:mget_exists');
    await redis.set(k1, 'val');
    const vals = await redis.mget(k1, key(prefix, 'str:mget_nonexist'));
    assertEqualInt(2, vals.length);
    assertEqual('val', vals[0]);
    assertTrue(vals[1] === null, 'missing key should return null');
    await redis.del(k1);
  }));

  return results;
});
