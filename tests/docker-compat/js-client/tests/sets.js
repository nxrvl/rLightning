const { register, runTest, key, assertTrue, assertEqualInt, assertArrayEqualUnordered } = require('./framework');

register('sets', async (redis, prefix) => {
  const results = [];
  const cat = 'sets';

  results.push(await runTest('SADD_single_and_multiple', cat, async () => {
    const k = key(prefix, 'set:add');
    const n1 = await redis.sadd(k, 'a');
    assertEqualInt(1, n1);
    const n2 = await redis.sadd(k, 'b', 'c', 'd');
    assertEqualInt(3, n2);
    const n3 = await redis.sadd(k, 'a');
    assertEqualInt(0, n3);
    await redis.del(k);
  }));

  results.push(await runTest('SMEMBERS_SISMEMBER_SMISMEMBER', cat, async () => {
    const k = key(prefix, 'set:members');
    await redis.sadd(k, 'a', 'b', 'c');
    const members = await redis.smembers(k);
    assertArrayEqualUnordered(['a', 'b', 'c'], members);
    const is1 = await redis.sismember(k, 'a');
    assertEqualInt(1, is1);
    const is2 = await redis.sismember(k, 'z');
    assertEqualInt(0, is2);
    const mis = await redis.smismember(k, 'a', 'z', 'c');
    assertEqualInt(1, mis[0]);
    assertEqualInt(0, mis[1]);
    assertEqualInt(1, mis[2]);
    await redis.del(k);
  }));

  results.push(await runTest('SREM_SPOP_SRANDMEMBER', cat, async () => {
    const k = key(prefix, 'set:rem');
    await redis.sadd(k, 'a', 'b', 'c', 'd', 'e');
    const n = await redis.srem(k, 'a', 'b');
    assertEqualInt(2, n);
    const popped = await redis.spop(k);
    const valid = popped === 'c' || popped === 'd' || popped === 'e';
    assertTrue(valid, 'popped should be c, d, or e');
    const rand = await redis.srandmember(k);
    assertTrue(rand !== null && rand.length > 0, 'SRANDMEMBER should return a member');
    await redis.del(k);
  }));

  results.push(await runTest('SCARD', cat, async () => {
    const k = key(prefix, 'set:card');
    await redis.sadd(k, 'a', 'b', 'c');
    const n = await redis.scard(k);
    assertEqualInt(3, n);
    await redis.del(k);
  }));

  results.push(await runTest('SUNION_SINTER_SDIFF', cat, async () => {
    const k1 = key(prefix, 'set:op1');
    const k2 = key(prefix, 'set:op2');
    await redis.sadd(k1, 'a', 'b', 'c');
    await redis.sadd(k2, 'b', 'c', 'd');

    const union = await redis.sunion(k1, k2);
    assertArrayEqualUnordered(['a', 'b', 'c', 'd'], union);

    const inter = await redis.sinter(k1, k2);
    assertArrayEqualUnordered(['b', 'c'], inter);

    const diff = await redis.sdiff(k1, k2);
    assertArrayEqualUnordered(['a'], diff);

    await redis.del(k1, k2);
  }));

  results.push(await runTest('SUNIONSTORE_SINTERSTORE_SDIFFSTORE', cat, async () => {
    const k1 = key(prefix, 'set:store1');
    const k2 = key(prefix, 'set:store2');
    const dest1 = key(prefix, 'set:union_dest');
    const dest2 = key(prefix, 'set:inter_dest');
    const dest3 = key(prefix, 'set:diff_dest');
    await redis.sadd(k1, 'a', 'b', 'c');
    await redis.sadd(k2, 'b', 'c', 'd');

    const n1 = await redis.sunionstore(dest1, k1, k2);
    assertEqualInt(4, n1);
    const n2 = await redis.sinterstore(dest2, k1, k2);
    assertEqualInt(2, n2);
    const n3 = await redis.sdiffstore(dest3, k1, k2);
    assertEqualInt(1, n3);

    await redis.del(k1, k2, dest1, dest2, dest3);
  }));

  results.push(await runTest('SMOVE', cat, async () => {
    const src = key(prefix, 'set:move_src');
    const dst = key(prefix, 'set:move_dst');
    await redis.sadd(src, 'a', 'b');
    await redis.sadd(dst, 'c');
    const ok = await redis.smove(src, dst, 'a');
    assertEqualInt(1, ok);
    const srcMembers = await redis.smembers(src);
    assertArrayEqualUnordered(['b'], srcMembers);
    const dstMembers = await redis.smembers(dst);
    assertArrayEqualUnordered(['a', 'c'], dstMembers);
    await redis.del(src, dst);
  }));

  results.push(await runTest('SSCAN', cat, async () => {
    const k = key(prefix, 'set:scan');
    for (let i = 0; i < 20; i++) {
      await redis.sadd(k, `member${i}`);
    }
    const all = [];
    let cursor = '0';
    do {
      const [nextCursor, members] = await redis.sscan(k, cursor, 'MATCH', '*', 'COUNT', 10);
      cursor = nextCursor;
      all.push(...members);
    } while (cursor !== '0');
    assertEqualInt(20, all.length);
    await redis.del(k);
  }));

  results.push(await runTest('SINTERCARD', cat, async () => {
    const k1 = key(prefix, 'set:ic1');
    const k2 = key(prefix, 'set:ic2');
    await redis.sadd(k1, 'a', 'b', 'c');
    await redis.sadd(k2, 'b', 'c', 'd');
    const n = await redis.call('SINTERCARD', 2, k1, k2);
    assertEqualInt(2, n);
    await redis.del(k1, k2);
  }));

  return results;
});
