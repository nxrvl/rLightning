const { register, runTest, key, assertEqual, assertTrue, assertEqualInt } = require('./framework');

register('streams', async (redis, prefix) => {
  const results = [];
  const cat = 'streams';

  results.push(await runTest('XADD_auto_ID', cat, async () => {
    const k = key(prefix, 'stream:auto');
    const id = await redis.xadd(k, '*', 'field', 'value1');
    assertTrue(id.includes('-'), `auto ID should contain '-': ${id}`);
    await redis.del(k);
  }));

  results.push(await runTest('XADD_explicit_ID', cat, async () => {
    const k = key(prefix, 'stream:explicit');
    const id = await redis.xadd(k, '1-1', 'field', 'value');
    assertEqual('1-1', id);
    await redis.del(k);
  }));

  results.push(await runTest('XLEN', cat, async () => {
    const k = key(prefix, 'stream:len');
    for (let i = 0; i < 5; i++) {
      await redis.xadd(k, '*', 'i', String(i));
    }
    const n = await redis.xlen(k);
    assertEqualInt(5, n);
    await redis.del(k);
  }));

  results.push(await runTest('XRANGE_XREVRANGE', cat, async () => {
    const k = key(prefix, 'stream:range');
    await redis.xadd(k, '1-1', 'a', '1');
    await redis.xadd(k, '2-1', 'b', '2');
    await redis.xadd(k, '3-1', 'c', '3');

    const msgs = await redis.xrange(k, '-', '+');
    assertEqualInt(3, msgs.length);
    assertEqual('1-1', msgs[0][0]);

    const rmsgs = await redis.xrevrange(k, '+', '-');
    assertEqualInt(3, rmsgs.length);
    assertEqual('3-1', rmsgs[0][0]);
    await redis.del(k);
  }));

  results.push(await runTest('XREAD', cat, async () => {
    const k = key(prefix, 'stream:read');
    await redis.xadd(k, '1-1', 'x', '1');
    await redis.xadd(k, '2-1', 'x', '2');

    const res = await redis.xread('COUNT', 10, 'STREAMS', k, '0');
    assertEqualInt(1, res.length);
    assertEqualInt(2, res[0][1].length);
    await redis.del(k);
  }));

  results.push(await runTest('XINFO_STREAM', cat, async () => {
    const k = key(prefix, 'stream:info');
    await redis.xadd(k, '*', 'f', 'v');
    const info = await redis.call('XINFO', 'STREAM', k);
    // info is a flat array: [key, val, key, val, ...]
    // find 'length' key
    let length = null;
    for (let i = 0; i < info.length; i += 2) {
      if (info[i] === 'length') {
        length = info[i + 1];
        break;
      }
    }
    assertEqualInt(1, length);
    await redis.del(k);
  }));

  results.push(await runTest('XTRIM_MAXLEN', cat, async () => {
    const k = key(prefix, 'stream:trim');
    for (let i = 0; i < 10; i++) {
      await redis.xadd(k, '*', 'i', String(i));
    }
    const n = await redis.xtrim(k, 'MAXLEN', 5);
    assertTrue(n >= 0, 'XTRIM should return non-negative count');
    const length = await redis.xlen(k);
    assertTrue(length <= 5, 'stream should have <= 5 entries after trim');
    await redis.del(k);
  }));

  results.push(await runTest('XDEL', cat, async () => {
    const k = key(prefix, 'stream:del');
    const id = await redis.xadd(k, '*', 'f', 'v');
    await redis.xadd(k, '*', 'f2', 'v2');
    const n = await redis.xdel(k, id);
    assertEqualInt(1, n);
    const length = await redis.xlen(k);
    assertEqualInt(1, length);
    await redis.del(k);
  }));

  return results;
});
