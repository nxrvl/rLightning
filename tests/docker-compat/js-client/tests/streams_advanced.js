const { register, runTest, key, assertEqual, assertTrue, assertEqualInt, sleep } = require('./framework');

register('streams_advanced', async (redis, prefix) => {
  const results = [];
  const cat = 'streams_advanced';

  // Helper to parse flat XINFO array into object
  function parseInfoArray(arr) {
    const obj = {};
    for (let i = 0; i < arr.length; i += 2) {
      obj[arr[i]] = arr[i + 1];
    }
    return obj;
  }

  // XGROUP_CREATE: Create consumer group, verify with XINFO GROUPS
  results.push(await runTest('XGROUP_CREATE', cat, async () => {
    const k = key(prefix, 'sa:xgroup');
    await redis.xadd(k, '1-1', 'f', 'v');
    await redis.xgroup('CREATE', k, 'mygroup', '0');
    const groups = await redis.call('XINFO', 'GROUPS', k);
    assertEqualInt(1, groups.length);
    const g = parseInfoArray(groups[0]);
    assertEqual('mygroup', g.name);
    await redis.del(k);
  }));

  // XREADGROUP_basic: Create group, XREADGROUP, verify message delivered
  results.push(await runTest('XREADGROUP_basic', cat, async () => {
    const k = key(prefix, 'sa:xreadgroup');
    await redis.xadd(k, '1-1', 'msg', 'hello');
    await redis.xgroup('CREATE', k, 'grp1', '0');
    const res = await redis.xreadgroup('GROUP', 'grp1', 'consumer1', 'COUNT', 10, 'STREAMS', k, '>');
    assertEqualInt(1, res.length);
    assertEqualInt(1, res[0][1].length);
    // res[0][1][0] = [id, [field, value, ...]]
    assertEqual('hello', res[0][1][0][1][1]);
    await redis.del(k);
  }));

  // XREADGROUP_pending: XREADGROUP without ACK, verify pending
  results.push(await runTest('XREADGROUP_pending', cat, async () => {
    const k = key(prefix, 'sa:xreadpend');
    await redis.xadd(k, '1-1', 'f', 'v');
    await redis.xadd(k, '2-1', 'f', 'v2');
    await redis.xgroup('CREATE', k, 'grp1', '0');
    await redis.xreadgroup('GROUP', 'grp1', 'c1', 'COUNT', 10, 'STREAMS', k, '>');
    const pending = await redis.xpending(k, 'grp1');
    // pending = [count, minId, maxId, [[consumer, count], ...]]
    assertEqualInt(2, pending[0]);
    await redis.del(k);
  }));

  // XACK_basic: Read message, XACK, verify removed from pending
  results.push(await runTest('XACK_basic', cat, async () => {
    const k = key(prefix, 'sa:xack');
    const id = await redis.xadd(k, '*', 'f', 'v');
    await redis.xgroup('CREATE', k, 'grp1', '0');
    await redis.xreadgroup('GROUP', 'grp1', 'c1', 'COUNT', 10, 'STREAMS', k, '>');

    // Verify pending before ACK
    let pending = await redis.xpending(k, 'grp1');
    assertEqualInt(1, pending[0]);

    // ACK
    const n = await redis.xack(k, 'grp1', id);
    assertEqualInt(1, n);

    // Verify pending after ACK
    pending = await redis.xpending(k, 'grp1');
    assertEqualInt(0, pending[0]);
    await redis.del(k);
  }));

  // XCLAIM_basic: Read on consumer1, XCLAIM to consumer2
  results.push(await runTest('XCLAIM_basic', cat, async () => {
    const k = key(prefix, 'sa:xclaim');
    const id = await redis.xadd(k, '*', 'f', 'v');
    await redis.xgroup('CREATE', k, 'grp1', '0');
    await redis.xreadgroup('GROUP', 'grp1', 'consumer1', 'COUNT', 10, 'STREAMS', k, '>');

    // Claim to consumer2 with 0 min-idle-time
    const msgs = await redis.xclaim(k, 'grp1', 'consumer2', 0, id);
    assertEqualInt(1, msgs.length);

    // Verify consumer2 has it in pending
    const detail = await redis.call('XPENDING', k, 'grp1', '-', '+', '10');
    assertEqualInt(1, detail.length);
    assertEqual('consumer2', detail[0][1]);
    await redis.del(k);
  }));

  // XAUTOCLAIM_basic: Read message, wait, XAUTOCLAIM with min-idle
  results.push(await runTest('XAUTOCLAIM_basic', cat, async () => {
    const k = key(prefix, 'sa:xautoclaim');
    await redis.xadd(k, '1-1', 'f', 'v');
    await redis.xgroup('CREATE', k, 'grp1', '0');
    await redis.xreadgroup('GROUP', 'grp1', 'consumer1', 'COUNT', 10, 'STREAMS', k, '>');

    // Small wait to ensure idle time > 0
    await sleep(50);

    // XAUTOCLAIM to consumer2 with very low min-idle
    const result = await redis.call('XAUTOCLAIM', k, 'grp1', 'consumer2', '1', '0-0', 'COUNT', '10');
    // result = [nextStartId, [[id, [field, value, ...]], ...], deletedIds]
    assertTrue(result[1].length >= 1, `expected at least 1 autoclaimed message, got ${result[1].length}`);
    await redis.del(k);
  }));

  // XPENDING_summary: Check pending summary
  results.push(await runTest('XPENDING_summary', cat, async () => {
    const k = key(prefix, 'sa:xpendsumm');
    await redis.xadd(k, '1-1', 'f', 'v1');
    await redis.xadd(k, '2-1', 'f', 'v2');
    await redis.xadd(k, '3-1', 'f', 'v3');
    await redis.xgroup('CREATE', k, 'grp1', '0');

    await redis.xreadgroup('GROUP', 'grp1', 'c1', 'COUNT', 2, 'STREAMS', k, '>');
    await redis.xreadgroup('GROUP', 'grp1', 'c2', 'COUNT', 1, 'STREAMS', k, '>');

    const pending = await redis.xpending(k, 'grp1');
    // pending = [count, minId, maxId, [[consumer, count], ...]]
    assertEqualInt(3, pending[0]);
    assertEqual('1-1', pending[1]);
    assertEqual('3-1', pending[2]);
    // Should have 2 consumers
    assertEqualInt(2, pending[3].length);
    await redis.del(k);
  }));

  // XPENDING_detail: Check per-consumer pending detail
  results.push(await runTest('XPENDING_detail', cat, async () => {
    const k = key(prefix, 'sa:xpenddet');
    await redis.xadd(k, '1-1', 'f', 'v1');
    await redis.xadd(k, '2-1', 'f', 'v2');
    await redis.xgroup('CREATE', k, 'grp1', '0');
    await redis.xreadgroup('GROUP', 'grp1', 'c1', 'COUNT', 10, 'STREAMS', k, '>');

    const detail = await redis.call('XPENDING', k, 'grp1', '-', '+', '10');
    assertEqualInt(2, detail.length);
    // Each entry: [id, consumer, idleTime, deliveryCount]
    assertEqual('c1', detail[0][1]);
    assertEqual('1-1', detail[0][0]);
    await redis.del(k);
  }));

  // XINFO_STREAM: Verify full stream info including length, groups, first/last entry
  results.push(await runTest('XINFO_STREAM', cat, async () => {
    const k = key(prefix, 'sa:xinfost');
    await redis.xadd(k, '1-1', 'f', 'v1');
    await redis.xadd(k, '2-1', 'f', 'v2');
    await redis.xadd(k, '3-1', 'f', 'v3');
    await redis.xgroup('CREATE', k, 'grp1', '0');

    const info = await redis.call('XINFO', 'STREAM', k);
    const obj = parseInfoArray(info);
    assertEqualInt(3, obj.length);
    assertEqualInt(1, obj.groups);
    // first-entry and last-entry are arrays: [id, [field, value, ...]]
    assertEqual('1-1', obj['first-entry'][0]);
    assertEqual('3-1', obj['last-entry'][0]);
    await redis.del(k);
  }));

  // XINFO_CONSUMERS: Verify consumer info
  results.push(await runTest('XINFO_CONSUMERS', cat, async () => {
    const k = key(prefix, 'sa:xinfocons');
    await redis.xadd(k, '1-1', 'f', 'v');
    await redis.xadd(k, '2-1', 'f', 'v2');
    await redis.xgroup('CREATE', k, 'grp1', '0');
    await redis.xreadgroup('GROUP', 'grp1', 'consumer1', 'COUNT', 10, 'STREAMS', k, '>');

    const consumers = await redis.call('XINFO', 'CONSUMERS', k, 'grp1');
    assertEqualInt(1, consumers.length);
    const c = parseInfoArray(consumers[0]);
    assertEqual('consumer1', c.name);
    assertEqualInt(2, c.pending);
    await redis.del(k);
  }));

  // XTRIM_MAXLEN: Add entries, XTRIM MAXLEN 5, verify only 5 remain
  results.push(await runTest('XTRIM_MAXLEN', cat, async () => {
    const k = key(prefix, 'sa:xtrimmax');
    for (let i = 0; i < 10; i++) {
      await redis.xadd(k, '*', 'i', String(i));
    }
    await redis.xtrim(k, 'MAXLEN', 5);
    const length = await redis.xlen(k);
    assertEqualInt(5, length);
    await redis.del(k);
  }));

  // XTRIM_MINID: Add entries, XTRIM MINID, verify entries before ID removed
  results.push(await runTest('XTRIM_MINID', cat, async () => {
    const k = key(prefix, 'sa:xtrimmin');
    await redis.xadd(k, '1-1', 'f', 'v1');
    await redis.xadd(k, '2-1', 'f', 'v2');
    await redis.xadd(k, '3-1', 'f', 'v3');
    await redis.xadd(k, '4-1', 'f', 'v4');
    await redis.xadd(k, '5-1', 'f', 'v5');

    await redis.xtrim(k, 'MINID', '3-1');
    const length = await redis.xlen(k);
    assertEqualInt(3, length);
    // Verify first entry is now 3-1
    const msgs = await redis.xrange(k, '-', '+');
    assertEqual('3-1', msgs[0][0]);
    await redis.del(k);
  }));

  return results;
});
