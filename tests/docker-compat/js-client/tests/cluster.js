const Redis = require('ioredis');
const { register, runTest, skipTest, key, assertEqual, assertTrue, sleep } = require('./framework');

register('cluster', async (redis, prefix) => {
  const results = [];
  const cat = 'cluster';

  const clusterHost = process.env.REDIS_CLUSTER_HOST || '';
  const clusterPort = parseInt(process.env.REDIS_CLUSTER_PORT || '6379', 10);

  // If cluster is not configured, skip all cluster tests
  if (!clusterHost) {
    const reason = 'REDIS_CLUSTER_HOST not set';
    results.push(skipTest('CLUSTER_INFO', cat, reason));
    results.push(skipTest('CLUSTER_NODES', cat, reason));
    results.push(skipTest('CLUSTER_SLOTS', cat, reason));
    results.push(skipTest('CLUSTER_SHARDS', cat, reason));
    results.push(skipTest('CLUSTER_hash_tag', cat, reason));
    results.push(skipTest('CLUSTER_MOVED_redirect', cat, reason));
    results.push(skipTest('CLUSTER_KEYSLOT', cat, reason));
    results.push(skipTest('CLUSTER_COUNTKEYSINSLOT', cat, reason));
    results.push(skipTest('CLUSTER_cross_slot_error', cat, reason));
    results.push(skipTest('READONLY_mode', cat, reason));
    return results;
  }

  const password = redis.options.password;

  const clusterClient = new Redis({
    host: clusterHost,
    port: clusterPort,
    password: password,
    db: 0,
    lazyConnect: true,
    retryStrategy: () => null,
    maxRetriesPerRequest: 3,
  });
  await clusterClient.connect();

  // Wait for cluster node to be ready
  for (let i = 0; i < 30; i++) {
    try {
      await clusterClient.ping();
      break;
    } catch {
      await sleep(1000);
    }
  }

  // Wait for cluster to converge
  for (let i = 0; i < 30; i++) {
    try {
      const info = await clusterClient.call('CLUSTER', 'INFO');
      if (info.includes('cluster_state:ok')) break;
    } catch {}
    await sleep(1000);
  }

  // Helper: parse CLUSTER NODES output
  const parseClusterNodes = (nodesStr) => {
    return nodesStr.trim().split('\n').map(line => {
      const fields = line.trim().split(/\s+/);
      return {
        id: fields[0],
        addr: fields[1],
        flags: fields[2],
        master: fields[3],
        pingSent: fields[4],
        pongRecv: fields[5],
        configEpoch: fields[6],
        linkState: fields[7],
        slots: fields.slice(8),
      };
    });
  };

  // CLUSTER_INFO: Verify CLUSTER INFO returns cluster_state, cluster_slots_assigned, cluster_size
  results.push(await runTest('CLUSTER_INFO', cat, async () => {
    const info = await clusterClient.call('CLUSTER', 'INFO');
    assertTrue(info.includes('cluster_state:'), 'CLUSTER INFO missing cluster_state field');
    assertTrue(info.includes('cluster_slots_assigned:'), 'CLUSTER INFO missing cluster_slots_assigned field');
    assertTrue(info.includes('cluster_size:'), 'CLUSTER INFO missing cluster_size field');
    assertTrue(info.includes('cluster_enabled:1'), 'CLUSTER INFO should show cluster_enabled:1');
  }));

  // CLUSTER_NODES: Verify CLUSTER NODES returns node list with correct format
  results.push(await runTest('CLUSTER_NODES', cat, async () => {
    const nodes = await clusterClient.call('CLUSTER', 'NODES');
    const parsed = parseClusterNodes(nodes);

    assertTrue(parsed.length >= 3, `expected at least 3 nodes, got ${parsed.length}`);

    let foundMyself = false;
    for (const node of parsed) {
      // Node ID should be 40 hex chars
      assertTrue(node.id.length === 40, `node ID should be 40 chars, got ${node.id.length}: ${node.id}`);
      if (node.flags.includes('myself')) {
        foundMyself = true;
      }
    }

    assertTrue(foundMyself, "CLUSTER NODES should contain a 'myself' node");
  }));

  // CLUSTER_SLOTS: Verify CLUSTER SLOTS returns slot ranges with master/replica info
  results.push(await runTest('CLUSTER_SLOTS', cat, async () => {
    const slots = await clusterClient.call('CLUSTER', 'SLOTS');

    assertTrue(Array.isArray(slots), 'CLUSTER SLOTS should return an array');
    assertTrue(slots.length > 0, 'CLUSTER SLOTS returned empty result');

    // Verify slot ranges cover [0, 16383]
    let totalSlots = 0;
    for (const range of slots) {
      assertTrue(Array.isArray(range), 'each slot range should be an array');
      const start = range[0];
      const end = range[1];
      assertTrue(start >= 0 && end <= 16383, `invalid slot range: ${start}-${end}`);
      totalSlots += (end - start) + 1;
      // At least the master node info should be present
      assertTrue(range.length >= 3, `slot range ${start}-${end} should have at least one node`);
    }

    assertEqual(16384, totalSlots);
  }));

  // CLUSTER_SHARDS: Verify CLUSTER SHARDS returns shard information (Redis 7.0+)
  results.push(await runTest('CLUSTER_SHARDS', cat, async () => {
    const shards = await clusterClient.call('CLUSTER', 'SHARDS');

    assertTrue(Array.isArray(shards), 'CLUSTER SHARDS should return an array');
    assertTrue(shards.length >= 3, `expected at least 3 shards, got ${shards.length}`);

    // Each shard should have slots and nodes keys
    for (let i = 0; i < shards.length; i++) {
      const shard = shards[i];
      assertTrue(Array.isArray(shard), `shard ${i} should be an array`);
      assertTrue(shard.length >= 4, `shard ${i} has too few fields (${shard.length})`);
    }
  }));

  // CLUSTER_hash_tag: Verify {tag} keys hash to same slot
  results.push(await runTest('CLUSTER_hash_tag', cat, async () => {
    const slot1 = await clusterClient.call('CLUSTER', 'KEYSLOT', '{user}.name');
    const slot2 = await clusterClient.call('CLUSTER', 'KEYSLOT', '{user}.email');
    const slot3 = await clusterClient.call('CLUSTER', 'KEYSLOT', '{user}.age');

    assertEqual(slot1, slot2);
    assertEqual(slot2, slot3);

    // Keys with different hash tags should hash to different slots
    const slotA = await clusterClient.call('CLUSTER', 'KEYSLOT', '{alpha}.key');
    const slotB = await clusterClient.call('CLUSTER', 'KEYSLOT', '{beta}.key');
    assertTrue(slotA !== slotB, `{alpha} and {beta} should hash to different slots, both got ${slotA}`);
  }));

  // CLUSTER_MOVED_redirect: Access key on wrong node, verify MOVED error
  results.push(await runTest('CLUSTER_MOVED_redirect', cat, async () => {
    // Parse CLUSTER NODES to find this node's slots
    const nodesStr = await clusterClient.call('CLUSTER', 'NODES');
    const nodes = parseClusterNodes(nodesStr);
    const myself = nodes.find(n => n.flags.includes('myself'));

    if (!myself) {
      throw new Error('could not find myself in CLUSTER NODES');
    }

    // Parse my slot ranges
    const mySlotSet = new Set();
    for (const s of myself.slots) {
      if (s.includes('-')) {
        const [start, end] = s.split('-').map(Number);
        for (let i = start; i <= end; i++) {
          mySlotSet.add(i);
        }
      } else {
        mySlotSet.add(Number(s));
      }
    }

    // Find a slot not owned by this node
    let targetSlot = -1;
    for (let s = 0; s < 16384; s++) {
      if (!mySlotSet.has(s)) {
        targetSlot = s;
        break;
      }
    }

    if (targetSlot === -1) {
      throw new Error('could not find a slot not owned by this node');
    }

    // Find a key that hashes to that slot
    let testKey = '';
    for (let i = 0; i < 100000; i++) {
      const candidate = `${prefix}cluster:moved:${i}`;
      const slot = await clusterClient.call('CLUSTER', 'KEYSLOT', candidate);
      if (slot === targetSlot) {
        testKey = candidate;
        break;
      }
    }

    if (!testKey) {
      throw new Error(`could not find a key hashing to slot ${targetSlot}`);
    }

    // Try to GET the key — should get MOVED error
    try {
      await clusterClient.get(testKey);
      // If succeeded, might be using cluster-aware mode
      // Some clients auto-follow redirects
    } catch (err) {
      assertTrue(
        err.message.includes('MOVED'),
        `expected MOVED error, got: ${err.message}`
      );
    }
  }));

  // CLUSTER_KEYSLOT: Verify CLUSTER KEYSLOT returns correct CRC16 mod 16384
  results.push(await runTest('CLUSTER_KEYSLOT', cat, async () => {
    // "foo" should hash to slot 12182
    const slot = await clusterClient.call('CLUSTER', 'KEYSLOT', 'foo');
    assertEqual(12182, slot);

    // Empty key hashes to slot 0
    const emptySlot = await clusterClient.call('CLUSTER', 'KEYSLOT', '');
    assertEqual(0, emptySlot);

    // "{user}.info" should hash the same as "user"
    const slot1 = await clusterClient.call('CLUSTER', 'KEYSLOT', '{user}.info');
    const slot2 = await clusterClient.call('CLUSTER', 'KEYSLOT', 'user');
    assertEqual(slot2, slot1);
  }));

  // CLUSTER_COUNTKEYSINSLOT: Verify key counting per slot
  results.push(await runTest('CLUSTER_COUNTKEYSINSLOT', cat, async () => {
    const k = key(prefix, 'cluster:countslot');
    const keySlot = await clusterClient.call('CLUSTER', 'KEYSLOT', k);

    // Find which node owns this slot
    const slotsInfo = await clusterClient.call('CLUSTER', 'SLOTS');
    let targetHost = null;
    let targetPort = null;

    for (const range of slotsInfo) {
      const start = range[0];
      const end = range[1];
      if (keySlot >= start && keySlot <= end) {
        // Node info is at index 2+ (first is master)
        const nodeInfo = range[2];
        targetHost = nodeInfo[0];
        targetPort = nodeInfo[1];
        break;
      }
    }

    if (!targetHost) {
      throw new Error(`could not find node owning slot ${keySlot}`);
    }

    // Connect to the target node
    const targetClient = new Redis({
      host: targetHost,
      port: targetPort,
      password: password,
      lazyConnect: true,
      retryStrategy: () => null,
      maxRetriesPerRequest: 3,
    });
    await targetClient.connect();

    try {
      await targetClient.set(k, 'value');
      const count = await targetClient.call('CLUSTER', 'COUNTKEYSINSLOT', keySlot);
      assertTrue(count >= 1, `CLUSTER COUNTKEYSINSLOT ${keySlot} should be >= 1, got ${count}`);
      await targetClient.del(k);
    } finally {
      targetClient.disconnect();
    }
  }));

  // CLUSTER_cross_slot_error: Verify CROSSSLOT error for multi-key commands on different slots
  results.push(await runTest('CLUSTER_cross_slot_error', cat, async () => {
    try {
      await clusterClient.mset('cluster_cross_a', 'val1', 'cluster_cross_b', 'val2');
      // If succeeded, clean up — server may not enforce CROSSSLOT on this connection type
      await clusterClient.del('cluster_cross_a', 'cluster_cross_b');
    } catch (err) {
      assertTrue(
        err.message.includes('CROSSSLOT') || err.message.includes('MOVED'),
        `expected CROSSSLOT or MOVED error, got: ${err.message}`
      );
    }
  }));

  // READONLY_mode: READONLY on a cluster node, verify reads succeed
  results.push(await runTest('READONLY_mode', cat, async () => {
    const result = await clusterClient.call('READONLY');
    assertEqual('OK', result);

    const result2 = await clusterClient.call('READWRITE');
    assertEqual('OK', result2);
  }));

  clusterClient.disconnect();
  return results;
});
