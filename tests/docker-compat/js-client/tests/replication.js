const Redis = require('ioredis');
const { register, runTest, skipTest, key, assertEqual, assertTrue, sleep } = require('./framework');

register('replication', async (redis, prefix) => {
  const results = [];
  const cat = 'replication';

  const replicaHost = process.env.REDIS_REPLICA_HOST || '';
  const replicaPort = parseInt(process.env.REDIS_REPLICA_PORT || '6379', 10);

  // If replica is not configured, skip all replication tests
  if (!replicaHost) {
    const reason = 'REDIS_REPLICA_HOST not set';
    results.push(skipTest('INFO_replication', cat, reason));
    results.push(skipTest('REPLICAOF_basic', cat, reason));
    results.push(skipTest('REPLICATION_data_sync', cat, reason));
    results.push(skipTest('REPLICATION_multi_commands', cat, reason));
    results.push(skipTest('REPLICATION_expiry', cat, reason));
    results.push(skipTest('REPLICA_read_only', cat, reason));
    results.push(skipTest('REPLICATION_after_reconnect', cat, reason));
    results.push(skipTest('WAIT_command', cat, reason));
    results.push(skipTest('REPLICATION_SELECT', cat, reason));
    return results;
  }

  const password = redis.options.password;

  const createReplicaClient = (db = 0) => new Redis({
    host: replicaHost,
    port: replicaPort,
    password: password,
    db,
    lazyConnect: true,
    retryStrategy: () => null,
    maxRetriesPerRequest: 3,
  });

  // Connect to replica and wait for it to be ready
  const replica = createReplicaClient(0);
  await replica.connect();
  for (let i = 0; i < 30; i++) {
    try {
      await replica.ping();
      break;
    } catch {
      await sleep(1000);
    }
  }

  // Helper: wait for a key to appear on the replica
  const waitForKey = async (client, k, maxWaitMs = 5000) => {
    const deadline = Date.now() + maxWaitMs;
    while (Date.now() < deadline) {
      const val = await client.get(k);
      if (val !== null) return val;
      await sleep(100);
    }
    throw new Error(`key ${k} not found on replica after ${maxWaitMs}ms`);
  };

  // INFO_replication: Verify INFO replication section on master
  results.push(await runTest('INFO_replication', cat, async () => {
    const info = await redis.info('replication');
    assertTrue(info.includes('role:master'), `expected role:master in INFO replication`);

    const match = info.match(/connected_slaves:(\d+)/);
    assertTrue(match !== null, 'connected_slaves not found in INFO replication');
    const count = parseInt(match[1], 10);
    assertTrue(count >= 1, `connected_slaves should be >= 1, got ${count}`);
  }));

  // REPLICAOF_basic: Verify replica is connected to master
  results.push(await runTest('REPLICAOF_basic', cat, async () => {
    const info = await replica.info('replication');
    assertTrue(info.includes('role:slave'), `expected role:slave in replica INFO replication`);
    assertTrue(info.includes('master_link_status:up'), `expected master_link_status:up in replica INFO`);
  }));

  // REPLICATION_data_sync: SET key on master, verify key appears on replica
  results.push(await runTest('REPLICATION_data_sync', cat, async () => {
    const k = key(prefix, 'repl:data_sync');
    try {
      await redis.set(k, 'replicated_value');
      const val = await waitForKey(replica, k);
      assertEqual('replicated_value', val);
    } finally {
      await redis.del(k);
    }
  }));

  // REPLICATION_multi_commands: SET multiple keys on master, verify all replicated
  results.push(await runTest('REPLICATION_multi_commands', cat, async () => {
    const keys = [];
    for (let i = 0; i < 10; i++) {
      const k = key(prefix, `repl:multi_${i}`);
      keys.push(k);
      await redis.set(k, `value_${i}`);
    }
    try {
      // Wait for last key to appear on replica
      await waitForKey(replica, keys[9]);
      // Verify all keys
      for (let i = 0; i < 10; i++) {
        const val = await replica.get(keys[i]);
        assertEqual(`value_${i}`, val);
      }
    } finally {
      for (const k of keys) await redis.del(k);
    }
  }));

  // REPLICATION_expiry: SET key with TTL on master, verify TTL replicated
  results.push(await runTest('REPLICATION_expiry', cat, async () => {
    const k = key(prefix, 'repl:expiry');
    try {
      await redis.set(k, 'ttl_value', 'EX', 120);
      await waitForKey(replica, k);
      const ttl = await replica.ttl(k);
      assertTrue(ttl > 0, `replica TTL should be > 0, got ${ttl}`);
    } finally {
      await redis.del(k);
    }
  }));

  // REPLICA_read_only: Verify writes to replica return READONLY error
  results.push(await runTest('REPLICA_read_only', cat, async () => {
    const k = key(prefix, 'repl:readonly_test');
    try {
      await replica.set(k, 'should_fail');
      await replica.del(k);
      throw new Error('expected READONLY error, but SET succeeded on replica');
    } catch (err) {
      if (err.message.includes('expected READONLY')) throw err;
      assertTrue(
        err.message.includes('READONLY') || err.message.includes('readonly'),
        `expected READONLY error, got: ${err.message}`
      );
    }
  }));

  // REPLICATION_after_reconnect: Disconnect replica, write to master, reconnect, verify sync
  results.push(await runTest('REPLICATION_after_reconnect', cat, async () => {
    // Get master host/port from replica's INFO
    const info = await replica.info('replication');
    const hostMatch = info.match(/master_host:(\S+)/);
    const portMatch = info.match(/master_port:(\d+)/);
    if (!hostMatch || !portMatch) {
      throw new Error('could not determine master host/port from replica INFO');
    }
    const masterHost = hostMatch[1];
    const masterPort = portMatch[1];

    // Disconnect replica
    await replica.call('REPLICAOF', 'NO', 'ONE');

    const k = key(prefix, 'repl:reconnect_test');
    try {
      // Write data on master while replica is disconnected
      await redis.set(k, 'after_disconnect');

      // Reconnect replica to master
      await replica.call('REPLICAOF', masterHost, masterPort);

      // Wait for sync and key to appear
      const val = await waitForKey(replica, k, 10000);
      assertEqual('after_disconnect', val);
    } finally {
      await redis.del(k);
    }
  }));

  // WAIT_command: SET key, WAIT 1 0, verify acknowledged
  results.push(await runTest('WAIT_command', cat, async () => {
    const k = key(prefix, 'repl:wait_test');
    try {
      await redis.set(k, 'wait_value');
      // WAIT for 1 replica with 5 second timeout
      const numReplicas = await redis.wait(1, 5000);
      assertTrue(numReplicas >= 1, `WAIT should return >= 1, got ${numReplicas}`);
    } finally {
      await redis.del(k);
    }
  }));

  // REPLICATION_SELECT: Master uses SELECT 3 + SET, verify replica DB 3 has key
  results.push(await runTest('REPLICATION_SELECT', cat, async () => {
    const k = key(prefix, 'repl:select_db3');

    // Create a client for master DB 3
    const masterDB3 = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      db: 3,
      lazyConnect: true,
    });
    await masterDB3.connect();

    // Create a client for replica DB 3
    const replicaDB3 = createReplicaClient(3);
    await replicaDB3.connect();

    try {
      await masterDB3.set(k, 'db3_replicated');
      const val = await waitForKey(replicaDB3, k);
      assertEqual('db3_replicated', val);
      await masterDB3.del(k);
    } finally {
      replicaDB3.disconnect();
      masterDB3.disconnect();
    }
  }));

  replica.disconnect();
  return results;
});
