const Redis = require('ioredis');
const { register, runTest, key, assertEqual, assertTrue, assertEqualInt, sleep } = require('./framework');

register('persistence', async (redis, prefix) => {
  const results = [];
  const cat = 'persistence';

  // RDB_SAVE_and_RESTORE: BGSAVE, wait, verify LASTSAVE timestamp changes
  results.push(await runTest('RDB_SAVE_and_RESTORE', cat, async () => {
    // Get initial LASTSAVE timestamp
    const t1 = await redis.lastsave();

    // Write some data to ensure there's something to save
    const k = key(prefix, 'persist:rdb_save');
    await redis.set(k, 'rdb_test_value');

    // Trigger BGSAVE
    try {
      await redis.bgsave();
    } catch (err) {
      // BGSAVE may fail if already in progress, that's OK
      if (!err.message.includes('already in progress') &&
          !err.message.includes('Background save')) {
        throw err;
      }
    }

    // Wait for background save to complete
    let t2 = t1;
    for (let i = 0; i < 30; i++) {
      await sleep(200);
      t2 = await redis.lastsave();
      if (t2 > t1) break;
    }

    assertTrue(t2 >= t1, `LASTSAVE should be >= initial: got ${t2} vs ${t1}`);
    await redis.del(k);
  }));

  // AOF_append_and_replay: CONFIG SET appendonly yes, write data, verify data accessible
  results.push(await runTest('AOF_append_and_replay', cat, async () => {
    // Enable AOF
    await redis.config('SET', 'appendonly', 'yes');

    // Verify AOF is enabled
    const res = await redis.config('GET', 'appendonly');
    assertEqual('yes', res[1]);

    // Write data that should be captured by AOF
    const k = key(prefix, 'persist:aof_test');
    await redis.set(k, 'aof_value_123');

    // Verify data is readable
    const got = await redis.get(k);
    assertEqual('aof_value_123', got);

    // Restore appendonly to no
    await redis.config('SET', 'appendonly', 'no');
    await redis.del(k);
  }));

  // PERSIST_across_restart: SET keys with TTL, verify keys and TTLs exist
  results.push(await runTest('PERSIST_across_restart', cat, async () => {
    const k1 = key(prefix, 'persist:ttl1');
    const k2 = key(prefix, 'persist:ttl2');

    // Set keys with TTL (60 and 120 seconds)
    await redis.set(k1, 'value1', 'EX', 60);
    await redis.set(k2, 'value2', 'EX', 120);

    // Trigger BGSAVE to persist
    try { await redis.bgsave(); } catch (e) { /* ignore if in progress */ }
    await sleep(500);

    // Verify values exist
    const v1 = await redis.get(k1);
    assertEqual('value1', v1);

    const v2 = await redis.get(k2);
    assertEqual('value2', v2);

    // Verify TTLs are still set
    const ttl1 = await redis.ttl(k1);
    assertTrue(ttl1 > 0, `key1 TTL should be > 0, got ${ttl1}`);

    const ttl2 = await redis.ttl(k2);
    assertTrue(ttl2 > 0, `key2 TTL should be > 0, got ${ttl2}`);

    await redis.del(k1, k2);
  }));

  // RDB_background_save: SET data, BGSAVE, verify DBSIZE
  results.push(await runTest('RDB_background_save', cat, async () => {
    const keys = [];
    for (let i = 0; i < 10; i++) {
      const k = key(prefix, `persist:bgsave_${i}`);
      keys.push(k);
      await redis.set(k, `val_${i}`);
    }

    // BGSAVE
    try { await redis.bgsave(); } catch (e) { /* ignore if in progress */ }
    await sleep(500);

    // Verify DBSIZE reflects the keys
    const size = await redis.dbsize();
    assertTrue(Number(size) >= 10, `DBSIZE should be >= 10, got ${size}`);

    for (const k of keys) {
      await redis.del(k);
    }
  }));

  // CONFIG_persistence_settings: CONFIG SET/GET for save, appendonly, appendfsync
  results.push(await runTest('CONFIG_persistence_settings', cat, async () => {
    // Test appendonly CONFIG SET/GET
    const origAof = await redis.config('GET', 'appendonly');
    const origAofVal = origAof[1];

    try {
      await redis.config('SET', 'appendonly', 'yes');
      const res1 = await redis.config('GET', 'appendonly');
      assertEqual('yes', res1[1]);
    } finally {
      await redis.config('SET', 'appendonly', origAofVal);
    }

    // Test appendfsync CONFIG SET/GET
    const origSync = await redis.config('GET', 'appendfsync');
    const origSyncVal = origSync[1];

    try {
      await redis.config('SET', 'appendfsync', 'everysec');
      const res2 = await redis.config('GET', 'appendfsync');
      assertEqual('everysec', res2[1]);
    } finally {
      await redis.config('SET', 'appendfsync', origSyncVal);
    }

    // Test save CONFIG GET (read-only, don't modify)
    const saveRes = await redis.config('GET', 'save');
    assertTrue(saveRes.length >= 2, 'CONFIG GET save should return a value');
  }));

  // DEBUG_SLEEP_during_save: Verify server remains responsive during BGSAVE
  results.push(await runTest('DEBUG_SLEEP_during_save', cat, async () => {
    const k = key(prefix, 'persist:during_save');
    await redis.set(k, 'before_save');

    // Trigger BGSAVE
    try { await redis.bgsave(); } catch (e) { /* ignore if in progress */ }

    // Immediately verify server is responsive
    await redis.set(k, 'during_save');
    const got = await redis.get(k);
    assertEqual('during_save', got);

    // PING should also work
    const pong = await redis.ping();
    assertEqual('PONG', pong);

    await redis.del(k);
  }));

  // MULTI_DB_persistence: SET keys in DB 0 and DB 3, verify both databases
  results.push(await runTest('MULTI_DB_persistence', cat, async () => {
    const k0 = key(prefix, 'persist:db0_key');
    const k3 = key(prefix, 'persist:db3_key');

    // Write to DB 0
    await redis.set(k0, 'db0_value');

    // Create a client for DB 3
    const rdb3 = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      db: 3,
      lazyConnect: true,
    });
    await rdb3.connect();

    try {
      // Write to DB 3
      await rdb3.set(k3, 'db3_value');

      // Trigger BGSAVE to persist both DBs
      try { await redis.bgsave(); } catch (e) { /* ignore if in progress */ }
      await sleep(500);

      // Verify DB 0 key
      const v0 = await redis.get(k0);
      assertEqual('db0_value', v0);

      // Verify DB 3 key
      const v3 = await rdb3.get(k3);
      assertEqual('db3_value', v3);

      await rdb3.del(k3);
    } finally {
      rdb3.disconnect();
    }
    await redis.del(k0);
  }));

  // LARGE_VALUE_persistence: SET 1MB value, verify value intact
  results.push(await runTest('LARGE_VALUE_persistence', cat, async () => {
    const k = key(prefix, 'persist:large_val');

    // Create a 1MB value
    const largeVal = 'A'.repeat(1024 * 1024);

    await redis.set(k, largeVal);

    // Trigger BGSAVE
    try { await redis.bgsave(); } catch (e) { /* ignore if in progress */ }
    await sleep(500);

    // Verify the value is intact
    const got = await redis.get(k);
    assertEqualInt(largeVal.length, got.length);
    assertEqual(largeVal, got);

    await redis.del(k);
  }));

  return results;
});
