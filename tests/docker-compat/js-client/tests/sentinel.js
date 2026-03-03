const Redis = require('ioredis');
const { register, runTest, skipTest, key, assertEqual, assertTrue, sleep } = require('./framework');

register('sentinel', async (redis, prefix) => {
  const results = [];
  const cat = 'sentinel';

  const sentinelHost = process.env.REDIS_SENTINEL_HOST || '';
  const sentinelPort = parseInt(process.env.REDIS_SENTINEL_PORT || '26379', 10);

  // If sentinel is not configured, skip all sentinel tests
  if (!sentinelHost) {
    const reason = 'REDIS_SENTINEL_HOST not set';
    results.push(skipTest('SENTINEL_master_info', cat, reason));
    results.push(skipTest('SENTINEL_replicas', cat, reason));
    results.push(skipTest('SENTINEL_sentinels', cat, reason));
    results.push(skipTest('SENTINEL_get_master_addr', cat, reason));
    results.push(skipTest('SENTINEL_ckquorum', cat, reason));
    results.push(skipTest('SENTINEL_failover', cat, reason));
    results.push(skipTest('SENTINEL_client_discovery', cat, reason));
    return results;
  }

  const password = redis.options.password;

  // Create a direct connection to the sentinel
  const sentinel = new Redis({
    host: sentinelHost,
    port: sentinelPort,
    lazyConnect: true,
    retryStrategy: () => null,
    maxRetriesPerRequest: 3,
  });
  await sentinel.connect();

  // Wait for sentinel to be ready
  for (let i = 0; i < 30; i++) {
    try {
      await sentinel.ping();
      break;
    } catch {
      await sleep(1000);
    }
  }

  // Helper: parse sentinel array response into array of maps
  const parseSentinelArray = (arr) => {
    if (!Array.isArray(arr)) return [];
    return arr.map(item => {
      const map = {};
      if (Array.isArray(item)) {
        for (let i = 0; i < item.length - 1; i += 2) {
          map[item[i]] = item[i + 1];
        }
      }
      return map;
    });
  };

  // Helper: parse sentinel flat array into map
  const parseSentinelMap = (arr) => {
    const map = {};
    if (Array.isArray(arr)) {
      for (let i = 0; i < arr.length - 1; i += 2) {
        map[arr[i]] = arr[i + 1];
      }
    }
    return map;
  };

  // SENTINEL_master_info: SENTINEL MASTER mymaster — verify master host/port/flags
  results.push(await runTest('SENTINEL_master_info', cat, async () => {
    const result = await sentinel.call('SENTINEL', 'MASTER', 'mymaster');
    const master = parseSentinelMap(result);

    assertEqual('mymaster', master.name);
    assertTrue(
      master.flags && master.flags.includes('master'),
      `expected flags to contain 'master', got: ${master.flags}`
    );
    assertTrue(master.ip !== undefined && master.ip !== '', 'expected non-empty ip');
    assertTrue(master.port !== undefined && master.port !== '', 'expected non-empty port');
  }));

  // SENTINEL_replicas: SENTINEL REPLICAS mymaster — verify replica list
  results.push(await runTest('SENTINEL_replicas', cat, async () => {
    let result;
    try {
      result = await sentinel.call('SENTINEL', 'REPLICAS', 'mymaster');
    } catch {
      // Fallback to SLAVES for older implementations
      result = await sentinel.call('SENTINEL', 'SLAVES', 'mymaster');
    }

    const replicas = parseSentinelArray(result);
    assertTrue(replicas.length >= 1, `expected at least 1 replica, got ${replicas.length}`);
    assertTrue(replicas[0].ip !== undefined, 'expected ip field in replica info');
    assertTrue(replicas[0].port !== undefined, 'expected port field in replica info');
  }));

  // SENTINEL_sentinels: SENTINEL SENTINELS mymaster — verify sentinel peer list
  results.push(await runTest('SENTINEL_sentinels', cat, async () => {
    const result = await sentinel.call('SENTINEL', 'SENTINELS', 'mymaster');
    const sentinels = parseSentinelArray(result);

    assertTrue(sentinels.length >= 2, `expected at least 2 sentinel peers, got ${sentinels.length}`);
    assertTrue(sentinels[0].ip !== undefined, 'expected ip field in sentinel peer info');
    assertTrue(sentinels[0].port !== undefined, 'expected port field in sentinel peer info');
  }));

  // SENTINEL_get_master_addr: SENTINEL GET-MASTER-ADDR-BY-NAME mymaster — verify address
  results.push(await runTest('SENTINEL_get_master_addr', cat, async () => {
    const addr = await sentinel.call('SENTINEL', 'GET-MASTER-ADDR-BY-NAME', 'mymaster');

    assertTrue(Array.isArray(addr) && addr.length === 2, `expected [host, port] array, got: ${JSON.stringify(addr)}`);
    assertTrue(addr[0] !== '' && addr[0] !== null, 'expected non-empty host');
    assertTrue(addr[1] !== '' && addr[1] !== null, 'expected non-empty port');
  }));

  // SENTINEL_ckquorum: SENTINEL CKQUORUM mymaster — verify quorum check
  results.push(await runTest('SENTINEL_ckquorum', cat, async () => {
    const result = await sentinel.call('SENTINEL', 'CKQUORUM', 'mymaster');
    const msg = String(result);
    assertTrue(
      msg.includes('OK') || msg.includes('ok') || msg.includes('reachable'),
      `expected quorum OK response, got: ${msg}`
    );
  }));

  // SENTINEL_failover: Trigger failover, verify master address still available
  results.push(await runTest('SENTINEL_failover', cat, async () => {
    try {
      await sentinel.call('SENTINEL', 'FAILOVER', 'mymaster');
    } catch (err) {
      const errMsg = err.message || '';
      // Acceptable errors: no good slave, failover already in progress
      if (errMsg.includes('NOGOODSLAVE') || errMsg.includes('in progress') ||
          errMsg.includes('INPROG') || errMsg.includes('no good')) {
        return; // Acceptable — infrastructure works but conditions not met
      }
      throw err;
    }

    // Wait for failover to stabilize
    await sleep(3000);

    // Verify we can still get master address
    const addr = await sentinel.call('SENTINEL', 'GET-MASTER-ADDR-BY-NAME', 'mymaster');
    assertTrue(Array.isArray(addr) && addr.length === 2, 'expected valid master address after failover');
    assertTrue(addr[0] !== '' && addr[1] !== '', 'expected non-empty master address after failover');
  }));

  // SENTINEL_client_discovery: Connect via sentinel, verify automatic master discovery
  results.push(await runTest('SENTINEL_client_discovery', cat, async () => {
    // Wait for sentinel to recover from any previous failover
    await sleep(2000);

    // Use ioredis sentinel-aware client
    const client = new Redis({
      sentinels: [{ host: sentinelHost, port: sentinelPort }],
      name: 'mymaster',
      password: password,
      db: 0,
      lazyConnect: true,
      sentinelRetryStrategy: (times) => {
        if (times > 5) return null;
        return 1000;
      },
      maxRetriesPerRequest: 3,
    });

    try {
      await client.connect();

      // Wait for connection to be ready
      for (let i = 0; i < 10; i++) {
        try {
          await client.ping();
          break;
        } catch {
          await sleep(1000);
        }
      }

      const k = key(prefix, 'sentinel:discovery_test');
      try {
        await client.set(k, 'discovered', 'EX', 30);
        const val = await client.get(k);
        assertEqual('discovered', val);
      } finally {
        await client.del(k);
      }
    } finally {
      client.disconnect();
    }
  }));

  sentinel.disconnect();
  return results;
});
