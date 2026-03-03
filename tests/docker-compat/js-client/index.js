const Redis = require('ioredis');
const { runAll } = require('./tests/framework');

// Load all test modules
require('./tests/connection');
require('./tests/strings');
require('./tests/hashes');
require('./tests/lists');
require('./tests/sets');
require('./tests/sorted_sets');
require('./tests/keys');
require('./tests/transactions');
require('./tests/pubsub');
require('./tests/pipeline');
require('./tests/scripting');
require('./tests/streams');
require('./tests/advanced');
require('./tests/edge_cases');
require('./tests/server');
require('./tests/persistence');
require('./tests/acl');
require('./tests/blocking');
require('./tests/js_specific');

async function main() {
  const host = process.env.REDIS_HOST || 'localhost';
  const port = parseInt(process.env.REDIS_PORT || '6379', 10);
  const password = process.env.REDIS_PASSWORD || '';
  const prefix = process.env.TEST_PREFIX || 'jstest:';

  const redis = new Redis({
    host,
    port,
    password: password || undefined,
    db: 0,
    lazyConnect: true,
    retryStrategy: (times) => {
      if (times > 30) return null;
      return 1000;
    },
    maxRetriesPerRequest: 3,
  });

  // Wait for server to be ready
  let connected = false;
  for (let i = 0; i < 30; i++) {
    try {
      await redis.connect();
      await redis.ping();
      connected = true;
      break;
    } catch {
      if (redis.status === 'ready' || redis.status === 'connect') {
        try { redis.disconnect(); } catch {}
      }
      await new Promise(r => setTimeout(r, 1000));
      // Recreate if needed
      if (redis.status !== 'wait') {
        redis.disconnect();
      }
    }
  }

  if (!connected) {
    // One more attempt
    try {
      if (redis.status === 'wait') await redis.connect();
      await redis.ping();
      connected = true;
    } catch (err) {
      process.stderr.write(`Failed to connect to Redis at ${host}:${port}: ${err.message}\n`);
      process.exit(1);
    }
  }

  // Detect server type
  let serverType = 'unknown';
  let serverVersion = '';
  try {
    const info = await redis.info('server');
    const lines = info.split('\n');
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.startsWith('redis_version:')) {
        serverVersion = trimmed.replace('redis_version:', '');
      }
    }
    if (info.includes('rlightning') || info.includes('rLightning')) {
      serverType = 'rlightning';
    } else {
      serverType = 'redis';
    }
  } catch {}

  process.stderr.write(`Connected to ${serverType} at ${host}:${port} (version: ${serverVersion})\n`);
  process.stderr.write(`Running tests with prefix: ${prefix}\n`);

  const startTime = Date.now();
  const results = await runAll(redis, prefix);
  const totalDuration = Date.now() - startTime;

  // Build summary
  const summary = {
    total: 0,
    passed: 0,
    failed: 0,
    skipped: 0,
    errored: 0,
    pass_rate: 0,
    categories: {},
  };

  const catTotals = {};
  const catPassed = {};

  for (const r of results) {
    summary.total++;
    catTotals[r.category] = (catTotals[r.category] || 0) + 1;
    switch (r.status) {
      case 'pass':
        summary.passed++;
        catPassed[r.category] = (catPassed[r.category] || 0) + 1;
        break;
      case 'fail':
        summary.failed++;
        break;
      case 'skip':
        summary.skipped++;
        break;
      case 'error':
        summary.errored++;
        break;
    }
  }

  if (summary.total > 0) {
    summary.pass_rate = (summary.passed / summary.total) * 100;
  }

  for (const cat of Object.keys(catTotals)) {
    const total = catTotals[cat];
    const passed = catPassed[cat] || 0;
    summary.categories[cat] = {
      total,
      passed,
      pass_rate: total > 0 ? (passed / total) * 100 : 0,
    };
  }

  const report = {
    client: {
      language: 'javascript',
      library: 'ioredis',
      version: '5.3.2',
    },
    server: {
      host,
      port,
      type: serverType,
      version: serverVersion,
    },
    timestamp: new Date().toISOString(),
    duration_ms: totalDuration,
    results,
    summary,
  };

  // Output JSON report to stdout
  console.log(JSON.stringify(report, null, 2));

  // Print summary to stderr
  process.stderr.write(`\n=== Test Summary ===\n`);
  process.stderr.write(`Total: ${summary.total} | Pass: ${summary.passed} | Fail: ${summary.failed} | Skip: ${summary.skipped} | Error: ${summary.errored} | Rate: ${summary.pass_rate.toFixed(1)}%\n`);
  for (const [cat, cs] of Object.entries(summary.categories)) {
    process.stderr.write(`  ${cat.padEnd(20)} ${cs.passed}/${cs.total} (${cs.pass_rate.toFixed(1)}%)\n`);
  }

  await redis.quit();
  process.exit(summary.failed > 0 || summary.errored > 0 ? 1 : 0);
}

main().catch(err => {
  process.stderr.write(`Fatal error: ${err.message}\n`);
  process.exit(1);
});
