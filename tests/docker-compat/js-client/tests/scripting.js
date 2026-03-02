const { register, runTest, key, assertEqual, assertTrue } = require('./framework');

register('scripting', async (redis, prefix) => {
  const results = [];
  const cat = 'scripting';

  results.push(await runTest('EVAL_simple', cat, async () => {
    const res = await redis.eval('return 42', 0);
    assertEqual(42, res);
  }));

  results.push(await runTest('EVAL_with_keys_and_args', cat, async () => {
    const k = key(prefix, 'lua:kv');
    const script = `
      redis.call('SET', KEYS[1], ARGV[1])
      return redis.call('GET', KEYS[1])
    `;
    const res = await redis.eval(script, 1, k, 'lua_value');
    assertEqual('lua_value', res);
    await redis.del(k);
  }));

  results.push(await runTest('EVALSHA_after_SCRIPT_LOAD', cat, async () => {
    const k = key(prefix, 'lua:sha');
    const script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    const sha = await redis.call('SCRIPT', 'LOAD', script);
    assertTrue(sha.length === 40, 'SHA should be 40 hex chars');
    const res = await redis.evalsha(sha, 1, k, 'sha_value');
    assertEqual('sha_value', res);
    await redis.del(k);
  }));

  results.push(await runTest('script_error_handling', cat, async () => {
    let errored = false;
    try {
      await redis.eval("return redis.call('GET')", 0);
    } catch {
      errored = true;
    }
    assertTrue(errored, 'script with error should throw');
  }));

  results.push(await runTest('redis_call_vs_pcall', cat, async () => {
    const k = key(prefix, 'lua:pcall');
    await redis.set(k, 'string_val');

    // redis.call raises error on WRONGTYPE
    let errored = false;
    try {
      await redis.eval("return redis.call('LPUSH', KEYS[1], 'bad')", 1, k);
    } catch {
      errored = true;
    }
    assertTrue(errored, 'redis.call should propagate WRONGTYPE error');

    // redis.pcall catches error
    const res = await redis.eval(
      "local ok, err = pcall(redis.call, 'LPUSH', KEYS[1], 'bad'); if ok then return 'ok' else return 'caught' end",
      1, k
    );
    assertEqual('caught', res);
    await redis.del(k);
  }));

  results.push(await runTest('SCRIPT_EXISTS_FLUSH', cat, async () => {
    const script = "return 'test_exists'";
    const sha = await redis.call('SCRIPT', 'LOAD', script);
    const exists = await redis.call('SCRIPT', 'EXISTS', sha, '0000000000000000000000000000000000000000');
    assertTrue(exists[0] === 1, 'loaded script should exist');
    assertTrue(exists[1] === 0, 'fake SHA should not exist');
    await redis.call('SCRIPT', 'FLUSH');
    const exists2 = await redis.call('SCRIPT', 'EXISTS', sha);
    assertTrue(exists2[0] === 0, 'script should not exist after FLUSH');
  }));

  return results;
});
