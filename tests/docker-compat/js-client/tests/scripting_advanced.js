const { register, runTest, key, assertEqual, assertTrue } = require('./framework');

register('scripting_advanced', async (redis, prefix) => {
  const results = [];
  const cat = 'scripting_advanced';

  // EVAL_return_types: Test Lua returning string, integer, table, nil, boolean, error
  results.push(await runTest('EVAL_return_types', cat, async () => {
    // String
    const str = await redis.eval("return 'hello'", 0);
    assertEqual('hello', str);

    // Integer
    const num = await redis.eval('return 123', 0);
    assertEqual(123, num);

    // Table (array)
    const arr = await redis.eval('return {1, 2, 3}', 0);
    assertTrue(Array.isArray(arr), 'table should return as array');
    assertTrue(arr.length === 3, 'table should have 3 elements');

    // Nil (Lua false returns as null in Redis)
    const nil = await redis.eval('return false', 0);
    assertTrue(nil === null, 'false should return as null');

    // Boolean true -> integer 1
    const bool = await redis.eval('return true', 0);
    assertEqual(1, bool);

    // Negative integer
    const neg = await redis.eval('return -42', 0);
    assertEqual(-42, neg);
  }));

  // EVAL_redis_call: Lua calling redis.call('SET', KEYS[1], ARGV[1])
  results.push(await runTest('EVAL_redis_call', cat, async () => {
    const k = key(prefix, 'lua:adv:call');

    // SET via redis.call
    const res = await redis.eval("return redis.call('SET', KEYS[1], ARGV[1])", 1, k, 'myval');
    assertEqual('OK', res);

    // Verify via direct GET
    const val = await redis.get(k);
    assertEqual('myval', val);

    // Multi-command script via redis.call
    const res2 = await redis.eval(`
      redis.call('SET', KEYS[1], ARGV[1])
      local v = redis.call('GET', KEYS[1])
      redis.call('APPEND', KEYS[1], ARGV[2])
      return redis.call('GET', KEYS[1])
    `, 1, k, 'hello', '_world');
    assertEqual('hello_world', res2);

    await redis.del(k);
  }));

  // EVAL_redis_pcall: Lua calling redis.pcall() with error handling
  results.push(await runTest('EVAL_redis_pcall', cat, async () => {
    const k = key(prefix, 'lua:adv:pcall');
    await redis.set(k, 'str_val');

    // pcall catches the WRONGTYPE error
    const res = await redis.eval(`
      local ok, err = pcall(redis.call, 'LPUSH', KEYS[1], 'x')
      if not ok then
        return 'error_caught'
      end
      return 'no_error'
    `, 1, k);
    assertEqual('error_caught', res);

    // redis.pcall also catches the error (direct wrapper)
    const res2 = await redis.eval(`
      local result = redis.pcall('LPUSH', KEYS[1], 'x')
      if type(result) == 'table' and result.err then
        return 'pcall_error: ' .. result.err
      end
      return 'no_error'
    `, 1, k);
    assertTrue(String(res2).includes('pcall_error'), 'pcall should catch error');

    await redis.del(k);
  }));

  // EVALSHA_cached: SCRIPT LOAD, then EVALSHA, verify execution
  results.push(await runTest('EVALSHA_cached', cat, async () => {
    const k = key(prefix, 'lua:adv:sha');

    const script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    const sha = await redis.call('SCRIPT', 'LOAD', script);
    assertTrue(sha.length === 40, 'SHA should be 40 hex chars');

    // First call
    const res1 = await redis.evalsha(sha, 1, k, 'cached1');
    assertEqual('cached1', res1);

    // Second call with different args (cached script reused)
    const res2 = await redis.evalsha(sha, 1, k, 'cached2');
    assertEqual('cached2', res2);

    // EVALSHA with bad SHA should fail with NOSCRIPT
    let errored = false;
    try {
      await redis.evalsha('0000000000000000000000000000000000000000', 1, k, 'x');
    } catch {
      errored = true;
    }
    assertTrue(errored, 'EVALSHA with bad SHA should fail with NOSCRIPT');

    await redis.del(k);
  }));

  // SCRIPT_EXISTS: SCRIPT LOAD, SCRIPT EXISTS with SHA, verify true
  results.push(await runTest('SCRIPT_EXISTS', cat, async () => {
    const sha1 = await redis.call('SCRIPT', 'LOAD', "return 'exists_test_1'");
    const sha2 = await redis.call('SCRIPT', 'LOAD', "return 'exists_test_2'");
    const fakeSha = '0000000000000000000000000000000000000000';

    const exists = await redis.call('SCRIPT', 'EXISTS', sha1, sha2, fakeSha);
    assertTrue(exists.length === 3, 'should return 3 results');
    assertTrue(exists[0] === 1, 'script1 should exist');
    assertTrue(exists[1] === 1, 'script2 should exist');
    assertTrue(exists[2] === 0, 'fake SHA should not exist');
  }));

  // SCRIPT_FLUSH: Load scripts, SCRIPT FLUSH, verify NOSCRIPT on EVALSHA
  results.push(await runTest('SCRIPT_FLUSH', cat, async () => {
    const script = "return 'flush_test'";
    const sha = await redis.call('SCRIPT', 'LOAD', script);

    // Verify script exists
    const exists = await redis.call('SCRIPT', 'EXISTS', sha);
    assertTrue(exists[0] === 1, 'script should exist before flush');

    // Flush
    await redis.call('SCRIPT', 'FLUSH');

    // Verify NOSCRIPT error on EVALSHA
    let errored = false;
    try {
      await redis.evalsha(sha, 0);
    } catch (err) {
      errored = true;
      assertTrue(String(err).includes('NOSCRIPT'), 'error should be NOSCRIPT');
    }
    assertTrue(errored, 'EVALSHA should fail after flush');
  }));

  // FUNCTION_LOAD: Load Lua function library (Redis 7.0 FUNCTION LOAD)
  results.push(await runTest('FUNCTION_LOAD', cat, async () => {
    // Clean up
    try { await redis.call('FUNCTION', 'FLUSH'); } catch {}

    const code = "#!lua name=testlib\nredis.register_function('myfunc', function(keys, args) return 'hello_from_func' end)";
    const res = await redis.call('FUNCTION', 'LOAD', code);
    assertEqual('testlib', res);

    // Verify library appears in FUNCTION LIST
    const list = await redis.call('FUNCTION', 'LIST');
    assertTrue(list !== null && list !== undefined, 'FUNCTION LIST should return results');

    // Clean up
    await redis.call('FUNCTION', 'FLUSH');
  }));

  // FCALL_basic: FCALL loaded function, verify execution
  results.push(await runTest('FCALL_basic', cat, async () => {
    const k = key(prefix, 'lua:adv:fcall');

    try {
      // Clean up
      try { await redis.call('FUNCTION', 'FLUSH'); } catch {}

      // Load function that sets and returns a value
      const code = "#!lua name=fcallib\nredis.register_function('setget', function(keys, args) redis.call('SET', keys[1], args[1]); return redis.call('GET', keys[1]) end)";
      await redis.call('FUNCTION', 'LOAD', code);

      // Call the function
      const res = await redis.call('FCALL', 'setget', 1, k, 'fcall_value');
      assertEqual('fcall_value', res);

      // Verify via direct GET
      const val = await redis.get(k);
      assertEqual('fcall_value', val);
    } finally {
      await redis.del(k);
      try { await redis.call('FUNCTION', 'FLUSH'); } catch {}
    }
  }));

  // EVAL_KEYS_ARGV: Verify KEYS and ARGV arrays passed correctly
  results.push(await runTest('EVAL_KEYS_ARGV', cat, async () => {
    const k1 = key(prefix, 'lua:adv:k1');
    const k2 = key(prefix, 'lua:adv:k2');
    const k3 = key(prefix, 'lua:adv:k3');

    const script = `
      redis.call('SET', KEYS[1], ARGV[1])
      redis.call('SET', KEYS[2], ARGV[2])
      redis.call('SET', KEYS[3], ARGV[3])
      return {
        redis.call('GET', KEYS[1]),
        redis.call('GET', KEYS[2]),
        redis.call('GET', KEYS[3])
      }
    `;
    const res = await redis.eval(script, 3, k1, k2, k3, 'val1', 'val2', 'val3');
    assertTrue(Array.isArray(res), 'should return array');
    assertTrue(res.length === 3, 'should return 3 values');
    assertEqual('val1', res[0]);
    assertEqual('val2', res[1]);
    assertEqual('val3', res[2]);

    await redis.del(k1, k2, k3);
  }));

  // EVAL_error_handling: Verify error propagation from Lua to client
  results.push(await runTest('EVAL_error_handling', cat, async () => {
    // Syntax error in Lua
    let errored = false;
    try {
      await redis.eval('this is not valid lua', 0);
    } catch {
      errored = true;
    }
    assertTrue(errored, 'syntax error should propagate to client');

    // Runtime error via redis.error_reply
    errored = false;
    let errMsg = '';
    try {
      await redis.eval("return redis.error_reply('MY_CUSTOM_ERROR')", 0);
    } catch (err) {
      errored = true;
      errMsg = String(err);
    }
    assertTrue(errored, 'redis.error_reply should propagate as error');
    assertTrue(errMsg.includes('MY_CUSTOM_ERROR'), 'custom error should propagate');

    // Runtime error via redis.call with wrong args
    errored = false;
    try {
      await redis.eval("return redis.call('SET')", 0);
    } catch {
      errored = true;
    }
    assertTrue(errored, 'redis.call with wrong args should propagate error');

    // Successful redis.status_reply
    const res = await redis.eval("return redis.status_reply('PONG')", 0);
    assertEqual('PONG', res);
  }));

  return results;
});
