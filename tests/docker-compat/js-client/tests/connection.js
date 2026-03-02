const Redis = require('ioredis');
const { register, runTest, key, assertEqual, assertTrue } = require('./framework');

register('connection', async (redis, prefix) => {
  const results = [];

  results.push(await runTest('PING_PONG', 'connection', async () => {
    const res = await redis.ping();
    assertEqual('PONG', res);
  }));

  results.push(await runTest('ECHO', 'connection', async () => {
    const msg = 'hello rLightning';
    const res = await redis.echo(msg);
    assertEqual(msg, res);
  }));

  results.push(await runTest('SELECT_database', 'connection', async () => {
    const k = key(prefix, 'select_test');
    const opts = redis.options;
    const rdb0 = new Redis({ host: opts.host, port: opts.port, password: opts.password, db: 0, lazyConnect: true });
    const rdb1 = new Redis({ host: opts.host, port: opts.port, password: opts.password, db: 1, lazyConnect: true });
    await rdb0.connect();
    await rdb1.connect();
    try {
      await rdb0.del(k);
      await rdb1.del(k);
      await rdb1.set(k, 'db1value');
      const val = await rdb1.get(k);
      assertEqual('db1value', val);
      const val0 = await rdb0.get(k);
      assertTrue(val0 === null, 'key should not exist in DB 0');
    } finally {
      await rdb0.del(k);
      await rdb1.del(k);
      rdb0.disconnect();
      rdb1.disconnect();
    }
  }));

  results.push(await runTest('CLIENT_SETNAME_GETNAME', 'connection', async () => {
    const name = 'js-compat-test';
    await redis.call('CLIENT', 'SETNAME', name);
    const got = await redis.call('CLIENT', 'GETNAME');
    assertEqual(name, got);
  }));

  results.push(await runTest('CLIENT_ID', 'connection', async () => {
    const id = await redis.call('CLIENT', 'ID');
    assertTrue(Number(id) > 0, 'client ID should be positive');
  }));

  results.push(await runTest('DBSIZE', 'connection', async () => {
    const k = key(prefix, 'dbsize_test');
    await redis.set(k, 'val');
    const size = await redis.dbsize();
    assertTrue(Number(size) > 0, 'DBSIZE should be > 0');
    await redis.del(k);
  }));

  results.push(await runTest('INFO_server', 'connection', async () => {
    const info = await redis.info('server');
    assertTrue(info.length > 0, 'INFO should return data');
    assertTrue(info.includes('redis_version') || info.includes('server'), 'INFO server should contain version info');
  }));

  results.push(await runTest('AUTH_already_authenticated', 'connection', async () => {
    const password = redis.options.password;
    if (password) {
      await redis.call('AUTH', password);
    }
  }));

  return results;
});
