const Redis = require('ioredis');
const { register, runTest, key, assertEqual, assertTrue, sleep } = require('./framework');

register('pubsub', async (redis, prefix) => {
  const results = [];
  const cat = 'pubsub';

  results.push(await runTest('SUBSCRIBE_PUBLISH_UNSUBSCRIBE', cat, async () => {
    const ch = key(prefix, 'ps:chan1');
    const sub = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await sub.connect();

    const pub = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await pub.connect();

    try {
      const messagePromise = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout waiting for message')), 5000);
        sub.on('message', (channel, message) => {
          clearTimeout(timeout);
          resolve({ channel, message });
        });
      });

      await sub.subscribe(ch);
      await sleep(100);
      const n = await pub.publish(ch, 'hello');
      assertTrue(n >= 1, 'at least 1 subscriber should receive');

      const msg = await messagePromise;
      assertEqual('hello', msg.message);
      assertEqual(ch, msg.channel);
    } finally {
      sub.disconnect();
      pub.disconnect();
    }
  }));

  results.push(await runTest('PSUBSCRIBE_pattern', cat, async () => {
    const pattern = key(prefix, 'ps:pchan*');
    const sub = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await sub.connect();

    const pub = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await pub.connect();

    try {
      const messagePromise = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout waiting for pmessage')), 5000);
        sub.on('pmessage', (pat, channel, message) => {
          clearTimeout(timeout);
          resolve({ pattern: pat, channel, message });
        });
      });

      await sub.psubscribe(pattern);
      await sleep(100);
      await pub.publish(key(prefix, 'ps:pchan_test'), 'pattern_msg');

      const msg = await messagePromise;
      assertEqual('pattern_msg', msg.message);
      assertEqual(pattern, msg.pattern);
    } finally {
      sub.disconnect();
      pub.disconnect();
    }
  }));

  results.push(await runTest('multiple_channel_subscription', cat, async () => {
    const ch1 = key(prefix, 'ps:multi1');
    const ch2 = key(prefix, 'ps:multi2');
    const sub = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await sub.connect();

    const pub = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await pub.connect();

    try {
      const messages = [];
      const messagePromise = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout waiting for messages')), 5000);
        sub.on('message', (channel, message) => {
          messages.push({ channel, message });
          if (messages.length === 2) {
            clearTimeout(timeout);
            resolve(messages);
          }
        });
      });

      await sub.subscribe(ch1, ch2);
      await sleep(100);
      await pub.publish(ch1, 'msg1');
      await pub.publish(ch2, 'msg2');

      const msgs = await messagePromise;
      assertEqual('msg1', msgs[0].message);
      assertEqual('msg2', msgs[1].message);
    } finally {
      sub.disconnect();
      pub.disconnect();
    }
  }));

  results.push(await runTest('message_ordering', cat, async () => {
    const ch = key(prefix, 'ps:order');
    const sub = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await sub.connect();

    const pub = new Redis({
      host: redis.options.host,
      port: redis.options.port,
      password: redis.options.password,
      lazyConnect: true,
    });
    await pub.connect();

    try {
      const messages = [];
      const messagePromise = new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('timeout waiting for 5 messages')), 5000);
        sub.on('message', (channel, message) => {
          messages.push(message);
          if (messages.length === 5) {
            clearTimeout(timeout);
            resolve(messages);
          }
        });
      });

      await sub.subscribe(ch);
      await sleep(100);
      for (let i = 0; i < 5; i++) {
        await pub.publish(ch, `msg_${i}`);
      }

      const msgs = await messagePromise;
      for (let i = 0; i < 5; i++) {
        assertEqual(`msg_${i}`, msgs[i]);
      }
    } finally {
      sub.disconnect();
      pub.disconnect();
    }
  }));

  return results;
});
