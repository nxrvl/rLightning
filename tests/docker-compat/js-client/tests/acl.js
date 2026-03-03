const Redis = require('ioredis');
const { register, runTest, key, assertEqual, assertTrue, sleep } = require('./framework');

register('acl', async (redis, prefix) => {
  const results = [];
  const cat = 'acl';

  // Helper to clean up test users
  const cleanupUsers = async (...users) => {
    for (const u of users) {
      try { await redis.acl('DELUSER', u); } catch {}
    }
  };

  // Helper to create a user connection
  const createUserClient = (user, password) => {
    return new Redis({
      host: redis.options.host,
      port: redis.options.port,
      username: user,
      password: password,
      lazyConnect: true,
      retryStrategy: () => null,
      maxRetriesPerRequest: 1,
    });
  };

  // ACL_SETUSER_basic: Create user with specific permissions, verify access
  results.push(await runTest('ACL_SETUSER_basic', cat, async () => {
    const user = 'acl_test_basic';
    await cleanupUsers(user);

    try {
      await redis.acl('SETUSER', user, 'on', '>testpass123', '~*', '&*', '+@all');

      const userRedis = createUserClient(user, 'testpass123');
      await userRedis.connect();

      try {
        const k = key(prefix, 'acl:basic_test');
        await userRedis.set(k, 'hello_acl');
        const got = await userRedis.get(k);
        assertEqual('hello_acl', got);
        await redis.del(k);
      } finally {
        userRedis.disconnect();
      }
    } finally {
      await cleanupUsers(user);
    }
  }));

  // ACL_SETUSER_command_restrictions: Restricted commands, verify NOPERM
  results.push(await runTest('ACL_SETUSER_command_restrictions', cat, async () => {
    const user = 'acl_test_cmdrestr';
    await cleanupUsers(user);

    try {
      await redis.acl('SETUSER', user, 'on', '>testpass123', '~*', '&*', '+@all', '-del');

      const userRedis = createUserClient(user, 'testpass123');
      await userRedis.connect();

      try {
        const k = key(prefix, 'acl:cmdrestr_test');

        // SET should work
        await userRedis.set(k, 'value');

        // GET should work
        const got = await userRedis.get(k);
        assertEqual('value', got);

        // DEL should be denied (NOPERM)
        let denied = false;
        try {
          await userRedis.del(k);
        } catch (err) {
          if (err.message.includes('NOPERM')) {
            denied = true;
          } else {
            throw err;
          }
        }
        assertTrue(denied, 'DEL should return NOPERM for restricted user');

        await redis.del(k);
      } finally {
        userRedis.disconnect();
      }
    } finally {
      await cleanupUsers(user);
    }
  }));

  // ACL_SETUSER_key_patterns: Key pattern restrictions (~key:*), verify access control
  results.push(await runTest('ACL_SETUSER_key_patterns', cat, async () => {
    const user = 'acl_test_keypattern';
    await cleanupUsers(user);

    try {
      const allowedPattern = `~${prefix}acl:allowed:*`;
      await redis.acl('SETUSER', user, 'on', '>testpass123', allowedPattern, '&*', '+@all');

      const userRedis = createUserClient(user, 'testpass123');
      await userRedis.connect();

      try {
        const allowedKey = key(prefix, 'acl:allowed:testkey');
        const deniedKey = key(prefix, 'acl:denied:testkey');

        // SET on allowed key should work
        await userRedis.set(allowedKey, 'allowed_val');
        const got = await userRedis.get(allowedKey);
        assertEqual('allowed_val', got);

        // SET on denied key should fail
        let denied = false;
        try {
          await userRedis.set(deniedKey, 'denied_val');
        } catch (err) {
          if (err.message.includes('NOPERM')) {
            denied = true;
          } else {
            throw err;
          }
        }
        assertTrue(denied, 'SET on denied key should return NOPERM');

        await redis.del(allowedKey, deniedKey);
      } finally {
        userRedis.disconnect();
      }
    } finally {
      await cleanupUsers(user);
    }
  }));

  // ACL_DELUSER: Create user, delete, verify deleted user cannot AUTH
  results.push(await runTest('ACL_DELUSER', cat, async () => {
    const user = 'acl_test_delme';
    await cleanupUsers(user);

    // Create user
    await redis.acl('SETUSER', user, 'on', '>testpass123', '~*', '&*', '+@all');

    // Verify user works before deletion
    const userRedis1 = createUserClient(user, 'testpass123');
    await userRedis1.connect();
    await userRedis1.ping();
    userRedis1.disconnect();

    // Delete user
    await redis.acl('DELUSER', user);

    // Try to connect as deleted user - should fail
    const userRedis2 = createUserClient(user, 'testpass123');
    let authFailed = false;
    try {
      await userRedis2.connect();
      await userRedis2.ping();
    } catch (err) {
      authFailed = true;
    }
    try { userRedis2.disconnect(); } catch {}
    assertTrue(authFailed, 'AUTH should fail for deleted user');
  }));

  // ACL_LIST: Create multiple users, verify ACL LIST returns all
  results.push(await runTest('ACL_LIST', cat, async () => {
    const user1 = 'acl_test_list1';
    const user2 = 'acl_test_list2';
    await cleanupUsers(user1, user2);

    try {
      await redis.acl('SETUSER', user1, 'on', '>pass1', '~*', '&*', '+@all');
      await redis.acl('SETUSER', user2, 'on', '>pass2', '~*', '&*', '+@all');

      const list = await redis.acl('LIST');
      assertTrue(Array.isArray(list), 'ACL LIST should return an array');

      const listStr = list.join('\n');
      assertTrue(listStr.includes(user1), `ACL LIST should contain ${user1}`);
      assertTrue(listStr.includes(user2), `ACL LIST should contain ${user2}`);
    } finally {
      await cleanupUsers(user1, user2);
    }
  }));

  // ACL_WHOAMI: AUTH as different users, verify ACL WHOAMI returns correct username
  results.push(await runTest('ACL_WHOAMI', cat, async () => {
    const user = 'acl_test_whoami';
    await cleanupUsers(user);

    try {
      await redis.acl('SETUSER', user, 'on', '>testpass123', '~*', '&*', '+@all');

      const userRedis = createUserClient(user, 'testpass123');
      await userRedis.connect();

      try {
        const whoami = await userRedis.acl('WHOAMI');
        assertEqual(user, whoami);
      } finally {
        userRedis.disconnect();
      }
    } finally {
      await cleanupUsers(user);
    }
  }));

  // ACL_LOG: Execute denied command, verify ACL LOG records the denial
  results.push(await runTest('ACL_LOG', cat, async () => {
    const user = 'acl_test_log';
    await cleanupUsers(user);

    try {
      // Reset ACL LOG
      await redis.acl('LOG', 'RESET');

      await redis.acl('SETUSER', user, 'on', '>testpass123', '~*', '&*', '+@all', '-del');

      const userRedis = createUserClient(user, 'testpass123');
      await userRedis.connect();

      try {
        const k = key(prefix, 'acl:log_test');
        // Try DEL (should be denied, ignore error)
        try { await userRedis.del(k); } catch {}
      } finally {
        userRedis.disconnect();
      }

      // Check ACL LOG on admin connection
      const log = await redis.acl('LOG');
      assertTrue(Array.isArray(log) && log.length > 0, 'ACL LOG should have entries after denied command');
    } finally {
      await cleanupUsers(user);
    }
  }));

  // ACL_CAT: Verify ACL CAT returns command categories
  results.push(await runTest('ACL_CAT', cat, async () => {
    const categories = await redis.acl('CAT');
    assertTrue(Array.isArray(categories), 'ACL CAT should return an array');
    assertTrue(categories.length > 0, 'ACL CAT should return categories');

    const catLower = categories.map(c => String(c).toLowerCase());
    for (const expected of ['string', 'hash', 'list', 'set', 'sortedset']) {
      assertTrue(catLower.includes(expected), `ACL CAT should contain '${expected}'`);
    }
  }));

  // AUTH_username_password: Test named user authentication (AUTH username password)
  results.push(await runTest('AUTH_username_password', cat, async () => {
    const user = 'acl_test_auth';
    await cleanupUsers(user);

    try {
      await redis.acl('SETUSER', user, 'on', '>authpass456', '~*', '&*', '+@all');

      const userRedis = createUserClient(user, 'authpass456');
      await userRedis.connect();

      try {
        const pong = await userRedis.ping();
        assertEqual('PONG', pong);

        const whoami = await userRedis.acl('WHOAMI');
        assertEqual(user, whoami);
      } finally {
        userRedis.disconnect();
      }
    } finally {
      await cleanupUsers(user);
    }
  }));

  // AUTH_default_user: Test default user password authentication
  results.push(await runTest('AUTH_default_user', cat, async () => {
    const whoami = await redis.acl('WHOAMI');
    assertEqual('default', whoami);
  }));

  return results;
});
