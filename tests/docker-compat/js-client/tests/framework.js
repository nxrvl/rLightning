// Test framework for ioredis compatibility testing.
// Mirrors the Go test framework patterns.

const registry = [];

function register(category, fn) {
  registry.push({ category, fn });
}

async function runAll(redis, prefix) {
  const results = [];
  for (const { fn } of registry) {
    const categoryResults = await fn(redis, prefix);
    results.push(...categoryResults);
  }
  return results;
}

async function runTest(name, category, fn) {
  const start = Date.now();
  const result = { name, category, status: 'pass', duration_ms: 0 };
  try {
    await fn();
  } catch (err) {
    if (err && err._skip) {
      result.status = 'skip';
      result.details = err.message;
    } else if (err && err._error) {
      result.status = 'error';
      result.error = err.message;
    } else {
      result.status = 'fail';
      result.error = err ? err.message || String(err) : 'unknown error';
    }
  }
  result.duration_ms = Date.now() - start;
  return result;
}

function skipTest(name, category, reason) {
  return { name, category, status: 'skip', duration_ms: 0, details: reason };
}

function key(prefix, name) {
  return prefix + name;
}

function assertEqual(expected, actual) {
  const e = String(expected);
  const a = String(actual);
  if (e !== a) {
    throw new Error(`expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function assertStrictEqual(expected, actual) {
  if (expected !== actual) {
    throw new Error(`expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

function assertTrue(val, msg) {
  if (!val) {
    throw new Error(`assertion failed: ${msg}`);
  }
}

function assertNil(val) {
  if (val !== null && val !== undefined) {
    throw new Error(`expected null/undefined, got ${JSON.stringify(val)}`);
  }
}

function assertErr(val) {
  if (val === null || val === undefined) {
    throw new Error('expected error, got null/undefined');
  }
}

function assertEqualInt(expected, actual) {
  const e = Number(expected);
  const a = Number(actual);
  if (e !== a) {
    throw new Error(`expected ${e}, got ${a}`);
  }
}

function assertEqualFloat(expected, actual, tolerance) {
  const diff = Math.abs(expected - actual);
  if (diff > tolerance) {
    throw new Error(`expected ${expected}, got ${actual} (tolerance ${tolerance})`);
  }
}

function assertArrayEqual(expected, actual) {
  if (expected.length !== actual.length) {
    throw new Error(`expected array length ${expected.length}, got ${actual.length}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
  for (let i = 0; i < expected.length; i++) {
    if (String(expected[i]) !== String(actual[i])) {
      throw new Error(`at index ${i}: expected ${JSON.stringify(expected[i])}, got ${JSON.stringify(actual[i])}`);
    }
  }
}

function assertArrayEqualUnordered(expected, actual) {
  if (expected.length !== actual.length) {
    throw new Error(`expected array length ${expected.length}, got ${actual.length}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
  const e = [...expected].map(String).sort();
  const a = [...actual].map(String).sort();
  for (let i = 0; i < e.length; i++) {
    if (e[i] !== a[i]) {
      throw new Error(`sorted mismatch at ${i}: expected ${JSON.stringify(e[i])}, got ${JSON.stringify(a[i])}`);
    }
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = {
  register,
  runAll,
  runTest,
  skipTest,
  key,
  assertEqual,
  assertStrictEqual,
  assertTrue,
  assertNil,
  assertErr,
  assertEqualInt,
  assertEqualFloat,
  assertArrayEqual,
  assertArrayEqualUnordered,
  sleep,
};
