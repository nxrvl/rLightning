"""Test framework for redis-py compatibility testing."""

import time
import traceback

_registry = []


def register(category, fn):
    """Register a test function for a category."""
    _registry.append((category, fn))


def run_all(r, prefix):
    """Run all registered tests and return results."""
    results = []
    for _category, fn in _registry:
        results.extend(fn(r, prefix))
    return results


def run_test(name, category, fn):
    """Run a single test, catching exceptions."""
    start = time.time()
    result = {"name": name, "category": category, "status": "pass", "duration_ms": 0}
    try:
        fn()
    except SkipError as e:
        result["status"] = "skip"
        result["details"] = str(e)
    except Exception as e:
        result["status"] = "fail"
        result["error"] = str(e)
    result["duration_ms"] = round((time.time() - start) * 1000, 2)
    return result


def skip_test(name, category, reason):
    """Return a skipped test result."""
    return {"name": name, "category": category, "status": "skip", "duration_ms": 0, "details": reason}


def key(prefix, name):
    """Return a prefixed key for test isolation."""
    return prefix + name


class SkipError(Exception):
    pass


def assert_equal(expected, actual):
    if str(expected) != str(actual):
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def assert_strict_equal(expected, actual):
    if expected != actual:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def assert_true(val, msg):
    if not val:
        raise AssertionError(f"assertion failed: {msg}")


def assert_nil(val):
    if val is not None:
        raise AssertionError(f"expected None, got {val!r}")


def assert_err(val):
    """Assert that val is an exception or truthy error indicator."""
    if val is None:
        raise AssertionError("expected error, got None")


def assert_equal_int(expected, actual):
    e = int(expected)
    a = int(actual)
    if e != a:
        raise AssertionError(f"expected {e}, got {a}")


def assert_equal_float(expected, actual, tolerance):
    diff = abs(float(expected) - float(actual))
    if diff > tolerance:
        raise AssertionError(f"expected {expected}, got {actual} (tolerance {tolerance})")


def assert_list_equal(expected, actual):
    if len(expected) != len(actual):
        raise AssertionError(f"expected list len {len(expected)}, got {len(actual)}: expected {expected}, got {actual}")
    for i in range(len(expected)):
        if str(expected[i]) != str(actual[i]):
            raise AssertionError(f"at index {i}: expected {expected[i]!r}, got {actual[i]!r}")


def assert_list_equal_unordered(expected, actual):
    if len(expected) != len(actual):
        raise AssertionError(f"expected list len {len(expected)}, got {len(actual)}: expected {expected}, got {actual}")
    e = sorted(str(x) for x in expected)
    a = sorted(str(x) for x in actual)
    for i in range(len(e)):
        if e[i] != a[i]:
            raise AssertionError(f"sorted mismatch at {i}: expected {e[i]!r}, got {a[i]!r}")


class AssertionError(Exception):
    """Custom assertion error for test framework."""
    pass
