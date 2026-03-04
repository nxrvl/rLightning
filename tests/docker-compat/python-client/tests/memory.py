"""Category: Memory management & eviction tests."""

import time

from .framework import (
    register, run_test, key, assert_equal, assert_true,
)


def test_memory(r, prefix):
    results = []
    cat = "memory"

    # MEMORY_USAGE_basic: SET key, verify MEMORY USAGE returns reasonable byte count
    def t_memory_usage_basic():
        k = key(prefix, "mem:usage_basic")
        try:
            r.set(k, "hello world")
            usage = r.memory_usage(k)
            assert_true(usage is not None, "MEMORY USAGE returned None")
            assert_true(usage > 0, f"MEMORY USAGE should be > 0, got {usage}")
            assert_true(usage < 1024, f"MEMORY USAGE should be < 1024 bytes for small string, got {usage}")
        finally:
            r.delete(k)
    results.append(run_test("MEMORY_USAGE_basic", cat, t_memory_usage_basic))

    # MEMORY_USAGE_types: Test MEMORY USAGE for string, hash, list, set, zset
    def t_memory_usage_types():
        k_str = key(prefix, "mem:type_str")
        k_hash = key(prefix, "mem:type_hash")
        k_list = key(prefix, "mem:type_list")
        k_set = key(prefix, "mem:type_set")
        k_zset = key(prefix, "mem:type_zset")
        try:
            r.set(k_str, "value")
            r.hset(k_hash, mapping={"field1": "val1", "field2": "val2"})
            r.rpush(k_list, "a", "b", "c")
            r.sadd(k_set, "m1", "m2", "m3")
            r.zadd(k_zset, {"x": 1.0, "y": 2.0})

            types = {
                k_str: "string",
                k_hash: "hash",
                k_list: "list",
                k_set: "set",
                k_zset: "zset",
            }
            for k, type_name in types.items():
                usage = r.memory_usage(k)
                assert_true(usage is not None, f"MEMORY USAGE for {type_name} returned None")
                assert_true(usage > 0, f"MEMORY USAGE for {type_name} should be > 0, got {usage}")
        finally:
            r.delete(k_str, k_hash, k_list, k_set, k_zset)
    results.append(run_test("MEMORY_USAGE_types", cat, t_memory_usage_types))

    # MEMORY_DOCTOR: Run MEMORY DOCTOR, verify response format
    def t_memory_doctor():
        result = r.execute_command("MEMORY", "DOCTOR")
        assert_true(isinstance(result, str), f"MEMORY DOCTOR should return string, got {type(result)}")
        assert_true(len(result) > 0, "MEMORY DOCTOR response should not be empty")
    results.append(run_test("MEMORY_DOCTOR", cat, t_memory_doctor))

    # INFO_memory_section: Verify INFO memory section contains used_memory, used_memory_peak
    def t_info_memory_section():
        # Ensure some data exists so used_memory > 0 on all implementations
        k_info = key(prefix, "mem:info_data")
        try:
            r.set(k_info, "x" * 100)
            info = r.info("memory")
            assert_true("used_memory" in info, "INFO memory missing used_memory")
            assert_true("used_memory_peak" in info, "INFO memory missing used_memory_peak")
            used_memory = info["used_memory"]
            assert_true(used_memory > 0, f"used_memory should be > 0, got {used_memory}")
        finally:
            r.delete(k_info)
    results.append(run_test("INFO_memory_section", cat, t_info_memory_section))

    # CONFIG_maxmemory: CONFIG SET maxmemory, verify it takes effect
    def t_config_maxmemory():
        orig = r.config_get("maxmemory")
        orig_maxmem = orig.get("maxmemory", "0")
        try:
            r.config_set("maxmemory", "104857600")
            vals = r.config_get("maxmemory")
            assert_equal("104857600", str(vals.get("maxmemory", "")))
        finally:
            r.config_set("maxmemory", orig_maxmem)
    results.append(run_test("CONFIG_maxmemory", cat, t_config_maxmemory))

    # CONFIG_maxmemory_policy: Test maxmemory-policy configuration
    def t_config_maxmemory_policy():
        orig = r.config_get("maxmemory-policy")
        orig_policy = orig.get("maxmemory-policy", "noeviction")
        try:
            policies = ["noeviction", "allkeys-lru", "volatile-lru", "allkeys-random"]
            for policy in policies:
                r.config_set("maxmemory-policy", policy)
                vals = r.config_get("maxmemory-policy")
                assert_equal(policy, vals.get("maxmemory-policy", ""))
        finally:
            r.config_set("maxmemory-policy", orig_policy)
    results.append(run_test("CONFIG_maxmemory_policy", cat, t_config_maxmemory_policy))

    # OBJECT_ENCODING: Verify encoding changes based on data size
    def t_object_encoding():
        k_int = key(prefix, "mem:enc_int")
        k_str = key(prefix, "mem:enc_str")
        k_embstr = key(prefix, "mem:enc_embstr")
        k_list = key(prefix, "mem:enc_list")
        k_set = key(prefix, "mem:enc_set")
        k_hash = key(prefix, "mem:enc_hash")
        k_zset = key(prefix, "mem:enc_zset")
        try:
            # Integer encoding
            r.set(k_int, "12345")
            enc = r.object("encoding", k_int)
            assert_equal("int", enc)

            # Embstr encoding for short strings
            r.set(k_embstr, "hello")
            enc = r.object("encoding", k_embstr)
            assert_equal("embstr", enc)

            # Raw encoding for long strings
            r.set(k_str, "x" * 100)
            enc = r.object("encoding", k_str)
            assert_equal("raw", enc)

            # Small list -> listpack
            r.rpush(k_list, "a", "b", "c")
            enc = r.object("encoding", k_list)
            assert_true(enc in ("listpack", "ziplist"),
                        f"small list should be listpack or ziplist, got {enc}")

            # Small set -> listpack
            r.sadd(k_set, "m1", "m2", "m3")
            enc = r.object("encoding", k_set)
            assert_true(enc in ("listpack", "ziplist"),
                        f"small set should be listpack or ziplist, got {enc}")

            # Small hash -> listpack
            r.hset(k_hash, mapping={"f1": "v1", "f2": "v2"})
            enc = r.object("encoding", k_hash)
            assert_true(enc in ("listpack", "ziplist"),
                        f"small hash should be listpack or ziplist, got {enc}")

            # Small zset -> listpack
            r.zadd(k_zset, {"a": 1.0, "b": 2.0})
            enc = r.object("encoding", k_zset)
            assert_true(enc in ("listpack", "ziplist"),
                        f"small zset should be listpack or ziplist, got {enc}")
        finally:
            r.delete(k_int, k_str, k_embstr, k_list, k_set, k_hash, k_zset)
    results.append(run_test("OBJECT_ENCODING", cat, t_object_encoding))

    # OBJECT_REFCOUNT: Verify OBJECT REFCOUNT returns integer
    def t_object_refcount():
        k = key(prefix, "mem:refcount")
        try:
            r.set(k, "testvalue")
            refcount = r.object("refcount", k)
            assert_true(isinstance(refcount, int), f"OBJECT REFCOUNT should return int, got {type(refcount)}")
            assert_true(refcount >= 1, f"OBJECT REFCOUNT should be >= 1, got {refcount}")
        finally:
            r.delete(k)
    results.append(run_test("OBJECT_REFCOUNT", cat, t_object_refcount))

    # OBJECT_IDLETIME: SET key, verify OBJECT IDLETIME reflects elapsed time
    def t_object_idletime():
        k = key(prefix, "mem:idletime")
        try:
            r.set(k, "testvalue")
            idletime = r.object("idletime", k)
            assert_true(isinstance(idletime, int), f"OBJECT IDLETIME should return int, got {type(idletime)}")
            assert_equal_int(0, idletime)
        finally:
            r.delete(k)
    results.append(run_test("OBJECT_IDLETIME", cat, t_object_idletime))

    # OBJECT_FREQ: Verify OBJECT FREQ returns LFU frequency counter
    def t_object_freq():
        k = key(prefix, "mem:freq")
        orig = r.config_get("maxmemory-policy")
        orig_policy = orig.get("maxmemory-policy", "noeviction")
        try:
            r.config_set("maxmemory-policy", "allkeys-lfu")
            r.set(k, "testvalue")
            # Access the key a few times
            for _ in range(5):
                r.get(k)
            time.sleep(0.05)

            freq = r.object("freq", k)
            assert_true(isinstance(freq, int), f"OBJECT FREQ should return int, got {type(freq)}")
            assert_true(freq > 0, f"OBJECT FREQ should be > 0 after access, got {freq}")
        finally:
            r.config_set("maxmemory-policy", orig_policy)
            r.delete(k)
    results.append(run_test("OBJECT_FREQ", cat, t_object_freq))

    return results


register("memory", test_memory)
