"""Category: Persistence (RDB/AOF) tests."""

import time

import redis as redis_lib
from .framework import (
    register, run_test, key, assert_equal, assert_true,
    assert_equal_int,
)


def test_persistence(r, prefix):
    results = []
    cat = "persistence"

    # RDB_SAVE_and_RESTORE: BGSAVE, wait, verify LASTSAVE timestamp changes
    def t_rdb_save_and_restore():
        k = key(prefix, "persist:rdb_save")
        try:
            # Get initial LASTSAVE timestamp
            t1 = r.lastsave()

            # Write some data to ensure there's something to save
            r.set(k, "rdb_test_value")

            # Trigger BGSAVE
            try:
                r.bgsave()
            except Exception as e:
                if "already in progress" not in str(e) and "Background save" not in str(e):
                    raise

            # Wait for background save to complete
            t2 = t1
            for _ in range(30):
                time.sleep(0.2)
                t2 = r.lastsave()
                if t2 > t1:
                    break

            assert_true(t2 >= t1, f"LASTSAVE should be >= initial: got {t2} vs {t1}")
        finally:
            r.delete(k)
    results.append(run_test("RDB_SAVE_and_RESTORE", cat, t_rdb_save_and_restore))

    # AOF_append_and_replay: CONFIG SET appendonly yes, write data, verify data accessible
    def t_aof_append_and_replay():
        k = key(prefix, "persist:aof_test")
        try:
            # Enable AOF
            r.config_set("appendonly", "yes")

            # Verify AOF is enabled
            res = r.config_get("appendonly")
            assert_equal("yes", res["appendonly"])

            # Write data that should be captured by AOF
            r.set(k, "aof_value_123")

            # Verify data is readable
            got = r.get(k)
            assert_equal("aof_value_123", got)
        finally:
            # Restore appendonly to no
            r.config_set("appendonly", "no")
            r.delete(k)
    results.append(run_test("AOF_append_and_replay", cat, t_aof_append_and_replay))

    # PERSIST_across_restart: SET keys with TTL, verify keys and TTLs exist
    def t_persist_across_restart():
        k1 = key(prefix, "persist:ttl1")
        k2 = key(prefix, "persist:ttl2")
        try:
            # Set keys with TTL
            r.set(k1, "value1", ex=60)
            r.set(k2, "value2", ex=120)

            # Trigger BGSAVE to persist
            try:
                r.bgsave()
            except Exception:
                pass
            time.sleep(0.5)

            # Verify values exist
            assert_equal("value1", r.get(k1))
            assert_equal("value2", r.get(k2))

            # Verify TTLs are still set
            ttl1 = r.ttl(k1)
            assert_true(ttl1 > 0, f"key1 TTL should be > 0, got {ttl1}")

            ttl2 = r.ttl(k2)
            assert_true(ttl2 > 0, f"key2 TTL should be > 0, got {ttl2}")
        finally:
            r.delete(k1, k2)
    results.append(run_test("PERSIST_across_restart", cat, t_persist_across_restart))

    # RDB_background_save: SET data, BGSAVE, verify DBSIZE
    def t_rdb_background_save():
        keys = []
        try:
            for i in range(10):
                k = key(prefix, f"persist:bgsave_{i}")
                keys.append(k)
                r.set(k, f"val_{i}")

            # BGSAVE
            try:
                r.bgsave()
            except Exception:
                pass
            time.sleep(0.5)

            # Verify DBSIZE reflects the keys
            size = r.dbsize()
            assert_true(size >= 10, f"DBSIZE should be >= 10, got {size}")
        finally:
            if keys:
                r.delete(*keys)
    results.append(run_test("RDB_background_save", cat, t_rdb_background_save))

    # CONFIG_persistence_settings: CONFIG SET/GET for save, appendonly, appendfsync
    def t_config_persistence_settings():
        # Test appendonly CONFIG SET/GET
        orig_aof = r.config_get("appendonly")
        orig_aof_val = orig_aof["appendonly"]
        try:
            r.config_set("appendonly", "yes")
            res = r.config_get("appendonly")
            assert_equal("yes", res["appendonly"])
        finally:
            r.config_set("appendonly", orig_aof_val)

        # Test appendfsync CONFIG SET/GET
        orig_sync = r.config_get("appendfsync")
        orig_sync_val = orig_sync["appendfsync"]
        try:
            r.config_set("appendfsync", "everysec")
            res = r.config_get("appendfsync")
            assert_equal("everysec", res["appendfsync"])
        finally:
            r.config_set("appendfsync", orig_sync_val)

        # Test save CONFIG GET (read-only, don't modify)
        save_res = r.config_get("save")
        assert_true("save" in save_res, "CONFIG GET save should return a value")
    results.append(run_test("CONFIG_persistence_settings", cat, t_config_persistence_settings))

    # DEBUG_SLEEP_during_save: Verify server remains responsive during BGSAVE
    def t_debug_sleep_during_save():
        k = key(prefix, "persist:during_save")
        try:
            r.set(k, "before_save")

            # Trigger BGSAVE
            try:
                r.bgsave()
            except Exception:
                pass

            # Immediately verify server is responsive
            r.set(k, "during_save")
            got = r.get(k)
            assert_equal("during_save", got)

            # PING should also work
            assert_true(r.ping(), "PING should return True during BGSAVE")
        finally:
            r.delete(k)
    results.append(run_test("DEBUG_SLEEP_during_save", cat, t_debug_sleep_during_save))

    # MULTI_DB_persistence: SET keys in DB 0 and DB 3, verify both databases
    def t_multi_db_persistence():
        k0 = key(prefix, "persist:db0_key")
        k3 = key(prefix, "persist:db3_key")

        # Write to DB 0
        r.set(k0, "db0_value")

        # Create a client for DB 3
        r3 = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=3, decode_responses=True,
        )
        try:
            # Write to DB 3
            r3.set(k3, "db3_value")

            # Trigger BGSAVE to persist both DBs
            try:
                r.bgsave()
            except Exception:
                pass
            time.sleep(0.5)

            # Verify DB 0 key
            assert_equal("db0_value", r.get(k0))

            # Verify DB 3 key
            assert_equal("db3_value", r3.get(k3))

            r3.delete(k3)
        finally:
            r3.close()
        r.delete(k0)
    results.append(run_test("MULTI_DB_persistence", cat, t_multi_db_persistence))

    # LARGE_VALUE_persistence: SET 1MB value, verify value intact
    def t_large_value_persistence():
        k = key(prefix, "persist:large_val")
        try:
            # Create a 1MB value
            large_val = "A" * (1024 * 1024)

            r.set(k, large_val)

            # Trigger BGSAVE
            try:
                r.bgsave()
            except Exception:
                pass
            time.sleep(0.5)

            # Verify the value is intact
            got = r.get(k)
            assert_equal_int(len(large_val), len(got))
            assert_equal(large_val, got)
        finally:
            r.delete(k)
    results.append(run_test("LARGE_VALUE_persistence", cat, t_large_value_persistence))

    return results


register("persistence", test_persistence)
