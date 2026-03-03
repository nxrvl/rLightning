"""Category: Replication tests."""

import os
import time

import redis as redis_lib
from .framework import (
    register, run_test, skip_test, key, assert_equal, assert_true,
)


def test_replication(r, prefix):
    results = []
    cat = "replication"

    replica_host = os.environ.get("REDIS_REPLICA_HOST", "")
    replica_port = int(os.environ.get("REDIS_REPLICA_PORT", "6379"))

    # If replica is not configured, skip all replication tests
    if not replica_host:
        reason = "REDIS_REPLICA_HOST not set"
        results.append(skip_test("INFO_replication", cat, reason))
        results.append(skip_test("REPLICAOF_basic", cat, reason))
        results.append(skip_test("REPLICATION_data_sync", cat, reason))
        results.append(skip_test("REPLICATION_multi_commands", cat, reason))
        results.append(skip_test("REPLICATION_expiry", cat, reason))
        results.append(skip_test("REPLICA_read_only", cat, reason))
        results.append(skip_test("REPLICATION_after_reconnect", cat, reason))
        results.append(skip_test("WAIT_command", cat, reason))
        results.append(skip_test("REPLICATION_SELECT", cat, reason))
        return results

    password = r.connection_pool.connection_kwargs.get("password")

    def create_replica_client(db=0):
        return redis_lib.Redis(
            host=replica_host,
            port=replica_port,
            password=password,
            db=db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
        )

    # Connect to replica and wait for it to be ready
    replica = create_replica_client(0)
    for _ in range(30):
        try:
            replica.ping()
            break
        except Exception:
            time.sleep(1)

    def wait_for_key(client, k, max_wait=5.0):
        """Wait for a key to appear on the replica."""
        deadline = time.time() + max_wait
        while time.time() < deadline:
            val = client.get(k)
            if val is not None:
                return val
            time.sleep(0.1)
        raise AssertionError(f"key {k} not found on replica after {max_wait}s")

    # INFO_replication: Verify INFO replication section on master
    def t_info_replication():
        info = r.info("replication")
        assert_equal("master", info.get("role", ""))
        count = info.get("connected_slaves", 0)
        assert_true(count >= 1, f"connected_slaves should be >= 1, got {count}")
    results.append(run_test("INFO_replication", cat, t_info_replication))

    # REPLICAOF_basic: Verify replica is connected to master
    def t_replicaof_basic():
        info = replica.info("replication")
        assert_equal("slave", info.get("role", ""))
        assert_equal("up", info.get("master_link_status", ""))
    results.append(run_test("REPLICAOF_basic", cat, t_replicaof_basic))

    # REPLICATION_data_sync: SET key on master, verify key appears on replica
    def t_replication_data_sync():
        k = key(prefix, "repl:data_sync")
        try:
            r.set(k, "replicated_value")
            val = wait_for_key(replica, k)
            assert_equal("replicated_value", val)
        finally:
            r.delete(k)
    results.append(run_test("REPLICATION_data_sync", cat, t_replication_data_sync))

    # REPLICATION_multi_commands: SET multiple keys on master, verify all replicated
    def t_replication_multi_commands():
        keys = []
        try:
            for i in range(10):
                k = key(prefix, f"repl:multi_{i}")
                keys.append(k)
                r.set(k, f"value_{i}")
            # Wait for last key to appear on replica
            wait_for_key(replica, keys[9])
            # Verify all keys
            for i in range(10):
                val = replica.get(keys[i])
                assert_equal(f"value_{i}", val)
        finally:
            if keys:
                r.delete(*keys)
    results.append(run_test("REPLICATION_multi_commands", cat, t_replication_multi_commands))

    # REPLICATION_expiry: SET key with TTL on master, verify TTL replicated
    def t_replication_expiry():
        k = key(prefix, "repl:expiry")
        try:
            r.set(k, "ttl_value", ex=120)
            wait_for_key(replica, k)
            ttl = replica.ttl(k)
            assert_true(ttl > 0, f"replica TTL should be > 0, got {ttl}")
        finally:
            r.delete(k)
    results.append(run_test("REPLICATION_expiry", cat, t_replication_expiry))

    # REPLICA_read_only: Verify writes to replica return READONLY error
    def t_replica_read_only():
        k = key(prefix, "repl:readonly_test")
        try:
            replica.set(k, "should_fail")
            replica.delete(k)
            raise AssertionError("expected READONLY error, but SET succeeded on replica")
        except redis_lib.ReadOnlyError:
            pass  # Expected
        except redis_lib.ResponseError as e:
            assert_true(
                "READONLY" in str(e) or "readonly" in str(e),
                f"expected READONLY error, got: {e}",
            )
    results.append(run_test("REPLICA_read_only", cat, t_replica_read_only))

    # REPLICATION_after_reconnect: Disconnect replica, write to master, reconnect
    def t_replication_after_reconnect():
        # Get master host/port from replica's INFO
        info = replica.info("replication")
        master_host = info.get("master_host", "")
        master_port = info.get("master_port", "")
        if not master_host or not master_port:
            raise AssertionError("could not determine master host/port from replica INFO")

        # Disconnect replica
        replica.execute_command("REPLICAOF", "NO", "ONE")

        k = key(prefix, "repl:reconnect_test")
        try:
            # Write data on master while replica is disconnected
            r.set(k, "after_disconnect")

            # Reconnect replica to master
            replica.execute_command("REPLICAOF", str(master_host), str(master_port))

            # Wait for sync and key to appear
            val = wait_for_key(replica, k, max_wait=10.0)
            assert_equal("after_disconnect", val)
        finally:
            r.delete(k)
    results.append(run_test("REPLICATION_after_reconnect", cat, t_replication_after_reconnect))

    # WAIT_command: SET key, WAIT 1 0, verify acknowledged
    def t_wait_command():
        k = key(prefix, "repl:wait_test")
        try:
            r.set(k, "wait_value")
            # WAIT for 1 replica with 5 second timeout
            num_replicas = r.execute_command("WAIT", 1, 5000)
            assert_true(
                int(num_replicas) >= 1,
                f"WAIT should return >= 1, got {num_replicas}",
            )
        finally:
            r.delete(k)
    results.append(run_test("WAIT_command", cat, t_wait_command))

    # REPLICATION_SELECT: Master uses SELECT 3 + SET, verify replica DB 3
    def t_replication_select():
        k = key(prefix, "repl:select_db3")

        # Create a client for master DB 3
        master_db3 = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=password,
            db=3,
            decode_responses=True,
        )
        replica_db3 = create_replica_client(3)
        try:
            master_db3.set(k, "db3_replicated")
            val = wait_for_key(replica_db3, k)
            assert_equal("db3_replicated", val)
            master_db3.delete(k)
        finally:
            replica_db3.close()
            master_db3.close()
    results.append(run_test("REPLICATION_SELECT", cat, t_replication_select))

    replica.close()
    return results


register("replication", test_replication)
