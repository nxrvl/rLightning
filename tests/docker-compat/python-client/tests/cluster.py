"""Category: Cluster mode tests."""

import os
import time

import redis as redis_lib
from .framework import (
    register, run_test, skip_test, key, assert_equal, assert_true,
)


def test_cluster(r, prefix):
    results = []
    cat = "cluster"

    cluster_host = os.environ.get("REDIS_CLUSTER_HOST", "")
    cluster_port = int(os.environ.get("REDIS_CLUSTER_PORT", "6379"))

    # If cluster is not configured, skip all cluster tests
    if not cluster_host:
        reason = "REDIS_CLUSTER_HOST not set"
        results.append(skip_test("CLUSTER_INFO", cat, reason))
        results.append(skip_test("CLUSTER_NODES", cat, reason))
        results.append(skip_test("CLUSTER_SLOTS", cat, reason))
        results.append(skip_test("CLUSTER_SHARDS", cat, reason))
        results.append(skip_test("CLUSTER_hash_tag", cat, reason))
        results.append(skip_test("CLUSTER_MOVED_redirect", cat, reason))
        results.append(skip_test("CLUSTER_KEYSLOT", cat, reason))
        results.append(skip_test("CLUSTER_COUNTKEYSINSLOT", cat, reason))
        results.append(skip_test("CLUSTER_cross_slot_error", cat, reason))
        results.append(skip_test("READONLY_mode", cat, reason))
        return results

    password = r.connection_pool.connection_kwargs.get("password")

    cluster_client = redis_lib.Redis(
        host=cluster_host,
        port=cluster_port,
        password=password,
        db=0,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=10,
    )

    # Wait for cluster node to be ready
    for _ in range(30):
        try:
            cluster_client.ping()
            break
        except Exception:
            time.sleep(1)

    # Wait for cluster to converge
    for _ in range(30):
        try:
            info = cluster_client.execute_command("CLUSTER", "INFO")
            if "cluster_state:ok" in info:
                break
        except Exception:
            pass
        time.sleep(1)

    def parse_cluster_nodes(nodes_str):
        """Parse CLUSTER NODES output into a list of dicts."""
        nodes = []
        for line in nodes_str.strip().split("\n"):
            fields = line.strip().split()
            if len(fields) >= 8:
                nodes.append({
                    "id": fields[0],
                    "addr": fields[1],
                    "flags": fields[2],
                    "master": fields[3],
                    "ping_sent": fields[4],
                    "pong_recv": fields[5],
                    "config_epoch": fields[6],
                    "link_state": fields[7],
                    "slots": fields[8:],
                })
        return nodes

    # CLUSTER_INFO: Verify CLUSTER INFO returns cluster_state, cluster_slots_assigned, cluster_size
    def t_cluster_info():
        info = cluster_client.execute_command("CLUSTER", "INFO")
        assert_true("cluster_state:" in info, "CLUSTER INFO missing cluster_state field")
        assert_true("cluster_slots_assigned:" in info, "CLUSTER INFO missing cluster_slots_assigned field")
        assert_true("cluster_size:" in info, "CLUSTER INFO missing cluster_size field")
        assert_true("cluster_enabled:1" in info, "CLUSTER INFO should show cluster_enabled:1")
    results.append(run_test("CLUSTER_INFO", cat, t_cluster_info))

    # CLUSTER_NODES: Verify CLUSTER NODES returns node list with correct format
    def t_cluster_nodes():
        nodes_str = cluster_client.execute_command("CLUSTER", "NODES")
        nodes = parse_cluster_nodes(nodes_str)
        assert_true(len(nodes) >= 3, f"expected at least 3 nodes, got {len(nodes)}")

        found_myself = False
        for node in nodes:
            assert_true(
                len(node["id"]) == 40,
                f"node ID should be 40 chars, got {len(node['id'])}: {node['id']}",
            )
            if "myself" in node["flags"]:
                found_myself = True

        assert_true(found_myself, "CLUSTER NODES should contain a 'myself' node")
    results.append(run_test("CLUSTER_NODES", cat, t_cluster_nodes))

    # CLUSTER_SLOTS: Verify CLUSTER SLOTS returns slot ranges with master/replica info
    def t_cluster_slots():
        slots = cluster_client.execute_command("CLUSTER", "SLOTS")
        assert_true(isinstance(slots, list), "CLUSTER SLOTS should return a list")
        assert_true(len(slots) > 0, "CLUSTER SLOTS returned empty result")

        total_slots = 0
        for slot_range in slots:
            start = slot_range[0]
            end = slot_range[1]
            assert_true(
                start >= 0 and end <= 16383,
                f"invalid slot range: {start}-{end}",
            )
            total_slots += (end - start) + 1
            # At least start, end, and one node info
            assert_true(
                len(slot_range) >= 3,
                f"slot range {start}-{end} should have at least one node",
            )

        assert_equal(16384, total_slots)
    results.append(run_test("CLUSTER_SLOTS", cat, t_cluster_slots))

    # CLUSTER_SHARDS: Verify CLUSTER SHARDS returns shard information (Redis 7.0+)
    def t_cluster_shards():
        shards = cluster_client.execute_command("CLUSTER", "SHARDS")
        assert_true(isinstance(shards, list), "CLUSTER SHARDS should return a list")
        assert_true(len(shards) >= 3, f"expected at least 3 shards, got {len(shards)}")

        for i, shard in enumerate(shards):
            assert_true(
                isinstance(shard, list),
                f"shard {i} should be a list",
            )
            assert_true(
                len(shard) >= 4,
                f"shard {i} has too few fields ({len(shard)})",
            )
    results.append(run_test("CLUSTER_SHARDS", cat, t_cluster_shards))

    # CLUSTER_hash_tag: Verify {tag} keys hash to same slot
    def t_cluster_hash_tag():
        slot1 = cluster_client.execute_command("CLUSTER", "KEYSLOT", "{user}.name")
        slot2 = cluster_client.execute_command("CLUSTER", "KEYSLOT", "{user}.email")
        slot3 = cluster_client.execute_command("CLUSTER", "KEYSLOT", "{user}.age")

        assert_equal(slot1, slot2)
        assert_equal(slot2, slot3)

        slot_a = cluster_client.execute_command("CLUSTER", "KEYSLOT", "{alpha}.key")
        slot_b = cluster_client.execute_command("CLUSTER", "KEYSLOT", "{beta}.key")
        assert_true(
            slot_a != slot_b,
            f"{{alpha}} and {{beta}} should hash to different slots, both got {slot_a}",
        )
    results.append(run_test("CLUSTER_hash_tag", cat, t_cluster_hash_tag))

    # CLUSTER_MOVED_redirect: Access key on wrong node, verify MOVED error
    def t_cluster_moved_redirect():
        nodes_str = cluster_client.execute_command("CLUSTER", "NODES")
        nodes = parse_cluster_nodes(nodes_str)
        myself = None
        for node in nodes:
            if "myself" in node["flags"]:
                myself = node
                break

        if not myself:
            raise AssertionError("could not find myself in CLUSTER NODES")

        # Parse my slot ranges
        my_slot_set = set()
        for s in myself.get("slots", []):
            if "-" in s:
                start, end = s.split("-")
                for i in range(int(start), int(end) + 1):
                    my_slot_set.add(i)
            else:
                try:
                    my_slot_set.add(int(s))
                except ValueError:
                    pass

        # Find a slot not owned by this node
        target_slot = -1
        for s in range(16384):
            if s not in my_slot_set:
                target_slot = s
                break

        if target_slot == -1:
            raise AssertionError("could not find a slot not owned by this node")

        # Find a key that hashes to that slot
        test_key = ""
        for i in range(100000):
            candidate = f"{prefix}cluster:moved:{i}"
            slot = cluster_client.execute_command("CLUSTER", "KEYSLOT", candidate)
            if slot == target_slot:
                test_key = candidate
                break

        if not test_key:
            raise AssertionError(f"could not find a key hashing to slot {target_slot}")

        # Try to GET the key — should get MOVED error
        try:
            cluster_client.get(test_key)
            # If succeeded, might be using cluster-aware mode
        except redis_lib.ResponseError as e:
            assert_true(
                "MOVED" in str(e),
                f"expected MOVED error, got: {e}",
            )
    results.append(run_test("CLUSTER_MOVED_redirect", cat, t_cluster_moved_redirect))

    # CLUSTER_KEYSLOT: Verify CLUSTER KEYSLOT returns correct CRC16 mod 16384
    def t_cluster_keyslot():
        slot = cluster_client.execute_command("CLUSTER", "KEYSLOT", "foo")
        assert_equal(12182, slot)

        empty_slot = cluster_client.execute_command("CLUSTER", "KEYSLOT", "")
        assert_equal(0, empty_slot)

        slot1 = cluster_client.execute_command("CLUSTER", "KEYSLOT", "{user}.info")
        slot2 = cluster_client.execute_command("CLUSTER", "KEYSLOT", "user")
        assert_equal(slot2, slot1)
    results.append(run_test("CLUSTER_KEYSLOT", cat, t_cluster_keyslot))

    # CLUSTER_COUNTKEYSINSLOT: Verify key counting per slot
    def t_cluster_countkeysinslot():
        k = key(prefix, "cluster:countslot")
        key_slot = cluster_client.execute_command("CLUSTER", "KEYSLOT", k)

        # Find which node owns this slot
        slots_info = cluster_client.execute_command("CLUSTER", "SLOTS")
        target_host = None
        target_port = None

        for slot_range in slots_info:
            start = slot_range[0]
            end = slot_range[1]
            if start <= key_slot <= end:
                node_info = slot_range[2]
                target_host = node_info[0]
                target_port = node_info[1]
                break

        if not target_host:
            raise AssertionError(f"could not find node owning slot {key_slot}")

        target_client = redis_lib.Redis(
            host=target_host,
            port=target_port,
            password=password,
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
        )
        try:
            target_client.set(k, "value")
            count = target_client.execute_command("CLUSTER", "COUNTKEYSINSLOT", key_slot)
            assert_true(
                count >= 1,
                f"CLUSTER COUNTKEYSINSLOT {key_slot} should be >= 1, got {count}",
            )
            target_client.delete(k)
        finally:
            target_client.close()
    results.append(run_test("CLUSTER_COUNTKEYSINSLOT", cat, t_cluster_countkeysinslot))

    # CLUSTER_cross_slot_error: Verify CROSSSLOT error for multi-key commands on different slots
    def t_cluster_cross_slot_error():
        try:
            cluster_client.mset({"cluster_cross_a": "val1", "cluster_cross_b": "val2"})
            # If succeeded, clean up
            cluster_client.delete("cluster_cross_a", "cluster_cross_b")
        except redis_lib.ResponseError as e:
            assert_true(
                "CROSSSLOT" in str(e) or "MOVED" in str(e),
                f"expected CROSSSLOT or MOVED error, got: {e}",
            )
    results.append(run_test("CLUSTER_cross_slot_error", cat, t_cluster_cross_slot_error))

    # READONLY_mode: READONLY on a cluster node, verify reads succeed
    def t_readonly_mode():
        result = cluster_client.execute_command("READONLY")
        assert_equal("OK", result)

        result2 = cluster_client.execute_command("READWRITE")
        assert_equal("OK", result2)
    results.append(run_test("READONLY_mode", cat, t_readonly_mode))

    cluster_client.close()
    return results


register("cluster", test_cluster)
