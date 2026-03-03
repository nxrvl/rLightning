"""Category: Sentinel tests."""

import os
import time

import redis as redis_lib
from redis.sentinel import Sentinel
from .framework import (
    register, run_test, skip_test, key, assert_equal, assert_true,
)


def test_sentinel(r, prefix):
    results = []
    cat = "sentinel"

    sentinel_host = os.environ.get("REDIS_SENTINEL_HOST", "")
    sentinel_port = int(os.environ.get("REDIS_SENTINEL_PORT", "26379"))

    # If sentinel is not configured, skip all sentinel tests
    if not sentinel_host:
        reason = "REDIS_SENTINEL_HOST not set"
        results.append(skip_test("SENTINEL_master_info", cat, reason))
        results.append(skip_test("SENTINEL_replicas", cat, reason))
        results.append(skip_test("SENTINEL_sentinels", cat, reason))
        results.append(skip_test("SENTINEL_get_master_addr", cat, reason))
        results.append(skip_test("SENTINEL_ckquorum", cat, reason))
        results.append(skip_test("SENTINEL_failover", cat, reason))
        results.append(skip_test("SENTINEL_client_discovery", cat, reason))
        return results

    password = r.connection_pool.connection_kwargs.get("password")

    # Create a direct connection to sentinel for raw commands
    sentinel_client = redis_lib.Redis(
        host=sentinel_host,
        port=sentinel_port,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=10,
    )

    # Wait for sentinel to be ready
    for _ in range(30):
        try:
            sentinel_client.ping()
            break
        except Exception:
            time.sleep(1)

    # Create a Sentinel object for discovery tests
    sentinel_obj = Sentinel(
        [(sentinel_host, sentinel_port)],
        socket_timeout=5,
    )

    # SENTINEL_master_info: SENTINEL MASTER mymaster — verify master host/port/flags
    def t_sentinel_master_info():
        result = sentinel_client.execute_command("SENTINEL", "MASTER", "mymaster")
        # redis-py returns this as a dict when decode_responses=True
        if isinstance(result, list):
            # Convert flat list to dict
            master = {}
            for i in range(0, len(result) - 1, 2):
                master[result[i]] = result[i + 1]
        else:
            master = result

        assert_equal("mymaster", master.get("name", ""))
        flags = master.get("flags", "")
        assert_true("master" in flags, f"expected flags to contain 'master', got: {flags}")
        assert_true("ip" in master and master["ip"], "expected non-empty ip")
        assert_true("port" in master and master["port"], "expected non-empty port")
    results.append(run_test("SENTINEL_master_info", cat, t_sentinel_master_info))

    # SENTINEL_replicas: SENTINEL REPLICAS mymaster — verify replica list
    def t_sentinel_replicas():
        try:
            result = sentinel_client.execute_command("SENTINEL", "REPLICAS", "mymaster")
        except redis_lib.ResponseError:
            # Fallback to SLAVES
            result = sentinel_client.execute_command("SENTINEL", "SLAVES", "mymaster")

        # Result is a list of replicas; each replica may be a dict or flat list
        assert_true(len(result) >= 1, f"expected at least 1 replica, got {len(result)}")

        replica = result[0]
        if isinstance(replica, list):
            replica_dict = {}
            for i in range(0, len(replica) - 1, 2):
                replica_dict[replica[i]] = replica[i + 1]
            replica = replica_dict

        assert_true("ip" in replica, "expected 'ip' field in replica info")
        assert_true("port" in replica, "expected 'port' field in replica info")
    results.append(run_test("SENTINEL_replicas", cat, t_sentinel_replicas))

    # SENTINEL_sentinels: SENTINEL SENTINELS mymaster — verify sentinel peer list
    def t_sentinel_sentinels():
        result = sentinel_client.execute_command("SENTINEL", "SENTINELS", "mymaster")

        assert_true(len(result) >= 2, f"expected at least 2 sentinel peers, got {len(result)}")

        peer = result[0]
        if isinstance(peer, list):
            peer_dict = {}
            for i in range(0, len(peer) - 1, 2):
                peer_dict[peer[i]] = peer[i + 1]
            peer = peer_dict

        assert_true("ip" in peer, "expected 'ip' field in sentinel peer info")
        assert_true("port" in peer, "expected 'port' field in sentinel peer info")
    results.append(run_test("SENTINEL_sentinels", cat, t_sentinel_sentinels))

    # SENTINEL_get_master_addr: SENTINEL GET-MASTER-ADDR-BY-NAME mymaster — verify address
    def t_sentinel_get_master_addr():
        addr = sentinel_client.execute_command("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster")

        assert_true(isinstance(addr, (list, tuple)) and len(addr) == 2,
                     f"expected [host, port] list, got: {addr}")
        assert_true(addr[0] != "" and addr[0] is not None, "expected non-empty host")
        assert_true(addr[1] != "" and addr[1] is not None, "expected non-empty port")
    results.append(run_test("SENTINEL_get_master_addr", cat, t_sentinel_get_master_addr))

    # SENTINEL_ckquorum: SENTINEL CKQUORUM mymaster — verify quorum check
    def t_sentinel_ckquorum():
        result = sentinel_client.execute_command("SENTINEL", "CKQUORUM", "mymaster")
        msg = str(result)
        assert_true(
            "OK" in msg or "ok" in msg or "reachable" in msg,
            f"expected quorum OK response, got: {msg}",
        )
    results.append(run_test("SENTINEL_ckquorum", cat, t_sentinel_ckquorum))

    # SENTINEL_failover: Trigger failover, verify master address still available
    def t_sentinel_failover():
        try:
            sentinel_client.execute_command("SENTINEL", "FAILOVER", "mymaster")
        except redis_lib.ResponseError as e:
            err_msg = str(e)
            # Acceptable: no good slave, failover in progress
            if any(x in err_msg for x in ["NOGOODSLAVE", "in progress", "INPROG", "no good"]):
                return
            raise

        # Wait for failover to stabilize
        time.sleep(3)

        # Verify we can still get master address
        addr = sentinel_client.execute_command("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster")
        assert_true(isinstance(addr, (list, tuple)) and len(addr) == 2,
                     "expected valid master address after failover")
        assert_true(addr[0] != "" and addr[1] != "",
                     "expected non-empty master address after failover")
    results.append(run_test("SENTINEL_failover", cat, t_sentinel_failover))

    # SENTINEL_client_discovery: Connect via sentinel, verify automatic master discovery
    def t_sentinel_client_discovery():
        # Wait for sentinel to recover from any previous failover
        time.sleep(2)

        # Use redis-py Sentinel client to discover master
        master = sentinel_obj.master_for(
            "mymaster",
            socket_timeout=5,
            password=password,
            db=0,
        )

        # Should be able to ping
        master.ping()

        k = key(prefix, "sentinel:discovery_test")
        try:
            master.set(k, "discovered", ex=30)
            val = master.get(k)
            # master_for with no decode_responses returns bytes
            if isinstance(val, bytes):
                val = val.decode("utf-8")
            assert_equal("discovered", val)
        finally:
            master.delete(k)
            master.close()
    results.append(run_test("SENTINEL_client_discovery", cat, t_sentinel_client_discovery))

    sentinel_client.close()
    return results


register("sentinel", test_sentinel)
