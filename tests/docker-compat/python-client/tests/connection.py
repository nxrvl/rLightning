"""Category 1: Connection & Auth tests."""

import redis as redis_lib
from .framework import register, run_test, key, assert_equal, assert_true


def test_connection(r, prefix):
    results = []

    def t_ping():
        res = r.ping()
        assert_true(res is True, "PING should return True")
    results.append(run_test("PING_PONG", "connection", t_ping))

    def t_echo():
        msg = "hello rLightning"
        res = r.echo(msg)
        assert_equal(msg, res)
    results.append(run_test("ECHO", "connection", t_echo))

    def t_select():
        r0 = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=True,
        )
        r1 = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=1, decode_responses=True,
        )
        k_ = key(prefix, "select_test")
        try:
            r0.delete(k_)
            r1.delete(k_)
            r1.set(k_, "db1value")
            val = r1.get(k_)
            assert_equal("db1value", val)
            val0 = r0.get(k_)
            assert_true(val0 is None, "key should not exist in DB 0")
        finally:
            r0.delete(k_)
            r1.delete(k_)
            r0.close()
            r1.close()
    results.append(run_test("SELECT_database", "connection", t_select))

    def t_client_setname_getname():
        name = "python-compat-test"
        r.client_setname(name)
        got = r.client_getname()
        assert_equal(name, got)
    results.append(run_test("CLIENT_SETNAME_GETNAME", "connection", t_client_setname_getname))

    def t_client_id():
        cid = r.client_id()
        assert_true(cid > 0, "client ID should be positive")
    results.append(run_test("CLIENT_ID", "connection", t_client_id))

    def t_dbsize():
        k_ = key(prefix, "dbsize_test")
        try:
            r.set(k_, "val")
            size = r.dbsize()
            assert_true(size > 0, "DBSIZE should be > 0")
        finally:
            r.delete(k_)
    results.append(run_test("DBSIZE", "connection", t_dbsize))

    def t_info_server():
        info = r.info("server")
        assert_true(len(info) > 0, "INFO should return data")
        assert_true(
            "redis_version" in info or "server" in str(info),
            "INFO server should contain version info",
        )
    results.append(run_test("INFO_server", "connection", t_info_server))

    def t_auth():
        pw = r.connection_pool.connection_kwargs.get("password")
        if pw:
            r.execute_command("AUTH", pw)
    results.append(run_test("AUTH_already_authenticated", "connection", t_auth))

    return results


register("connection", test_connection)
