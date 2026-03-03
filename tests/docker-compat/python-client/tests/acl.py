"""Category: ACL & Security tests."""

import redis as redis_lib
from .framework import (
    register, run_test, key, assert_equal, assert_true,
)


def test_acl(r, prefix):
    results = []
    cat = "acl"

    def cleanup_users(*users):
        for u in users:
            try:
                r.execute_command("ACL", "DELUSER", u)
            except Exception:
                pass

    def create_user_client(username, password):
        return redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            username=username,
            password=password,
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
        )

    # ACL_SETUSER_basic: Create user with specific permissions, verify access
    def t_acl_setuser_basic():
        user = "acl_test_basic"
        cleanup_users(user)
        try:
            r.execute_command("ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all")

            r2 = create_user_client(user, "testpass123")
            try:
                k = key(prefix, "acl:basic_test")
                r2.set(k, "hello_acl")
                got = r2.get(k)
                assert_equal("hello_acl", got)
                r.delete(k)
            finally:
                r2.close()
        finally:
            cleanup_users(user)
    results.append(run_test("ACL_SETUSER_basic", cat, t_acl_setuser_basic))

    # ACL_SETUSER_command_restrictions: Restricted commands, verify NOPERM
    def t_acl_setuser_command_restrictions():
        user = "acl_test_cmdrestr"
        cleanup_users(user)
        try:
            r.execute_command("ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all", "-del")

            r2 = create_user_client(user, "testpass123")
            try:
                k = key(prefix, "acl:cmdrestr_test")

                # SET should work
                r2.set(k, "value")

                # GET should work
                got = r2.get(k)
                assert_equal("value", got)

                # DEL should be denied (NOPERM)
                denied = False
                try:
                    r2.delete(k)
                except redis_lib.exceptions.ResponseError as e:
                    err_msg = str(e).lower()
                    if "noperm" in err_msg or "no permissions" in err_msg:
                        denied = True
                    else:
                        raise
                assert_true(denied, "DEL should return NOPERM for restricted user")

                r.delete(k)
            finally:
                r2.close()
        finally:
            cleanup_users(user)
    results.append(run_test("ACL_SETUSER_command_restrictions", cat, t_acl_setuser_command_restrictions))

    # ACL_SETUSER_key_patterns: Key pattern restrictions, verify access control
    def t_acl_setuser_key_patterns():
        user = "acl_test_keypattern"
        cleanup_users(user)
        try:
            allowed_pattern = f"~{prefix}acl:allowed:*"
            r.execute_command("ACL", "SETUSER", user, "on", ">testpass123", allowed_pattern, "&*", "+@all")

            r2 = create_user_client(user, "testpass123")
            try:
                allowed_key = key(prefix, "acl:allowed:testkey")
                denied_key = key(prefix, "acl:denied:testkey")

                # SET on allowed key should work
                r2.set(allowed_key, "allowed_val")
                got = r2.get(allowed_key)
                assert_equal("allowed_val", got)

                # SET on denied key should fail
                denied = False
                try:
                    r2.set(denied_key, "denied_val")
                except redis_lib.exceptions.ResponseError as e:
                    err_msg = str(e).lower()
                    if "noperm" in err_msg or "no permissions" in err_msg:
                        denied = True
                    else:
                        raise
                assert_true(denied, "SET on denied key should return NOPERM")

                r.delete(allowed_key, denied_key)
            finally:
                r2.close()
        finally:
            cleanup_users(user)
    results.append(run_test("ACL_SETUSER_key_patterns", cat, t_acl_setuser_key_patterns))

    # ACL_DELUSER: Create user, delete, verify deleted user cannot AUTH
    def t_acl_deluser():
        user = "acl_test_delme"
        cleanup_users(user)

        # Create user
        r.execute_command("ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all")

        # Verify user works before deletion
        r2 = create_user_client(user, "testpass123")
        try:
            r2.ping()
        finally:
            r2.close()

        # Delete user
        r.execute_command("ACL", "DELUSER", user)

        # Try to connect as deleted user - should fail
        r3 = create_user_client(user, "testpass123")
        auth_failed = False
        try:
            r3.ping()
        except (redis_lib.exceptions.AuthenticationError, redis_lib.exceptions.ResponseError):
            auth_failed = True
        finally:
            r3.close()
        assert_true(auth_failed, "AUTH should fail for deleted user")
    results.append(run_test("ACL_DELUSER", cat, t_acl_deluser))

    # ACL_LIST: Create multiple users, verify ACL LIST returns all
    def t_acl_list():
        user1 = "acl_test_list1"
        user2 = "acl_test_list2"
        cleanup_users(user1, user2)
        try:
            r.execute_command("ACL", "SETUSER", user1, "on", ">pass1", "~*", "&*", "+@all")
            r.execute_command("ACL", "SETUSER", user2, "on", ">pass2", "~*", "&*", "+@all")

            acl_list = r.execute_command("ACL", "LIST")
            list_str = "\n".join(str(entry) for entry in acl_list)
            assert_true(user1 in list_str, f"ACL LIST should contain {user1}")
            assert_true(user2 in list_str, f"ACL LIST should contain {user2}")
        finally:
            cleanup_users(user1, user2)
    results.append(run_test("ACL_LIST", cat, t_acl_list))

    # ACL_WHOAMI: AUTH as different users, verify ACL WHOAMI returns correct username
    def t_acl_whoami():
        user = "acl_test_whoami"
        cleanup_users(user)
        try:
            r.execute_command("ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all")

            r2 = create_user_client(user, "testpass123")
            try:
                whoami = r2.execute_command("ACL", "WHOAMI")
                assert_equal(user, whoami)
            finally:
                r2.close()
        finally:
            cleanup_users(user)
    results.append(run_test("ACL_WHOAMI", cat, t_acl_whoami))

    # ACL_LOG: Execute denied command, verify ACL LOG records the denial
    def t_acl_log():
        user = "acl_test_log"
        cleanup_users(user)
        try:
            # Reset ACL LOG
            r.execute_command("ACL", "LOG", "RESET")

            r.execute_command("ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all", "-del")

            r2 = create_user_client(user, "testpass123")
            try:
                k = key(prefix, "acl:log_test")
                # Try DEL (should be denied, ignore error)
                try:
                    r2.delete(k)
                except Exception:
                    pass
            finally:
                r2.close()

            # Check ACL LOG on admin connection
            log = r.execute_command("ACL", "LOG")
            assert_true(isinstance(log, list) and len(log) > 0,
                        "ACL LOG should have entries after denied command")
        finally:
            cleanup_users(user)
    results.append(run_test("ACL_LOG", cat, t_acl_log))

    # ACL_CAT: Verify ACL CAT returns command categories
    def t_acl_cat():
        categories = r.execute_command("ACL", "CAT")
        assert_true(isinstance(categories, (list, tuple)), "ACL CAT should return a list")
        assert_true(len(categories) > 0, "ACL CAT should return categories")

        cat_lower = [str(c).lower() for c in categories]
        for expected in ["string", "hash", "list", "set", "sortedset"]:
            assert_true(expected in cat_lower, f"ACL CAT should contain '{expected}'")
    results.append(run_test("ACL_CAT", cat, t_acl_cat))

    # AUTH_username_password: Test named user authentication (AUTH username password)
    def t_auth_username_password():
        user = "acl_test_auth"
        cleanup_users(user)
        try:
            r.execute_command("ACL", "SETUSER", user, "on", ">authpass456", "~*", "&*", "+@all")

            r2 = create_user_client(user, "authpass456")
            try:
                assert_true(r2.ping(), "PING should succeed for authenticated user")
                whoami = r2.execute_command("ACL", "WHOAMI")
                assert_equal(user, whoami)
            finally:
                r2.close()
        finally:
            cleanup_users(user)
    results.append(run_test("AUTH_username_password", cat, t_auth_username_password))

    # AUTH_default_user: Test default user password authentication
    def t_auth_default_user():
        whoami = r.execute_command("ACL", "WHOAMI")
        assert_equal("default", whoami)
    results.append(run_test("AUTH_default_user", cat, t_auth_default_user))

    return results


register("acl", test_acl)
