package tests

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("acl", testACL)
}

func testACL(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "acl"

	// Helper to clean up test users
	cleanupUsers := func(users ...string) {
		for _, u := range users {
			rdb.Do(ctx, "ACL", "DELUSER", u)
		}
	}

	// ACL_SETUSER_basic: Create user with specific permissions, verify access
	results = append(results, RunTest("ACL_SETUSER_basic", cat, func() error {
		user := "acl_test_basic"
		cleanupUsers(user)
		defer cleanupUsers(user)

		_, err := rdb.Do(ctx, "ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER failed: %v", err)
		}

		userClient := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Username: user,
			Password: "testpass123",
			DB:       0,
		})
		defer userClient.Close()

		k := Key(prefix, "acl:basic_test")
		defer rdb.Del(ctx, k)

		if err := userClient.Set(ctx, k, "hello_acl", 0).Err(); err != nil {
			return fmt.Errorf("SET as test user failed: %v", err)
		}
		got, err := userClient.Get(ctx, k).Result()
		if err != nil {
			return fmt.Errorf("GET as test user failed: %v", err)
		}
		return AssertEqual("hello_acl", got)
	}))

	// ACL_SETUSER_command_restrictions: Create user with restricted commands, verify NOPERM
	results = append(results, RunTest("ACL_SETUSER_command_restrictions", cat, func() error {
		user := "acl_test_cmdrestr"
		cleanupUsers(user)
		defer cleanupUsers(user)

		// Give all commands except DEL
		_, err := rdb.Do(ctx, "ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all", "-del").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER failed: %v", err)
		}

		userClient := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Username: user,
			Password: "testpass123",
			DB:       0,
		})
		defer userClient.Close()

		k := Key(prefix, "acl:cmdrestr_test")
		defer rdb.Del(ctx, k)

		// SET should work
		if err := userClient.Set(ctx, k, "value", 0).Err(); err != nil {
			return fmt.Errorf("SET should be allowed: %v", err)
		}

		// GET should work
		got, err := userClient.Get(ctx, k).Result()
		if err != nil {
			return fmt.Errorf("GET should be allowed: %v", err)
		}
		if err := AssertEqual("value", got); err != nil {
			return err
		}

		// DEL should be denied (NOPERM)
		err = userClient.Del(ctx, k).Err()
		if err == nil {
			return fmt.Errorf("DEL should be denied for restricted user")
		}
		if !strings.Contains(err.Error(), "NOPERM") {
			return fmt.Errorf("expected NOPERM error, got: %v", err)
		}

		return nil
	}))

	// ACL_SETUSER_key_patterns: Create user with key pattern restrictions
	results = append(results, RunTest("ACL_SETUSER_key_patterns", cat, func() error {
		user := "acl_test_keypattern"
		cleanupUsers(user)
		defer cleanupUsers(user)

		// Allow only keys matching prefix + "acl:allowed:*"
		allowedPattern := fmt.Sprintf("~%sacl:allowed:*", prefix)
		_, err := rdb.Do(ctx, "ACL", "SETUSER", user, "on", ">testpass123", allowedPattern, "&*", "+@all").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER failed: %v", err)
		}

		userClient := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Username: user,
			Password: "testpass123",
			DB:       0,
		})
		defer userClient.Close()

		allowedKey := Key(prefix, "acl:allowed:testkey")
		deniedKey := Key(prefix, "acl:denied:testkey")
		defer rdb.Del(ctx, allowedKey, deniedKey)

		// SET on allowed key should work
		if err := userClient.Set(ctx, allowedKey, "allowed_val", 0).Err(); err != nil {
			return fmt.Errorf("SET on allowed key failed: %v", err)
		}

		// GET on allowed key should work
		got, err := userClient.Get(ctx, allowedKey).Result()
		if err != nil {
			return fmt.Errorf("GET on allowed key failed: %v", err)
		}
		if err := AssertEqual("allowed_val", got); err != nil {
			return err
		}

		// SET on denied key should fail
		err = userClient.Set(ctx, deniedKey, "denied_val", 0).Err()
		if err == nil {
			return fmt.Errorf("SET on denied key should fail for restricted user")
		}
		if !strings.Contains(err.Error(), "NOPERM") {
			return fmt.Errorf("expected NOPERM error for key access, got: %v", err)
		}

		return nil
	}))

	// ACL_DELUSER: Create user, delete, verify deleted user cannot AUTH
	results = append(results, RunTest("ACL_DELUSER", cat, func() error {
		user := "acl_test_delme"
		cleanupUsers(user)

		// Create user
		_, err := rdb.Do(ctx, "ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER failed: %v", err)
		}

		// Verify user works before deletion
		userClient := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Username: user,
			Password: "testpass123",
			DB:       0,
		})
		if err := userClient.Ping(ctx).Err(); err != nil {
			userClient.Close()
			return fmt.Errorf("user should be able to connect before deletion: %v", err)
		}
		userClient.Close()

		// Delete user
		_, err = rdb.Do(ctx, "ACL", "DELUSER", user).Result()
		if err != nil {
			return fmt.Errorf("ACL DELUSER failed: %v", err)
		}

		// Try to connect as deleted user - should fail
		userClient2 := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Username: user,
			Password: "testpass123",
			DB:       0,
		})
		defer userClient2.Close()

		err = userClient2.Ping(ctx).Err()
		if err == nil {
			return fmt.Errorf("AUTH should fail for deleted user")
		}
		return nil
	}))

	// ACL_LIST: Create multiple users, verify ACL LIST returns all
	results = append(results, RunTest("ACL_LIST", cat, func() error {
		user1 := "acl_test_list1"
		user2 := "acl_test_list2"
		cleanupUsers(user1, user2)
		defer cleanupUsers(user1, user2)

		_, err := rdb.Do(ctx, "ACL", "SETUSER", user1, "on", ">pass1", "~*", "&*", "+@all").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER user1 failed: %v", err)
		}
		_, err = rdb.Do(ctx, "ACL", "SETUSER", user2, "on", ">pass2", "~*", "&*", "+@all").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER user2 failed: %v", err)
		}

		list, err := rdb.Do(ctx, "ACL", "LIST").StringSlice()
		if err != nil {
			return fmt.Errorf("ACL LIST failed: %v", err)
		}

		found1, found2 := false, false
		for _, entry := range list {
			if strings.Contains(entry, user1) {
				found1 = true
			}
			if strings.Contains(entry, user2) {
				found2 = true
			}
		}
		if !found1 {
			return fmt.Errorf("ACL LIST should contain user %s", user1)
		}
		if !found2 {
			return fmt.Errorf("ACL LIST should contain user %s", user2)
		}
		return nil
	}))

	// ACL_WHOAMI: AUTH as different users, verify ACL WHOAMI returns correct username
	results = append(results, RunTest("ACL_WHOAMI", cat, func() error {
		user := "acl_test_whoami"
		cleanupUsers(user)
		defer cleanupUsers(user)

		_, err := rdb.Do(ctx, "ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER failed: %v", err)
		}

		userClient := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Username: user,
			Password: "testpass123",
			DB:       0,
		})
		defer userClient.Close()

		whoami, err := userClient.Do(ctx, "ACL", "WHOAMI").Text()
		if err != nil {
			return fmt.Errorf("ACL WHOAMI failed: %v", err)
		}
		return AssertEqual(user, whoami)
	}))

	// ACL_LOG: Execute denied command, verify ACL LOG records the denial
	results = append(results, RunTest("ACL_LOG", cat, func() error {
		user := "acl_test_log"
		cleanupUsers(user)
		defer cleanupUsers(user)

		// Reset ACL LOG
		rdb.Do(ctx, "ACL", "LOG", "RESET")

		// Create user with restricted commands (no DEL)
		_, err := rdb.Do(ctx, "ACL", "SETUSER", user, "on", ">testpass123", "~*", "&*", "+@all", "-del").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER failed: %v", err)
		}

		// Connect as restricted user and try denied command
		userClient := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Username: user,
			Password: "testpass123",
			DB:       0,
		})
		defer userClient.Close()

		k := Key(prefix, "acl:log_test")
		defer rdb.Del(ctx, k)

		// Try DEL (should be denied, ignore error)
		userClient.Del(ctx, k)

		// Check ACL LOG on admin connection
		entries, err := rdb.Do(ctx, "ACL", "LOG").Slice()
		if err != nil {
			return fmt.Errorf("ACL LOG failed: %v", err)
		}

		return AssertTrue(len(entries) > 0, "ACL LOG should have entries after denied command")
	}))

	// ACL_CAT: Verify ACL CAT returns command categories
	results = append(results, RunTest("ACL_CAT", cat, func() error {
		categories, err := rdb.Do(ctx, "ACL", "CAT").StringSlice()
		if err != nil {
			return fmt.Errorf("ACL CAT failed: %v", err)
		}

		if err := AssertTrue(len(categories) > 0, "ACL CAT should return categories"); err != nil {
			return err
		}

		catSet := make(map[string]bool)
		for _, c := range categories {
			catSet[strings.ToLower(c)] = true
		}

		for _, expected := range []string{"string", "hash", "list", "set", "sortedset"} {
			if !catSet[expected] {
				return fmt.Errorf("ACL CAT should contain category '%s', got: %v", expected, categories)
			}
		}
		return nil
	}))

	// AUTH_username_password: Test named user authentication (AUTH username password)
	results = append(results, RunTest("AUTH_username_password", cat, func() error {
		user := "acl_test_auth"
		cleanupUsers(user)
		defer cleanupUsers(user)

		_, err := rdb.Do(ctx, "ACL", "SETUSER", user, "on", ">authpass456", "~*", "&*", "+@all").Result()
		if err != nil {
			return fmt.Errorf("ACL SETUSER failed: %v", err)
		}

		userClient := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Username: user,
			Password: "authpass456",
			DB:       0,
		})
		defer userClient.Close()

		pong, err := userClient.Ping(ctx).Result()
		if err != nil {
			return fmt.Errorf("PING as named user failed: %v", err)
		}
		if err := AssertEqual("PONG", pong); err != nil {
			return err
		}

		whoami, err := userClient.Do(ctx, "ACL", "WHOAMI").Text()
		if err != nil {
			return fmt.Errorf("ACL WHOAMI failed: %v", err)
		}
		return AssertEqual(user, whoami)
	}))

	// AUTH_default_user: Test default user password authentication
	results = append(results, RunTest("AUTH_default_user", cat, func() error {
		whoami, err := rdb.Do(ctx, "ACL", "WHOAMI").Text()
		if err != nil {
			return fmt.Errorf("ACL WHOAMI failed: %v", err)
		}
		return AssertEqual("default", whoami)
	}))

	return results
}
