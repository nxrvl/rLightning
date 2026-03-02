package tests

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("connection", testConnection)
}

func testConnection(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult

	results = append(results, RunTest("PING_PONG", "connection", func() error {
		res, err := rdb.Ping(ctx).Result()
		if err != nil {
			return err
		}
		return AssertEqual("PONG", res)
	}))

	results = append(results, RunTest("ECHO", "connection", func() error {
		msg := "hello rLightning"
		res, err := rdb.Echo(ctx, msg).Result()
		if err != nil {
			return err
		}
		return AssertEqual(msg, res)
	}))

	results = append(results, RunTest("SELECT_database", "connection", func() error {
		// Use two separate clients for DB 0 and DB 1
		rdb0 := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
			DB:       0,
		})
		defer rdb0.Close()

		rdb1 := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
			DB:       1,
		})
		defer rdb1.Close()

		k := Key(prefix, "select_test")
		defer rdb0.Del(ctx, k)
		defer rdb1.Del(ctx, k)

		// Clean both DBs first
		rdb0.Del(ctx, k)
		rdb1.Del(ctx, k)

		if err := rdb1.Set(ctx, k, "db1value", 0).Err(); err != nil {
			return err
		}
		// Key should exist in DB 1
		val, err := rdb1.Get(ctx, k).Result()
		if err != nil {
			return err
		}
		if err := AssertEqual("db1value", val); err != nil {
			return err
		}
		// Key should not exist in DB 0
		_, err = rdb0.Get(ctx, k).Result()
		return AssertErr(err)
	}))

	results = append(results, RunTest("CLIENT_SETNAME_GETNAME", "connection", func() error {
		name := "go-compat-test"
		if err := rdb.Do(ctx, "CLIENT", "SETNAME", name).Err(); err != nil {
			return err
		}
		got, err := rdb.Do(ctx, "CLIENT", "GETNAME").Text()
		if err != nil {
			return err
		}
		return AssertEqual(name, got)
	}))

	results = append(results, RunTest("CLIENT_ID", "connection", func() error {
		id, err := rdb.ClientID(ctx).Result()
		if err != nil {
			return err
		}
		return AssertTrue(id > 0, "client ID should be positive")
	}))

	results = append(results, RunTest("DBSIZE", "connection", func() error {
		// Set a key to ensure DBSIZE > 0
		k := Key(prefix, "dbsize_test")
		defer rdb.Del(ctx, k)

		rdb.Set(ctx, k, "val", 0)
		size, err := rdb.DBSize(ctx).Result()
		if err != nil {
			return err
		}
		return AssertTrue(size > 0, "DBSIZE should be > 0")
	}))

	results = append(results, RunTest("INFO_server", "connection", func() error {
		info, err := rdb.Info(ctx, "server").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(len(info) > 0, "INFO should return data"); err != nil {
			return err
		}
		return AssertTrue(strings.Contains(info, "redis_version") || strings.Contains(info, "server"),
			"INFO server should contain version info")
	}))

	results = append(results, RunTest("AUTH_already_authenticated", "connection", func() error {
		// We're already authenticated; re-AUTH should succeed
		err := rdb.Do(ctx, "AUTH", rdb.Options().Password).Err()
		return AssertNil(err)
	}))

	return results
}
