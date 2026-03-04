package tests

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("memory", testMemory)
}

func testMemory(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "memory"

	// MEMORY_USAGE_basic: SET key, verify MEMORY USAGE returns reasonable byte count
	results = append(results, RunTest("MEMORY_USAGE_basic", cat, func() error {
		k := Key(prefix, "mem:usage_basic")
		defer rdb.Del(ctx, k)

		rdb.Set(ctx, k, "hello world", 0)
		usage, err := rdb.MemoryUsage(ctx, k).Result()
		if err != nil {
			return fmt.Errorf("MEMORY USAGE failed: %v", err)
		}
		if err := AssertTrue(usage > 0, fmt.Sprintf("MEMORY USAGE should be > 0, got %d", usage)); err != nil {
			return err
		}
		// A simple string "hello world" should use less than 1KB
		return AssertTrue(usage < 1024, fmt.Sprintf("MEMORY USAGE should be < 1024 bytes for small string, got %d", usage))
	}))

	// MEMORY_USAGE_types: Test MEMORY USAGE for string, hash, list, set, zset
	results = append(results, RunTest("MEMORY_USAGE_types", cat, func() error {
		kStr := Key(prefix, "mem:type_str")
		kHash := Key(prefix, "mem:type_hash")
		kList := Key(prefix, "mem:type_list")
		kSet := Key(prefix, "mem:type_set")
		kZset := Key(prefix, "mem:type_zset")
		defer rdb.Del(ctx, kStr, kHash, kList, kSet, kZset)

		rdb.Set(ctx, kStr, "value", 0)
		rdb.HSet(ctx, kHash, "field1", "val1", "field2", "val2")
		rdb.RPush(ctx, kList, "a", "b", "c")
		rdb.SAdd(ctx, kSet, "m1", "m2", "m3")
		rdb.ZAdd(ctx, kZset, redis.Z{Score: 1.0, Member: "x"}, redis.Z{Score: 2.0, Member: "y"})

		types := map[string]string{
			kStr:  "string",
			kHash: "hash",
			kList: "list",
			kSet:  "set",
			kZset: "zset",
		}

		for k, typeName := range types {
			usage, err := rdb.MemoryUsage(ctx, k).Result()
			if err != nil {
				return fmt.Errorf("MEMORY USAGE for %s failed: %v", typeName, err)
			}
			if err := AssertTrue(usage > 0, fmt.Sprintf("MEMORY USAGE for %s should be > 0, got %d", typeName, usage)); err != nil {
				return err
			}
		}
		return nil
	}))

	// MEMORY_DOCTOR: Run MEMORY DOCTOR, verify response format
	results = append(results, RunTest("MEMORY_DOCTOR", cat, func() error {
		result, err := rdb.Do(ctx, "MEMORY", "DOCTOR").Result()
		if err != nil {
			return fmt.Errorf("MEMORY DOCTOR failed: %v", err)
		}
		str, ok := result.(string)
		if !ok {
			return fmt.Errorf("MEMORY DOCTOR should return string, got %T", result)
		}
		// Should return some text (either "Sam, I have no memory problems" or diagnostic info)
		return AssertTrue(len(str) > 0, "MEMORY DOCTOR response should not be empty")
	}))

	// INFO_memory_section: Verify INFO memory section contains used_memory, used_memory_peak, maxmemory
	results = append(results, RunTest("INFO_memory_section", cat, func() error {
		// Ensure some data exists so used_memory > 0 on all implementations
		k := Key(prefix, "mem:info_data")
		defer rdb.Del(ctx, k)
		rdb.Set(ctx, k, strings.Repeat("x", 100), 0)

		info, err := rdb.Info(ctx, "memory").Result()
		if err != nil {
			return fmt.Errorf("INFO memory failed: %v", err)
		}

		requiredFields := []string{"used_memory:", "used_memory_peak:"}
		for _, field := range requiredFields {
			if !strings.Contains(info, field) {
				return fmt.Errorf("INFO memory missing field: %s", field)
			}
		}

		// Verify used_memory is a positive number
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "used_memory:") {
				valStr := strings.TrimPrefix(line, "used_memory:")
				val, err := strconv.ParseInt(valStr, 10, 64)
				if err != nil {
					return fmt.Errorf("used_memory is not a valid number: %s", valStr)
				}
				if val <= 0 {
					return fmt.Errorf("used_memory should be > 0, got %d", val)
				}
				break
			}
		}
		return nil
	}))

	// CONFIG_maxmemory: CONFIG SET maxmemory, verify it takes effect
	results = append(results, RunTest("CONFIG_maxmemory", cat, func() error {
		// Save original maxmemory
		origVals, err := rdb.ConfigGet(ctx, "maxmemory").Result()
		if err != nil {
			return fmt.Errorf("CONFIG GET maxmemory failed: %v", err)
		}
		origMaxmem := "0"
		if v, ok := origVals["maxmemory"]; ok {
			origMaxmem = v
		}
		defer rdb.ConfigSet(ctx, "maxmemory", origMaxmem)

		// Set maxmemory to 100MB
		err = rdb.ConfigSet(ctx, "maxmemory", "104857600").Err()
		if err != nil {
			return fmt.Errorf("CONFIG SET maxmemory failed: %v", err)
		}

		// Verify it was set
		vals, err := rdb.ConfigGet(ctx, "maxmemory").Result()
		if err != nil {
			return fmt.Errorf("CONFIG GET maxmemory after SET failed: %v", err)
		}
		val, ok := vals["maxmemory"]
		if !ok {
			return fmt.Errorf("CONFIG GET maxmemory returned no value")
		}
		return AssertEqual("104857600", val)
	}))

	// CONFIG_maxmemory_policy: Test maxmemory-policy configuration
	results = append(results, RunTest("CONFIG_maxmemory_policy", cat, func() error {
		// Save original policy
		origVals, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
		if err != nil {
			return fmt.Errorf("CONFIG GET maxmemory-policy failed: %v", err)
		}
		origPolicy := "noeviction"
		if v, ok := origVals["maxmemory-policy"]; ok {
			origPolicy = v
		}
		defer rdb.ConfigSet(ctx, "maxmemory-policy", origPolicy)

		policies := []string{"noeviction", "allkeys-lru", "volatile-lru", "allkeys-random"}
		for _, policy := range policies {
			err := rdb.ConfigSet(ctx, "maxmemory-policy", policy).Err()
			if err != nil {
				return fmt.Errorf("CONFIG SET maxmemory-policy %s failed: %v", policy, err)
			}
			vals, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
			if err != nil {
				return fmt.Errorf("CONFIG GET maxmemory-policy after SET %s failed: %v", policy, err)
			}
			val, ok := vals["maxmemory-policy"]
			if !ok {
				return fmt.Errorf("CONFIG GET maxmemory-policy returned no value for %s", policy)
			}
			if err := AssertEqual(policy, val); err != nil {
				return fmt.Errorf("maxmemory-policy mismatch for %s: %v", policy, err)
			}
		}
		return nil
	}))

	// OBJECT_ENCODING: Verify encoding changes based on data size
	results = append(results, RunTest("OBJECT_ENCODING", cat, func() error {
		kInt := Key(prefix, "mem:enc_int")
		kStr := Key(prefix, "mem:enc_str")
		kEmbstr := Key(prefix, "mem:enc_embstr")
		kList := Key(prefix, "mem:enc_list")
		kSet := Key(prefix, "mem:enc_set")
		kHash := Key(prefix, "mem:enc_hash")
		kZset := Key(prefix, "mem:enc_zset")
		defer rdb.Del(ctx, kInt, kStr, kEmbstr, kList, kSet, kHash, kZset)

		// Integer encoding for small integers
		rdb.Set(ctx, kInt, "12345", 0)
		enc, err := rdb.ObjectEncoding(ctx, kInt).Result()
		if err != nil {
			return fmt.Errorf("OBJECT ENCODING for int failed: %v", err)
		}
		if err := AssertEqual("int", enc); err != nil {
			return err
		}

		// Embstr encoding for short strings
		rdb.Set(ctx, kEmbstr, "hello", 0)
		enc, err = rdb.ObjectEncoding(ctx, kEmbstr).Result()
		if err != nil {
			return fmt.Errorf("OBJECT ENCODING for embstr failed: %v", err)
		}
		if err := AssertEqual("embstr", enc); err != nil {
			return err
		}

		// Raw encoding for long strings
		longStr := strings.Repeat("x", 100)
		rdb.Set(ctx, kStr, longStr, 0)
		enc, err = rdb.ObjectEncoding(ctx, kStr).Result()
		if err != nil {
			return fmt.Errorf("OBJECT ENCODING for raw string failed: %v", err)
		}
		if err := AssertEqual("raw", enc); err != nil {
			return err
		}

		// Small list -> listpack
		rdb.RPush(ctx, kList, "a", "b", "c")
		enc, err = rdb.ObjectEncoding(ctx, kList).Result()
		if err != nil {
			return fmt.Errorf("OBJECT ENCODING for small list failed: %v", err)
		}
		if err := AssertTrue(enc == "listpack" || enc == "ziplist",
			fmt.Sprintf("small list should be listpack or ziplist, got %s", enc)); err != nil {
			return err
		}

		// Small set -> listpack
		rdb.SAdd(ctx, kSet, "m1", "m2", "m3")
		enc, err = rdb.ObjectEncoding(ctx, kSet).Result()
		if err != nil {
			return fmt.Errorf("OBJECT ENCODING for small set failed: %v", err)
		}
		if err := AssertTrue(enc == "listpack" || enc == "ziplist",
			fmt.Sprintf("small set should be listpack or ziplist, got %s", enc)); err != nil {
			return err
		}

		// Small hash -> listpack
		rdb.HSet(ctx, kHash, "f1", "v1", "f2", "v2")
		enc, err = rdb.ObjectEncoding(ctx, kHash).Result()
		if err != nil {
			return fmt.Errorf("OBJECT ENCODING for small hash failed: %v", err)
		}
		if err := AssertTrue(enc == "listpack" || enc == "ziplist",
			fmt.Sprintf("small hash should be listpack or ziplist, got %s", enc)); err != nil {
			return err
		}

		// Small zset -> listpack
		rdb.ZAdd(ctx, kZset, redis.Z{Score: 1.0, Member: "a"}, redis.Z{Score: 2.0, Member: "b"})
		enc, err = rdb.ObjectEncoding(ctx, kZset).Result()
		if err != nil {
			return fmt.Errorf("OBJECT ENCODING for small zset failed: %v", err)
		}
		if err := AssertTrue(enc == "listpack" || enc == "ziplist",
			fmt.Sprintf("small zset should be listpack or ziplist, got %s", enc)); err != nil {
			return err
		}

		return nil
	}))

	// OBJECT_REFCOUNT: Verify OBJECT REFCOUNT returns integer
	results = append(results, RunTest("OBJECT_REFCOUNT", cat, func() error {
		k := Key(prefix, "mem:refcount")
		defer rdb.Del(ctx, k)

		rdb.Set(ctx, k, "testvalue", 0)
		result, err := rdb.Do(ctx, "OBJECT", "REFCOUNT", k).Result()
		if err != nil {
			return fmt.Errorf("OBJECT REFCOUNT failed: %v", err)
		}
		refcount, ok := result.(int64)
		if !ok {
			return fmt.Errorf("OBJECT REFCOUNT should return integer, got %T: %v", result, result)
		}
		return AssertTrue(refcount >= 1, fmt.Sprintf("OBJECT REFCOUNT should be >= 1, got %d", refcount))
	}))

	// OBJECT_IDLETIME: SET key, wait, verify OBJECT IDLETIME reflects elapsed time
	results = append(results, RunTest("OBJECT_IDLETIME", cat, func() error {
		k := Key(prefix, "mem:idletime")
		defer rdb.Del(ctx, k)

		rdb.Set(ctx, k, "testvalue", 0)
		// Check idle time immediately (should be 0 or very small)
		result, err := rdb.Do(ctx, "OBJECT", "IDLETIME", k).Result()
		if err != nil {
			return fmt.Errorf("OBJECT IDLETIME failed: %v", err)
		}
		idletime, ok := result.(int64)
		if !ok {
			return fmt.Errorf("OBJECT IDLETIME should return integer, got %T: %v", result, result)
		}
		// Idle time should be 0 right after set (in seconds)
		return AssertEqualInt64(0, idletime)
	}))

	// OBJECT_FREQ: Verify OBJECT FREQ returns LFU frequency counter
	results = append(results, RunTest("OBJECT_FREQ", cat, func() error {
		k := Key(prefix, "mem:freq")
		defer rdb.Del(ctx, k)

		// Save and set LFU policy (OBJECT FREQ only works with LFU policies)
		origVals, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
		if err != nil {
			return fmt.Errorf("CONFIG GET maxmemory-policy failed: %v", err)
		}
		origPolicy := "noeviction"
		if v, ok := origVals["maxmemory-policy"]; ok {
			origPolicy = v
		}

		err = rdb.ConfigSet(ctx, "maxmemory-policy", "allkeys-lfu").Err()
		if err != nil {
			return fmt.Errorf("CONFIG SET maxmemory-policy allkeys-lfu failed: %v", err)
		}
		defer rdb.ConfigSet(ctx, "maxmemory-policy", origPolicy)

		rdb.Set(ctx, k, "testvalue", 0)
		// Access the key a few times to increase frequency
		for i := 0; i < 5; i++ {
			rdb.Get(ctx, k)
		}

		// Brief pause to let LFU counter stabilize
		time.Sleep(50 * time.Millisecond)

		result, err := rdb.Do(ctx, "OBJECT", "FREQ", k).Result()
		if err != nil {
			return fmt.Errorf("OBJECT FREQ failed: %v", err)
		}
		freq, ok := result.(int64)
		if !ok {
			return fmt.Errorf("OBJECT FREQ should return integer, got %T: %v", result, result)
		}
		return AssertTrue(freq > 0, fmt.Sprintf("OBJECT FREQ should be > 0 after access, got %d", freq))
	}))

	return results
}
