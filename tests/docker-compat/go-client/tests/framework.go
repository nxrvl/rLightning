package tests

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestResult represents a single test result matching the JSON schema.
type TestResult struct {
	Name       string  `json:"name"`
	Category   string  `json:"category"`
	Status     string  `json:"status"`
	DurationMs float64 `json:"duration_ms"`
	Error      string  `json:"error,omitempty"`
	Details    string  `json:"details,omitempty"`
}

// TestFunc runs tests for a category and returns results.
type TestFunc func(ctx context.Context, rdb *redis.Client, prefix string) []TestResult

type categoryTest struct {
	category string
	fn       TestFunc
}

var registry []categoryTest

// Register adds a test function for a category.
func Register(category string, fn TestFunc) {
	registry = append(registry, categoryTest{category: category, fn: fn})
}

// RunAll runs all registered tests and returns results.
func RunAll(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	for _, ct := range registry {
		results = append(results, ct.fn(ctx, rdb, prefix)...)
	}
	return results
}

// RunTest runs a single test function, catching panics, and returns a TestResult.
func RunTest(name, category string, fn func() error) TestResult {
	start := time.Now()
	result := TestResult{Name: name, Category: category}

	func() {
		defer func() {
			if r := recover(); r != nil {
				result.Status = "error"
				result.Error = fmt.Sprintf("panic: %v", r)
			}
		}()
		if err := fn(); err != nil {
			result.Status = "fail"
			result.Error = err.Error()
		} else {
			result.Status = "pass"
		}
	}()

	result.DurationMs = float64(time.Since(start).Milliseconds())
	return result
}

// SkipTest returns a skipped test result.
func SkipTest(name, category, reason string) TestResult {
	return TestResult{Name: name, Category: category, Status: "skip", Details: reason}
}

// AssertEqual checks that expected == actual using string comparison.
func AssertEqual(expected, actual interface{}) error {
	e := fmt.Sprintf("%v", expected)
	a := fmt.Sprintf("%v", actual)
	if e != a {
		return fmt.Errorf("expected %v, got %v", expected, actual)
	}
	return nil
}

// AssertTrue checks that val is true.
func AssertTrue(val bool, msg string) error {
	if !val {
		return fmt.Errorf("assertion failed: %s", msg)
	}
	return nil
}

// AssertNil checks that err is nil.
func AssertNil(err error) error {
	if err != nil {
		return fmt.Errorf("expected nil error, got: %v", err)
	}
	return nil
}

// AssertErr checks that err is not nil.
func AssertErr(err error) error {
	if err == nil {
		return fmt.Errorf("expected error, got nil")
	}
	return nil
}

// AssertEqualInt64 compares two int64 values.
func AssertEqualInt64(expected, actual int64) error {
	if expected != actual {
		return fmt.Errorf("expected %d, got %d", expected, actual)
	}
	return nil
}

// AssertEqualFloat64 compares two float64 values with tolerance.
func AssertEqualFloat64(expected, actual, tolerance float64) error {
	diff := expected - actual
	if diff < 0 {
		diff = -diff
	}
	if diff > tolerance {
		return fmt.Errorf("expected %f, got %f (tolerance %f)", expected, actual, tolerance)
	}
	return nil
}

// AssertStringSliceEqual checks two string slices are equal (order matters).
func AssertStringSliceEqual(expected, actual []string) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("expected slice len %d, got %d: expected %v, got %v", len(expected), len(actual), expected, actual)
	}
	for i := range expected {
		if expected[i] != actual[i] {
			return fmt.Errorf("at index %d: expected %q, got %q", i, expected[i], actual[i])
		}
	}
	return nil
}

// AssertStringSliceEqualUnordered checks two string slices have the same elements.
func AssertStringSliceEqualUnordered(expected, actual []string) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("expected slice len %d, got %d: expected %v, got %v", len(expected), len(actual), expected, actual)
	}
	e := make([]string, len(expected))
	a := make([]string, len(actual))
	copy(e, expected)
	copy(a, actual)
	sort.Strings(e)
	sort.Strings(a)
	for i := range e {
		if e[i] != a[i] {
			return fmt.Errorf("sorted mismatch at %d: expected %q, got %q (expected: %v, got: %v)", i, e[i], a[i], e, a)
		}
	}
	return nil
}

// Key returns a prefixed key for test isolation.
func Key(prefix, name string) string {
	return prefix + name
}
