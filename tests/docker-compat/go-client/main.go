package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"compat-test-go/tests"
)

type ClientInfo struct {
	Language string `json:"language"`
	Library  string `json:"library"`
	Version  string `json:"version"`
}

type ServerInfo struct {
	Host    string `json:"host"`
	Port    int    `json:"port"`
	Type    string `json:"type"`
	Version string `json:"version,omitempty"`
}

type CategorySummary struct {
	Total    int     `json:"total"`
	Passed   int     `json:"passed"`
	PassRate float64 `json:"pass_rate"`
}

type Summary struct {
	Total      int                        `json:"total"`
	Passed     int                        `json:"passed"`
	Failed     int                        `json:"failed"`
	Skipped    int                        `json:"skipped"`
	Errored    int                        `json:"errored"`
	PassRate   float64                    `json:"pass_rate"`
	Categories map[string]CategorySummary `json:"categories"`
}

type TestReport struct {
	Client     ClientInfo       `json:"client"`
	Server     ServerInfo       `json:"server"`
	Timestamp  string           `json:"timestamp"`
	DurationMs int64            `json:"duration_ms"`
	Results    []tests.TestResult `json:"results"`
	Summary    Summary          `json:"summary"`
}

func main() {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	password := os.Getenv("REDIS_PASSWORD")
	prefix := os.Getenv("TEST_PREFIX")
	if prefix == "" {
		prefix = "gotest:"
	}

	addr := fmt.Sprintf("%s:%s", host, port)
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	defer rdb.Close()

	ctx := context.Background()

	// Wait for server to be ready
	for i := 0; i < 30; i++ {
		if err := rdb.Ping(ctx).Err(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to Redis at %s: %v\n", addr, err)
		os.Exit(1)
	}

	// Detect server type
	serverType := "unknown"
	serverVersion := ""
	info, err := rdb.Info(ctx, "server").Result()
	if err == nil {
		for _, line := range strings.Split(info, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "redis_version:") {
				serverVersion = strings.TrimPrefix(line, "redis_version:")
			}
		}
		if strings.Contains(info, "rlightning") || strings.Contains(info, "rLightning") {
			serverType = "rlightning"
		} else {
			serverType = "redis"
		}
	}

	fmt.Fprintf(os.Stderr, "Connected to %s at %s (version: %s)\n", serverType, addr, serverVersion)
	fmt.Fprintf(os.Stderr, "Running tests with prefix: %s\n", prefix)

	startTime := time.Now()
	results := tests.RunAll(ctx, rdb, prefix)
	totalDuration := time.Since(startTime).Milliseconds()

	// Build summary
	summary := Summary{
		Categories: make(map[string]CategorySummary),
	}
	catTotals := make(map[string]int)
	catPassed := make(map[string]int)

	for _, r := range results {
		summary.Total++
		catTotals[r.Category]++
		switch r.Status {
		case "pass":
			summary.Passed++
			catPassed[r.Category]++
		case "fail":
			summary.Failed++
		case "skip":
			summary.Skipped++
		case "error":
			summary.Errored++
		}
	}

	if summary.Total > 0 {
		summary.PassRate = float64(summary.Passed) / float64(summary.Total) * 100
	}
	for cat, total := range catTotals {
		passed := catPassed[cat]
		rate := float64(0)
		if total > 0 {
			rate = float64(passed) / float64(total) * 100
		}
		summary.Categories[cat] = CategorySummary{Total: total, Passed: passed, PassRate: rate}
	}

	portNum := 6379
	fmt.Sscanf(port, "%d", &portNum)

	report := TestReport{
		Client: ClientInfo{
			Language: "go",
			Library:  "go-redis/v9",
			Version:  "v9.5.1",
		},
		Server: ServerInfo{
			Host:    host,
			Port:    portNum,
			Type:    serverType,
			Version: serverVersion,
		},
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		DurationMs: totalDuration,
		Results:    results,
		Summary:    summary,
	}

	out, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal report: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(out))

	// Print summary to stderr
	fmt.Fprintf(os.Stderr, "\n=== Test Summary ===\n")
	fmt.Fprintf(os.Stderr, "Total: %d | Pass: %d | Fail: %d | Skip: %d | Error: %d | Rate: %.1f%%\n",
		summary.Total, summary.Passed, summary.Failed, summary.Skipped, summary.Errored, summary.PassRate)
	for cat, cs := range summary.Categories {
		fmt.Fprintf(os.Stderr, "  %-20s %d/%d (%.1f%%)\n", cat, cs.Passed, cs.Total, cs.PassRate)
	}
}
