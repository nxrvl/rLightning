package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("pubsub", testPubSub)
}

func testPubSub(ctx context.Context, rdb *redis.Client, prefix string) []TestResult {
	var results []TestResult
	cat := "pubsub"

	results = append(results, RunTest("SUBSCRIBE_PUBLISH_UNSUBSCRIBE", cat, func() error {
		ch := Key(prefix, "ps:chan1")
		sub := rdb.Subscribe(ctx, ch)
		defer sub.Close()

		// Wait for subscription confirmation
		_, err := sub.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			return fmt.Errorf("subscribe confirmation: %v", err)
		}

		// Use separate client to publish
		pub := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
		})
		defer pub.Close()

		time.Sleep(100 * time.Millisecond) // let subscription settle
		n, err := pub.Publish(ctx, ch, "hello").Result()
		if err != nil {
			return err
		}
		if err := AssertTrue(n >= 1, "at least 1 subscriber should receive"); err != nil {
			return err
		}

		msg, err := sub.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			return fmt.Errorf("receive message: %v", err)
		}
		m, ok := msg.(*redis.Message)
		if !ok {
			return fmt.Errorf("expected *redis.Message, got %T", msg)
		}
		if err := AssertEqual("hello", m.Payload); err != nil {
			return err
		}
		return AssertEqual(ch, m.Channel)
	}))

	results = append(results, RunTest("PSUBSCRIBE_pattern", cat, func() error {
		pattern := Key(prefix, "ps:pchan*")
		sub := rdb.PSubscribe(ctx, pattern)
		defer sub.Close()

		_, err := sub.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			return fmt.Errorf("psubscribe confirmation: %v", err)
		}

		pub := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
		})
		defer pub.Close()

		time.Sleep(100 * time.Millisecond)
		pub.Publish(ctx, Key(prefix, "ps:pchan_test"), "pattern_msg")

		msg, err := sub.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			return fmt.Errorf("receive pattern message: %v", err)
		}
		m, ok := msg.(*redis.Message)
		if !ok {
			return fmt.Errorf("expected *redis.Message, got %T", msg)
		}
		if err := AssertEqual("pattern_msg", m.Payload); err != nil {
			return err
		}
		return AssertEqual(pattern, m.Pattern)
	}))

	results = append(results, RunTest("multiple_channel_subscription", cat, func() error {
		ch1 := Key(prefix, "ps:multi1")
		ch2 := Key(prefix, "ps:multi2")
		sub := rdb.Subscribe(ctx, ch1, ch2)
		defer sub.Close()

		// Receive both subscription confirmations
		sub.ReceiveTimeout(ctx, 3*time.Second)
		sub.ReceiveTimeout(ctx, 3*time.Second)

		pub := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
		})
		defer pub.Close()

		time.Sleep(100 * time.Millisecond)
		pub.Publish(ctx, ch1, "msg1")
		pub.Publish(ctx, ch2, "msg2")

		msg1, err := sub.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			return fmt.Errorf("receive msg1: %v", err)
		}
		m1, ok := msg1.(*redis.Message)
		if !ok {
			return fmt.Errorf("expected Message, got %T", msg1)
		}
		if err := AssertEqual("msg1", m1.Payload); err != nil {
			return err
		}

		msg2, err := sub.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			return fmt.Errorf("receive msg2: %v", err)
		}
		m2, ok := msg2.(*redis.Message)
		if !ok {
			return fmt.Errorf("expected Message, got %T", msg2)
		}
		return AssertEqual("msg2", m2.Payload)
	}))

	results = append(results, RunTest("message_ordering", cat, func() error {
		ch := Key(prefix, "ps:order")
		sub := rdb.Subscribe(ctx, ch)
		defer sub.Close()

		_, err := sub.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			return fmt.Errorf("subscribe: %v", err)
		}

		pub := redis.NewClient(&redis.Options{
			Addr:     rdb.Options().Addr,
			Password: rdb.Options().Password,
		})
		defer pub.Close()

		time.Sleep(100 * time.Millisecond)
		for i := 0; i < 5; i++ {
			pub.Publish(ctx, ch, fmt.Sprintf("msg_%d", i))
		}

		for i := 0; i < 5; i++ {
			msg, err := sub.ReceiveTimeout(ctx, 3*time.Second)
			if err != nil {
				return fmt.Errorf("receive msg_%d: %v", i, err)
			}
			m, ok := msg.(*redis.Message)
			if !ok {
				return fmt.Errorf("expected Message for msg_%d, got %T", i, msg)
			}
			expected := fmt.Sprintf("msg_%d", i)
			if err := AssertEqual(expected, m.Payload); err != nil {
				return err
			}
		}
		return nil
	}))

	return results
}
