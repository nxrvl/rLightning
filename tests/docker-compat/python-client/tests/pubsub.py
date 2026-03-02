"""Category 9: Pub/Sub tests."""

import time
import threading
import redis as redis_lib
from .framework import register, run_test, key, assert_equal, assert_true


def test_pubsub(r, prefix):
    results = []
    cat = "pubsub"

    def t_subscribe_publish():
        ch = key(prefix, "ps:chan1")
        pub = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=True,
        )
        ps = r.pubsub()
        try:
            ps.subscribe(ch)
            # Consume subscription confirmation
            msg = ps.get_message(timeout=3)
            assert_true(msg is not None and msg["type"] == "subscribe", "should get subscribe confirmation")

            time.sleep(0.1)
            n = pub.publish(ch, "hello")
            assert_true(n >= 1, "at least 1 subscriber should receive")

            msg = ps.get_message(timeout=3)
            assert_true(msg is not None, "should receive message")
            assert_equal("hello", msg["data"])
            assert_equal(ch, msg["channel"])
        finally:
            ps.unsubscribe(ch)
            ps.close()
            pub.close()
    results.append(run_test("SUBSCRIBE_PUBLISH_UNSUBSCRIBE", cat, t_subscribe_publish))

    def t_psubscribe():
        pattern = key(prefix, "ps:pchan*")
        pub = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=True,
        )
        ps = r.pubsub()
        try:
            ps.psubscribe(pattern)
            msg = ps.get_message(timeout=3)
            assert_true(msg is not None and msg["type"] == "psubscribe", "should get psubscribe confirmation")

            time.sleep(0.1)
            pub.publish(key(prefix, "ps:pchan_test"), "pattern_msg")

            msg = ps.get_message(timeout=3)
            assert_true(msg is not None, "should receive pattern message")
            assert_equal("pattern_msg", msg["data"])
            assert_equal(pattern, msg["pattern"])
        finally:
            ps.punsubscribe(pattern)
            ps.close()
            pub.close()
    results.append(run_test("PSUBSCRIBE_pattern", cat, t_psubscribe))

    def t_multi_channel():
        ch1 = key(prefix, "ps:multi1")
        ch2 = key(prefix, "ps:multi2")
        pub = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=True,
        )
        ps = r.pubsub()
        try:
            ps.subscribe(ch1, ch2)
            # Consume both subscription confirmations
            ps.get_message(timeout=3)
            ps.get_message(timeout=3)

            time.sleep(0.1)
            pub.publish(ch1, "msg1")
            pub.publish(ch2, "msg2")

            msg1 = ps.get_message(timeout=3)
            assert_true(msg1 is not None, "should receive msg1")
            assert_equal("msg1", msg1["data"])

            msg2 = ps.get_message(timeout=3)
            assert_true(msg2 is not None, "should receive msg2")
            assert_equal("msg2", msg2["data"])
        finally:
            ps.unsubscribe(ch1, ch2)
            ps.close()
            pub.close()
    results.append(run_test("multiple_channel_subscription", cat, t_multi_channel))

    def t_ordering():
        ch = key(prefix, "ps:order")
        pub = redis_lib.Redis(
            host=r.connection_pool.connection_kwargs["host"],
            port=r.connection_pool.connection_kwargs["port"],
            password=r.connection_pool.connection_kwargs.get("password"),
            db=0, decode_responses=True,
        )
        ps = r.pubsub()
        try:
            ps.subscribe(ch)
            ps.get_message(timeout=3)

            time.sleep(0.1)
            for i in range(5):
                pub.publish(ch, f"msg_{i}")

            for i in range(5):
                msg = ps.get_message(timeout=3)
                assert_true(msg is not None, f"should receive msg_{i}")
                assert_equal(f"msg_{i}", msg["data"])
        finally:
            ps.unsubscribe(ch)
            ps.close()
            pub.close()
    results.append(run_test("message_ordering", cat, t_ordering))

    return results


register("pubsub", test_pubsub)
