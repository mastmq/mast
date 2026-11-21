"""Run the Eclipse Paho interoperability suite against a running mast.

usage: run.py <paho.mqtt.testing checkout> <v3|v5> [host] [port]

The suite's own entry points parse argv twice, once with getopt and once
with unittest.main, so a -p flag for one is an error for the other. Importing
the module and setting its globals is the only way to point it at a port
other than 1883 that works for both client_test.py and client_test5.py.

Tests mast is known to fail live in EXPECTED below, each with the reason.
They run every time and are reported as expected failures, so the gap stays
visible; one that starts passing fails the run as an unexpected success, so
the list cannot go stale silently.
"""

import functools
import logging
import signal
import sys
import unittest

# Several tests wait in an unbounded loop for messages to arrive, and the
# suite's client kills its receive thread on any packet it cannot parse, so
# one bad packet hangs the whole run with nothing in the log. Each test gets
# a deadline instead; the slowest passing one (will delay) takes about 20s.
#
# The suite also shares two clients and the broker's retained messages across
# every test and cleans up only once, before the first. A test that fails
# half-way leaves retained messages and a dead client behind, and the next
# three fail for it. So each test starts from the suite's own clean-up, which
# costs a couple of seconds a test and makes every result its own.
DEADLINE = 60

# Each entry is a broker gap with an issue, or a place where the suite
# assumes something mast is not wrong to do differently. Keep the reason next
# to the name, and remove the entry in the change that fixes it.
DOLLAR = "wildcards match $ topics over NATS, mastmq/mast#35"
ACK_PROPS = (
    "PUBACK echoes the publisher's user properties, which the suite's client "
    "cannot parse, mastmq/mast#39"
)
EXPECTED = {
    "v3": {
        "test_dollar_topics": DOLLAR,
    },
    "v5": {
        "test_dollar_topics": DOLLAR,
        "test_user_properties": ACK_PROPS,
        "test_retained_message": ACK_PROPS,
        "test_publication_expiry": (
            "an expired message still reaches a reconnecting session, "
            "mastmq/mast#36"
        ),
        "test_subscribe_options": "No Local is ignored, mastmq/mast#37",
        "test_will_delay": "Will Delay Interval is ignored, mastmq/mast#38",
        # Not a gap. mast refuses the filter (the policy service in authz.py
        # denies it) with reason 0x87, Not authorized, which MQTT 5 defines
        # for exactly this. The suite's packet parser still allows only the
        # MQTT 3.1.1 codes 0, 1, 2 and 0x80 in a v5 SUBACK and drops the
        # connection. The v3 run checks the same refusal and passes.
        "test_subscribe_failure": "the suite rejects v5 SUBACK reason 0x87",
        # Not a gap. Server Keep Alive is optional; the suite expects the
        # reference broker's own setting, which caps a 120s keep-alive at 60.
        # mast accepts the keep-alive the client asks for.
        "test_server_keep_alive": "the suite expects a 60s server cap",
    },
}

# Tests that are not run at all, because their outcome depends on timing
# rather than on the broker.
SKIP = {
    "v3": {},
    "v5": {
        # mast does what the test wants: 1024 PUBRECs, then DISCONNECT with
        # 0x93, Receive Maximum exceeded, then it closes the socket. The
        # suite's client answers the buffered PUBRECs with PUBRELs before it
        # reads the DISCONNECT, and its write to the closed socket raises
        # BrokenPipeError. Whether it does depends on how fast the close
        # lands, so as an expected failure it would flake both ways.
        "test_flow_control2": (
            "the suite writes PUBRELs to a socket the broker closed after "
            "DISCONNECT 0x93"
        ),
    },
}


def with_deadline(test):
    @functools.wraps(test)
    def run(self):
        def expire(signum, frame):
            raise TimeoutError(f"{test.__name__} ran past {DEADLINE}s")

        previous = signal.signal(signal.SIGALRM, expire)
        signal.alarm(DEADLINE)
        try:
            type(self).setUpClass()
            return test(self)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous)

    return run


def main() -> int:
    suite_dir, version = sys.argv[1], sys.argv[2]
    host = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
    port = int(sys.argv[4]) if len(sys.argv) > 4 else 1883

    sys.path.insert(0, f"{suite_dir}/interoperability")
    if version == "v3":
        import client_test as mod
    else:
        import client_test5 as mod

    mod.host, mod.port = host, port
    mod.topics = ("TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA")
    mod.wildtopics = ("TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#")
    mod.nosubscribe_topics = ("test/nosubscribe",)
    # client_test5.py sets this only in its __main__ block, and its shared
    # subscriptions test reads it.
    mod.topic_prefix = "client_test5/"
    logging.getLogger().setLevel(logging.ERROR)

    for name in unittest.defaultTestLoader.getTestCaseNames(mod.Test):
        setattr(mod.Test, name, with_deadline(getattr(mod.Test, name)))

    expected = EXPECTED[version]
    for name, reason in expected.items():
        method = getattr(mod.Test, name)
        setattr(mod.Test, name, unittest.expectedFailure(method))
        print(f"expected failure: {name}: {reason}")

    for name, reason in SKIP[version].items():
        setattr(mod.Test, name, unittest.skip(reason)(getattr(mod.Test, name)))
        print(f"skipped: {name}: {reason}")

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(mod.Test)
    result = unittest.TextTestRunner(verbosity=2).run(suite)

    passed = max(
        0,
        result.testsRun
        - len(result.failures)
        - len(result.errors)
        - len(result.skipped)
        - len(result.expectedFailures)
        - len(result.unexpectedSuccesses),
    )
    print(
        f"\n{version}: {result.testsRun} run, {passed} passed, "
        f"{len(result.expectedFailures)} expected failures, "
        f"{len(result.skipped)} skipped, "
        f"{len(result.failures) + len(result.errors)} failed, "
        f"{len(result.unexpectedSuccesses)} unexpectedly passed"
    )
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
