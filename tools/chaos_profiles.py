"""Explicit, reviewed coverage; never discover profile members by a filename glob."""
PROFILES = {
    "chaos-quick": (
        "bounded-eos", "commit-request-lost", "commit-response-lost",
        "broker-leader-kill-v1", "broker-leader-pause-v1", "broker-coordinator-pause-v1",
        "coordinator-commit-kill-v2", "protocol-endtxn-delay-v2",
        "protocol-endtxn-coordinator-v2", "protocol-produce-response-v2",
    ),
    "chaos-full": (
        "bounded-eos", "commit-request-lost", "commit-response-lost",
        "broker-eos-control-v1", "broker-eos-control-v2",
        "broker-eos-kill-v1", "broker-eos-kill-v2",
        "broker-leader-kill-v1", "broker-leader-kill-v2",
        "broker-leader-pause-v1", "broker-leader-pause-v2",
        "broker-coordinator-pause-v1", "broker-coordinator-pause-v2",
        "protocol-endtxn-request-v1", "protocol-endtxn-request-v2",
        "protocol-endtxn-delay-v1", "protocol-endtxn-delay-v2",
        "protocol-endtxn-coordinator-v1", "protocol-endtxn-coordinator-v2",
        "protocol-produce-response-v1", "protocol-produce-response-v2",
        "protocol-add-partitions-concurrent-v1",
        "protocol-produce-after-append-v1", "protocol-produce-after-append-v2",
        "coordinator-commit-kill-v1", "coordinator-commit-kill-v2",
        "coordinator-commit-pause-v1", "coordinator-commit-pause-v2",
    ),
}
