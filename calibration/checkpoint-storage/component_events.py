"""Independent component-event extraction from explicitly bounded output captures.

This module reads no files and does not infer a collection boundary from a later
retained log. Stack traces remain part of the raw log, not independent events.
"""
import hashlib
import json
import re

MAX_FILES = 128
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BYTES = 32 * 1024 * 1024
MAX_EVENTS = 512
MAX_LINE_UNITS = 8192
PROCESS = re.compile(r"(?:taskmanager|jobmanager)-[1-9][0-9]*#[1-9][0-9]*", re.ASCII)
HEADER = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[,.]\d{3})\s+"
                    r"(?:\[[^]]*]\s+)?(ERROR|WARN|INFO)\s+(\S+)\s+.*? - (.*)$", re.ASCII)
PRODUCER = re.compile(r"\bproducerId[= :]+(-?\d+)", re.ASCII)
EPOCH = re.compile(r"\b(?:producerEpoch|epoch)[= :]+(-?\d+)", re.ASCII)
TRANSACTION = re.compile(r'''\btransactionalId[= :]+(?:'([^']*)'|"([^"]*)"|([^,}\]\s]+))''', re.ASCII)
COMMITTER = "org.apache.flink.connector.kafka.sink.internal.KafkaCommitter"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def kinds(logger, message):
    found, lower = [], message.lower()
    if "already fenced" in lower or "ProducerFenced" in message:
        found.append("producer-fenced")
    if "InvalidPidMapping" in message:
        found.append("invalid-pid-mapping")
    if "InvalidTxnState" in message:
        found.append("invalid-txn-state")
    if "transaction" in lower and "aborted" in lower:
        found.append("transaction-aborted")
    if "transaction" in lower and "expired" in lower:
        found.append("transaction-expired")
    if logger == COMMITTER:
        if message.startswith("Encountered retriable exception while committing "):
            found.append("commit-retriable")
        elif not found and (message.startswith("Unable to commit transaction (")
                            or message.startswith("Transaction (") and "encountered error" in lower):
            found.append("commit-failed")
        elif message.startswith("Committing transaction (") and "was interrupted" in lower:
            found.append("commit-interrupted")
    return found


def event(process, line):
    match = HEADER.fullmatch(line)
    if not match:
        return None
    timestamp, level, logger, message = match.groups()
    if "kafka" not in logger.lower():
        return None
    recognized = kinds(logger, message)
    if not recognized:
        return None
    value = {"process": process, "timestamp": timestamp.replace(".", ","), "level": level,
             "logger": logger, "kinds": recognized, "message": message}
    for name, pattern in (("producerId", PRODUCER), ("epoch", EPOCH)):
        field = pattern.search(message)
        if field:
            value[name] = field.group(1)
    transaction = TRANSACTION.search(message)
    if transaction:
        value["transactionalId"] = next(item for item in transaction.groups() if item is not None)
    elif "commit-retriable" in recognized:
        beginning = "Encountered retriable exception while committing "
        if message.startswith(beginning) and message.endswith("."):
            value["transactionalId"] = message[len(beginning):-1]
    return value


def event_key(value):
    """Every reported field contributes; altered/extra fields cannot alias a raw event."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def extract(logs):
    """Parse ordered (physical process, exact captured bytes) pairs, with production limits.

    Diagnostics describe the checked prefix only. The caller must separately bind
    its byte count and hash to a production receipt before asserting completeness.
    """
    require(isinstance(logs, list) and 0 < len(logs) <= MAX_FILES,
            "Missing component captures or capture file budget exceeded")
    found, diagnostics, seen_processes, total = {}, [], set(), 0
    for process, data in logs:
        require(isinstance(process, str) and PROCESS.fullmatch(process), "Invalid component capture process")
        require(process not in seen_processes, "Duplicate component capture process")
        seen_processes.add(process)
        require(isinstance(data, bytes) and len(data) <= MAX_FILE_BYTES, "Component capture byte budget exceeded")
        total += len(data)
        require(total <= MAX_TOTAL_BYTES, "Component aggregate capture byte budget exceeded")
        # Invalid UTF-8 cannot silently change an event key during an independent audit.
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("Component capture contains invalid UTF-8") from None
        if text and not text.endswith("\n"):
            diagnostics.append(process + ": unfinished line omitted")
            text = text[:text.rfind("\n") + 1]
        # BufferedReader recognizes CR, LF and CRLF, not Unicode line separators.
        for line in re.split(r"\r\n|\r|\n", text):
            if len(line.encode("utf-16-le")) // 2 > MAX_LINE_UNITS:
                issue = process + ": oversized line omitted"
                if issue not in diagnostics:
                    diagnostics.append(issue)
                continue
            value = event(process, line)
            if value is None:
                continue
            key = event_key(value)
            if key not in found and len(found) == MAX_EVENTS:
                issue = "Component event retention limit exceeded"
                if issue not in diagnostics:
                    diagnostics.append(issue)
                continue
            found[key] = value
    return {"events": list(found.values()), "eventCount": len(found), "diagnostics": diagnostics}


def compare_events(reported, extracted):
    require(isinstance(reported, list) and len(reported) <= MAX_EVENTS, "Malformed component event list")
    keys = [event_key(value) for value in reported]
    require(len(keys) == len(set(keys)), "Duplicate reported component event")
    require(set(keys) == {event_key(value) for value in extracted},
            "Component event list is incomplete or differs from captured output")


class CoverageError(ValueError):
    def __init__(self, proof):
        self.component_event_audit = proof
        super().__init__("Component retained-prefix audit failed: " + "; ".join(proof["diagnostics"]))


def audit_retained(components, logs):
    """Compare all recognized events in the final retained prefixes with the report.

    Equality proves consistency with these hashed bytes only. The old production
    result does not identify its earlier read boundary, so a later event is a
    visible mismatch, never an excuse to guess a convenient earlier cutoff.
    """
    require(isinstance(components, dict) and isinstance(components.get("events"), list)
            and type(components.get("eventCount")) is int
            and components["eventCount"] == len(components["events"])
            and isinstance(components.get("diagnostics"), list)
            and all(isinstance(value, str) for value in components["diagnostics"])
            and components.get("coverage") in ("captured-prefix", "partial"),
            "Component event totals or coverage are malformed")
    proof = {"coverage": "retained-final-prefix", "originalCaptureBoundaryKnown": False,
             "completeProcessLifetime": False, "originalCaptureCoverage": components["coverage"],
             "reportedEventCount": components["eventCount"], "events": [], "eventCount": 0,
             "captures": [], "diagnostics": [], "complete": False, "consistent": False, "confirmed": False,
             "limitation": "Checks the entire hashed retained final prefix for every physical process; "
                           "does not establish the original parser read boundary or complete process lifetime."}
    issues = proof["diagnostics"]
    if components["coverage"] != "captured-prefix" or components["diagnostics"]:
        issues.append("Original component capture is partial: " + "; ".join(components["diagnostics"]))
    try:
        extracted = extract(logs)
        proof.update(events=extracted["events"], eventCount=extracted["eventCount"])
        issues.extend(extracted["diagnostics"])
        for process, data in logs:
            proof["captures"].append({"process": process, "bytes": len(data), "sha256": sha(data),
                                      "newlineTerminated": bool(data) and data.endswith(b"\n")})
            if not data:
                issues.append(process + ": empty retained output")
            if len(data) == MAX_FILE_BYTES:
                issues.append(process + ": retained output reaches capture byte limit")
        try:
            compare_events(components["events"], extracted["events"])
            proof["consistent"] = True
        except ValueError as failure:
            issues.append(str(failure))
    except ValueError as failure:
        issues.append(str(failure))
    proof["complete"] = not issues
    proof["confirmed"] = proof["complete"] and proof["consistent"]
    if issues:
        raise CoverageError(proof)
    return proof
