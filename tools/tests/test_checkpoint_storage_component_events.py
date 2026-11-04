"""Generated component-prefix omission regressions; no Docker or retained runs."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).absolute().parents[2] / "calibration/checkpoint-storage"


def load(name):
    spec = importlib.util.spec_from_file_location("component_event_test_" + name, HERE / (name + ".py"))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


parser, check = load("component_events"), load("check")
PROCESS = "taskmanager-1#2"
STAMP = "2026-10-27 12:56:07,793"
LOGGER = "org.apache.kafka.clients.producer.internals.TransactionManager"
MESSAGE = "ProducerFencedException transactionalId='sink-0-2', producerId=12 epoch=0"
EVENT = {"process": PROCESS, "timestamp": STAMP, "level": "INFO", "logger": LOGGER,
         "producerId": "12", "epoch": "0", "transactionalId": "sink-0-2",
         "kinds": ["producer-fenced"], "message": MESSAGE}


def line(message=MESSAGE, *, timestamp=STAMP, level="INFO", logger=LOGGER, thread=""):
    return f"{timestamp} {thread}{level} {logger} [] - {message}\n".encode()


def reported(events):
    return {"eventCount": len(events), "events": deepcopy(events), "coverage": "captured-prefix", "diagnostics": []}


def fixture(directory):
    processes = ("jobmanager-1#1", "taskmanager-1#1", PROCESS)
    source = "/generated/connector-sha.jar"
    evidence = {"subjectClasses": {"expectedSource": source,
                    "processes": [{"process": name} for name in processes]},
                "flinkRuntime": {"components": [{"logicalName": name.split("#")[0],
                                                  "classLoadProcess": name} for name in processes]},
                "componentErrors": reported([EVENT]), "flinkRest": {"errorCount": 0, "errors": []}}
    files = []
    for name in processes:
        for kind in ("class-load", "output"):
            local = f"logs/flink-stability-{kind}-{name.replace('#', '-')}.log"
            data = (b"startup complete\n" if kind == "output" else
                    "".join("[class,load] " + entry + " source: file:" + source + "\n"
                            for entry in check.state.ENTRY_CLASSES).encode())
            if kind == "output" and name == PROCESS:
                data += line()
            path = directory / local
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            files.append({"sourcePath": "/generated/" + local, "localPath": local,
                          "sha256": check.digest(data), "kind": "log"})
    (directory / "retained-evidence.json").write_text(json.dumps({"version": 1, "files": files}))
    return {"evidence": evidence}, check.audit.RetainedEvidence(directory)


class ComponentEventsTest(unittest.TestCase):
    def test_exact_event_fields_and_multiline_stack_trace(self):
        output = line(thread="[AsyncSinkWriter] ", timestamp=STAMP.replace(",", "."))
        output += (b"org.apache.kafka.common.errors.ProducerFencedException: stack trace\n"
                   b"\tat generated.Frame.run(Frame.java:1)\n")
        output += line("transaction expired", logger="org.example.Other")
        proof = parser.audit_retained(reported([EVENT]), [(PROCESS, output)])
        self.assertEqual([EVENT], proof["events"])
        self.assertEqual("retained-final-prefix", proof["coverage"])
        self.assertFalse(proof["originalCaptureBoundaryKnown"])
        self.assertFalse(proof["completeProcessLifetime"])
        self.assertTrue(proof["complete"])
        self.assertEqual({"process": PROCESS, "bytes": len(output), "sha256": parser.sha(output),
                          "newlineTerminated": True}, proof["captures"][0])

    def test_omission_of_all_or_one_event_is_not_vacuous_success(self):
        second = dict(EVENT, level="ERROR", logger=parser.COMMITTER)
        logs = [(PROCESS, line() + line(level="ERROR", logger=parser.COMMITTER))]
        for events in ([], [EVENT], [second]):
            with self.subTest(events=events), self.assertRaises(parser.CoverageError) as caught:
                parser.audit_retained(reported(events), logs)
            self.assertEqual([EVENT, second], caught.exception.component_event_audit["events"])
            self.assertFalse(caught.exception.component_event_audit["consistent"])

    def test_reported_duplicates_rejected_but_identical_raw_copies_deduplicated(self):
        proof = parser.audit_retained(reported([EVENT]), [(PROCESS, line() * 3)])
        self.assertEqual(1, proof["eventCount"])
        with self.assertRaisesRegex(parser.CoverageError, "Duplicate reported"):
            parser.audit_retained(reported([EVENT, EVENT]), [(PROCESS, line() * 3)])

    def test_every_field_and_optional_field_presence_is_bound_to_raw_header(self):
        changes = {"timestamp": "2026-10-27 12:56:07,794", "level": "ERROR", "logger": parser.COMMITTER,
                   "process": "taskmanager-1#1", "producerId": "13", "epoch": "1", "transactionalId": "other",
                   "kinds": ["commit-failed"], "message": MESSAGE + " altered", "extra": "invented"}
        for field, value in changes.items():
            with self.subTest(field=field), self.assertRaises(parser.CoverageError):
                parser.audit_retained(reported([dict(EVENT, **{field: value})]), [(PROCESS, line())])
        for field in ("producerId", "epoch", "transactionalId", "message"):
            changed = dict(EVENT); changed.pop(field)
            with self.subTest(removed=field), self.assertRaises(parser.CoverageError):
                parser.audit_retained(reported([changed]), [(PROCESS, line())])

    def test_distinct_severity_logger_and_incarnation_are_independent_events(self):
        second = dict(EVENT, level="ERROR", logger=parser.COMMITTER)
        third = dict(EVENT, process="taskmanager-1#1")
        logs = [(PROCESS, line() + line(level="ERROR", logger=parser.COMMITTER)), (third["process"], line())]
        self.assertEqual(3, parser.audit_retained(reported([third, second, EVENT]), logs)["eventCount"])

    def test_all_recognized_kinds_and_retry_transaction_fallback(self):
        rows = [("InvalidPidMappingException InvalidTxnStateException transaction aborted and expired",
                 ["invalid-pid-mapping", "invalid-txn-state", "transaction-aborted", "transaction-expired"], None),
                ("Encountered retriable exception while committing sink-0-3.", ["commit-retriable"], "sink-0-3"),
                ("Unable to commit transaction (request) unknown producer.", ["commit-failed"], None),
                ("Transaction (request) encountered error.", ["commit-failed"], None),
                ("Committing transaction (request) was interrupted.", ["commit-interrupted"], None)]
        expected, raw = [], b""
        for message, recognized, transaction in rows:
            raw += line(message, level="WARN", logger=parser.COMMITTER)
            value = {"process": PROCESS, "timestamp": STAMP, "level": "WARN", "logger": parser.COMMITTER,
                     "message": message, "kinds": recognized}
            if transaction is not None:
                value["transactionalId"] = transaction
            expected.append(value)
        self.assertEqual(expected, parser.audit_retained(reported(expected), [(PROCESS, raw)])["events"])

    def test_transaction_forms_and_negative_producer_fields(self):
        for transaction in ("'quoted-id'", '"quoted-id"', "quoted-id"):
            message = f"ProducerFencedException transactionalId={transaction}, producerId=-12 producerEpoch=-1"
            expected = dict(EVENT, message=message, transactionalId="quoted-id", producerId="-12", epoch="-1")
            with self.subTest(transaction=transaction):
                self.assertEqual([expected], parser.audit_retained(reported([expected]), [(PROCESS, line(message))])["events"])

    def test_partial_original_capture_or_final_unterminated_log_remains_a_gap(self):
        for change in ("partial", "diagnostics", "unfinished", "empty", "oversized", "utf8"):
            component, raw = reported([EVENT]), line()
            if change == "partial": component["coverage"] = "partial"
            elif change == "diagnostics": component["diagnostics"] = [PROCESS + ": output capture truncated"]
            elif change == "unfinished": raw += b"unfinished tail"
            elif change == "empty": raw = b""
            elif change == "oversized": raw += ("\U0001f600" * 4097 + "\n").encode()
            else: raw += b"\xff\n"
            with self.subTest(change=change), self.assertRaises(parser.CoverageError) as caught:
                parser.audit_retained(component, [(PROCESS, raw)])
            self.assertFalse(caught.exception.component_event_audit["complete"])
            self.assertTrue(caught.exception.component_event_audit["diagnostics"])

    def test_late_recognized_event_is_mismatch_not_an_inferred_earlier_cutoff(self):
        late = line("InvalidTxnStateException", timestamp="2026-10-27 12:56:08,999")
        with self.assertRaises(parser.CoverageError) as caught:
            parser.audit_retained(reported([EVENT]), [(PROCESS, line() + late)])
        self.assertEqual(2, caught.exception.component_event_audit["eventCount"])
        self.assertFalse(caught.exception.component_event_audit["originalCaptureBoundaryKnown"])

    def test_unterminated_event_and_missing_inventory_cannot_prove_empty_event_set(self):
        for logs in ([], [(PROCESS, line().rstrip(b"\n"))]):
            with self.subTest(logs=logs), self.assertRaises(parser.CoverageError):
                parser.audit_retained(reported([]), logs)

    def test_byte_and_event_limits_are_explicit_gaps(self):
        at_limit = b"x\n" * (parser.MAX_FILE_BYTES // 2)
        with self.assertRaisesRegex(parser.CoverageError, "capture byte limit"):
            parser.audit_retained(reported([]), [(PROCESS, at_limit)])
        raw = b"".join(line(f"ProducerFencedException producerId={number}") for number in range(513))
        with self.assertRaises(parser.CoverageError) as caught:
            parser.audit_retained(reported([]), [(PROCESS, raw)])
        self.assertEqual(512, caught.exception.component_event_audit["eventCount"])
        self.assertIn("Component event retention limit exceeded", caught.exception.component_event_audit["diagnostics"])


class RawEvidenceIntegrationTest(unittest.TestCase):
    def test_all_physical_outputs_are_hashed_even_when_they_report_no_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve(); result, retained = fixture(directory)
            proof = check.raw_subject_and_events(directory, result, retained)
            self.assertEqual(3, len(proof["classLogSha256"]))
            self.assertEqual(3, len(proof["componentEventAudit"]["captures"]))
            result["evidence"]["componentErrors"] = reported([])
            with self.assertRaisesRegex(ValueError, "incomplete or differs"):
                check.raw_subject_and_events(directory, result, retained)

    def test_missing_unknown_duplicate_process_and_changed_raw_bytes_reject(self):
        for change in ("missing-output", "unknown-output", "missing-origin", "duplicate-process", "bytes"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve(); result, retained = fixture(directory)
                local = "logs/flink-stability-output-jobmanager-1-1.log"
                if change == "missing-output": retained.local.pop(local)
                elif change == "unknown-output": retained.local["logs/flink-stability-output-taskmanager-2-1.log"] = retained.local[local]
                elif change == "missing-origin": result["evidence"]["subjectClasses"]["processes"].pop(0)
                elif change == "duplicate-process":
                    result["evidence"]["flinkRuntime"]["components"].append(result["evidence"]["flinkRuntime"]["components"][0])
                else: (directory / local).write_bytes(b"changed\n")
                with self.assertRaises(ValueError): check.raw_subject_and_events(directory, result, retained)


if __name__ == "__main__":
    unittest.main()
