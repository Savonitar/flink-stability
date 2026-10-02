package org.savonitar.flink.stability.cli;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.savonitar.flink.stability.core.execution.KafkaLogEvidence;

/** Explicit receipt projection; no segment payloads are duplicated into the run summary. */
final class KafkaLogEvidenceRenderer {
    private KafkaLogEvidenceRenderer() {}
    static void render(ObjectNode node, KafkaLogEvidence evidence) {
        node.put("status", evidence.status());
        var diagnostics = node.putArray("diagnostics");
        evidence.diagnostics().forEach(diagnostics::add);
        evidence.capture().ifPresent(capture -> {
            node.put("retainedBytes", capture.retainedBytes()).put("reservedDockerCalls", capture.reservedCalls());
            var inventories = node.putArray("inventories");
            capture.inventories().forEach(inventory -> {
                var item = inventories.addObject().put("topic", inventory.partition().topic())
                        .put("partition", inventory.partition().partition()).put("directory", inventory.directory())
                        .put("status", inventory.status()).put("workerFinished", inventory.workerFinished())
                        .put("transcript", inventory.transcript().toString()).put("transcriptSha256", inventory.transcriptSha256())
                        .put("containerId", inventory.containerId()).put("imageId", inventory.imageId())
                        .put("networkId", inventory.networkId()).put("generation", inventory.generation());
                var files = item.putArray("files");
                inventory.files().forEach(file -> files.addObject().put("basename", file.basename())
                        .put("bytes", file.bytes()).put("modifiedSeconds", file.modifiedSeconds())
                        .put("inode", file.inode()).put("log", file.log()));
            });
            var archives = node.putArray("archives");
            capture.archives().forEach(archive -> {
                var item = archives.addObject().put("topic", archive.partition().topic())
                        .put("partition", archive.partition().partition()).put("basename", archive.basename())
                        .put("containerId", archive.containerId()).put("imageId", archive.imageId())
                        .put("networkId", archive.networkId()).put("generation", archive.generation())
                        .put("path", archive.path().toString()).put("status", archive.status()).put("bytes", archive.bytes())
                        .put("workerFinished", archive.workerFinished()).put("detail", archive.detail());
                archive.sha256().ifPresent(hash -> item.put("sha256", hash));
            });
        });
        evidence.transactions().ifPresent(receipt -> {
            var item = node.putObject("transactions").put("status", receipt.status());
            receipt.evidence().ifPresent(path -> item.put("evidence", path.toString()));
            receipt.sha256().ifPresent(hash -> item.put("sha256", hash));
        });
        var decoded = node.putArray("decoded");
        evidence.decoded().forEach(receipt -> {
            var item = decoded.addObject().put("archive", receipt.archive().toString()).put("status", receipt.status())
                    .put("records", receipt.records()).put("batches", receipt.batches());
            receipt.evidence().ifPresent(path -> item.put("evidence", path.toString()));
            receipt.sha256().ifPresent(hash -> item.put("sha256", hash));
        });
    }
}
