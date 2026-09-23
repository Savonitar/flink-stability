package org.savonitar.flink.stability.cli;

/** Raised when a validated catalog does not contain the explicitly selected target. */
final class UnknownSpecificationTargetException extends IllegalArgumentException {
    UnknownSpecificationTargetException(String message) {
        super(message);
    }
}
