#!/usr/bin/env python3
"""Opt-in entry point; container operations belong to the testcontainers module."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parents[1] / "testcontainers"))
from checkpoint_storage_probe import main

if __name__ == "__main__":
    raise SystemExit(main())
