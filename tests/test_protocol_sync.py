"""Drift guard: rda/protocol.py must be byte-identical to src/rao/protocol.py.

The RDA ships self-contained (rda.py + protocol.py deployed together), so the
copy in rda/ is generated. If this test fails, re-run scripts/sync_protocol.py.
"""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_protocol_copies_identical():
    src = open(os.path.join(ROOT, "src", "rao", "protocol.py"),
               "rb").read()
    rda = open(os.path.join(ROOT, "rda", "protocol.py"), "rb").read()
    assert src == rda, ("rda/protocol.py drifted from src/rao/protocol.py — "
                        "run: python scripts/sync_protocol.py")
