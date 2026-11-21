#!/usr/bin/env python3
"""Sync the canonical protocol.py (src/rao) into rda/ (self-contained deploy)."""
import os
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src = os.path.join(ROOT, "src", "rao", "protocol.py")
dst = os.path.join(ROOT, "src", "rao", "runtime", "protocol.py")
shutil.copyfile(src, dst)
print("synced %s -> %s" % (src, dst))
