"""RDA auto-provisioning (v0.5): ensure the remote runtime files match local
before dispatch. Eliminates the manual deploy_rda.py step."""
import hashlib
import os

RDA_FILES = ("rda.py", "protocol.py", "rda_daemon.py")


def runtime_dir():
    import rao
    return os.path.join(os.path.dirname(rao.__file__), "runtime")


def local_hashes():
    base = runtime_dir()
    out = {}
    for name in RDA_FILES:
        path = os.path.join(base, name)
        with open(path, "rb") as fh:
            out[name] = hashlib.sha256(fh.read()).hexdigest()
    # v0.10: combined runtime hash (matches daemon.json codeHash, which the
    # daemon computes over ALL runtime files — any single-file change must
    # trigger a daemon restart)
    h = hashlib.sha256()
    for name in sorted(RDA_FILES):
        path = os.path.join(base, name)
        h.update(name.encode())
        with open(path, "rb") as fh:
            h.update(fh.read())
    out["__combined__"] = h.hexdigest()
    return out


def remote_hashes(transport, device, rda_dir):
    """Query remote file hashes. Returns dict {name: hash_or_None}."""
    checks = " && ".join(
        'sha256sum "%s/%s" 2>/dev/null | cut -d" " -f1' % (rda_dir, f)
        for f in RDA_FILES)
    rc, out = transport._exec(
        device, "cd / 2>/dev/null; %s" % checks, timeout_s=15)
    lines = [l.strip() for l in out.strip().splitlines() if l.strip()]
    result = {}
    for i, name in enumerate(RDA_FILES):
        result[name] = lines[i] if rc == 0 and i < len(lines) and \
            len(lines[i]) == 64 else None
    return result


def ensure_rda(transport, device, rda_dir):
    """Probe remote RDA files; upload if missing or stale. Returns dict."""
    local = local_hashes()
    try:
        remote = remote_hashes(transport, device, rda_dir)
    except Exception as exc:
        return {"deployed": False, "checked": False, "error": str(exc)}
    stale = [name for name in RDA_FILES if remote.get(name) != local[name]]
    if not stale:
        return {"deployed": False, "checked": True, "files": list(RDA_FILES)}
    uploaded = transport._put_files(device, rda_dir,
                                    {n: os.path.join(runtime_dir(), n)
                                     for n in stale})
    return {"deployed": True, "checked": True,
            "uploaded": uploaded, "stale": stale}
