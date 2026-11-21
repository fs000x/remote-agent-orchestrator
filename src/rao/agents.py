"""Agent detection + plugin injection with backup/merge/rollback (v0.5).

Supported hosts (v1): zcode, claude. Codex: detector stub only (P3).

Injection contract per host:
  - MCP registration: JSON config file, backup -> merge -> atomic write
  - Skill: copy packaged SKILL.md into the host's skills directory
  - manifest at state_dir/manifest.json enables `rao uninstall` rollback
"""
import json
import os
import shutil
import sys
import time

MCP_SERVER_NAME = "remote-agent-orchestrator"
ENTRY_NOTE = "installed by rao"


def state_dir():
    return os.environ.get("RAO_STATE_DIR") or os.path.join(
        os.path.expanduser("~"), ".rao")


def skill_source_path():
    import rao
    return os.path.join(os.path.dirname(rao.__file__), "data", "skills",
                        "cross-device-orchestrator", "SKILL.md")


def _env_or(env_key, default):
    return os.environ.get(env_key) or default


class AgentDef:
    def __init__(self, name, cli, config_path, skills_dir, mcp_key_path):
        self.name = name
        self.cli = cli                    # CLI name on PATH (may be None)
        self.config_path = config_path    # JSON config file
        self.skills_dir = skills_dir      # skills root dir
        self.mcp_key_path = mcp_key_path  # e.g. ["mcp","servers"] / ["mcpServers"]

    def entry_get(self, cfg):
        node = cfg
        for k in self.mcp_key_path:
            node = node.setdefault(k, {})
        return node

    def entry_remove(self, cfg):
        node = cfg
        for k in self.mcp_key_path[:-1]:
            node = node.get(k, {})
        node.pop(self.mcp_key_path[-1], None)


def get_agent_def(name):
    home = os.path.expanduser("~")
    if name == "zcode":
        return AgentDef(
            "zcode", "zcode",
            _env_or("RAO_ZCODE_CONFIG",
                    os.path.join(home, ".zcode", "cli", "config.json")),
            _env_or("RAO_ZCODE_SKILLS",
                    os.path.join(home, ".zcode", "skills")),
            ["mcp", "servers"])
    if name == "claude":
        return AgentDef(
            "claude", "claude",
            _env_or("RAO_CLAUDE_CONFIG", os.path.join(home, ".claude.json")),
            _env_or("RAO_CLAUDE_SKILLS", os.path.join(home, ".claude", "skills")),
            ["mcpServers"])
    raise KeyError("unsupported agent: %s" % name)


SUPPORTED_AGENTS = ("zcode", "claude")


def detect_agents():
    """Detect installed agent hosts. Returns list of dicts (never raises)."""
    out = []
    for name in SUPPORTED_AGENTS:
        d = get_agent_def(name)
        cli_path = shutil.which(d.cli) if d.cli else None
        config_exists = os.path.isfile(d.config_path)
        dir_exists = os.path.isdir(os.path.dirname(d.config_path)) or \
            os.path.isdir(d.skills_dir)
        installed = bool(cli_path) or config_exists or dir_exists
        entry = {"name": name, "installed": installed,
                 "cli_path": cli_path, "config_path": d.config_path,
                 "injected": is_injected(name)}
        if name == "zcode":
            # installed desktop app ships the CLI outside PATH
            for cand in (r"C:\Program Files\ZCode\resources\glm\zcode.cjs",
                         os.path.join(home_n(), ".zcode", "cli")):
                if os.path.exists(cand):
                    entry["cli_path"] = entry["cli_path"] or cand
                    installed = True
                    entry["installed"] = True
                    break
        out.append(entry)
    return out


def home_n():
    return os.path.expanduser("~")


def is_injected(name):
    d = get_agent_def(name)
    try:
        with open(d.config_path, encoding="utf-8") as fh:
            cfg = json.load(fh)
        node = cfg
        for k in d.mcp_key_path:
            node = node.get(k, {}) if isinstance(node, dict) else {}
        return MCP_SERVER_NAME in node
    except Exception:
        return False


def _mcp_entry():
    return {"command": sys.executable, "args": ["-m", "rao.server"],
            "_note": ENTRY_NOTE}


def _atomic_write(path, data_bytes):
    tmp = path + ".rao-tmp"
    with open(tmp, "wb") as fh:
        fh.write(data_bytes)
    os.replace(tmp, path)


def inject_agent(name, assume_yes=True):
    """Backup -> merge -> write. Idempotent. Returns result dict."""
    d = get_agent_def(name)
    result = {"agent": name, "config": d.config_path, "actions": []}
    # backup
    backups = os.path.join(state_dir(), "backups")
    os.makedirs(backups, exist_ok=True)
    backup_path = None
    if os.path.isfile(d.config_path):
        backup_path = os.path.join(
            backups, "%s-%d-%s" % (name, int(time.time()),
                                   os.path.basename(d.config_path)))
        shutil.copy2(d.config_path, backup_path)
        result["actions"].append("backup -> %s" % backup_path)
    # merge
    cfg = {}
    if os.path.isfile(d.config_path):
        with open(d.config_path, encoding="utf-8") as fh:
            cfg = json.load(fh)
    entry_node = d.entry_get(cfg)
    desired = _mcp_entry()
    if entry_node.get(MCP_SERVER_NAME) == desired:
        result["actions"].append("mcp entry already up-to-date (skipped)")
    else:
        entry_node[MCP_SERVER_NAME] = desired
        os.makedirs(os.path.dirname(d.config_path) or ".", exist_ok=True)
        _atomic_write(d.config_path,
                      json.dumps(cfg, ensure_ascii=False, indent=2).encode("utf-8"))
        result["actions"].append("mcp entry written (%s)" % ".".join(d.mcp_key_path))
    # skill
    src = skill_source_path()
    dst = os.path.join(d.skills_dir, "cross-device-orchestrator", "SKILL.md")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    need_skill = not (os.path.isfile(dst) and
                      open(dst, "rb").read() == open(src, "rb").read())
    if need_skill:
        shutil.copy2(src, dst)
        result["actions"].append("skill -> %s" % dst)
    else:
        result["actions"].append("skill already up-to-date (skipped)")
    result["skill_path"] = dst
    # manifest for rollback
    manifest = _load_manifest()
    manifest[name] = {"config": d.config_path, "backup": backup_path,
                      "skill": dst, "at": int(time.time())}
    _save_manifest(manifest)
    result["ok"] = True
    return result


def uninstall_agent(name):
    """Rollback injections recorded in the manifest. Returns result dict."""
    d = get_agent_def(name)
    result = {"agent": name, "actions": []}
    manifest = _load_manifest()
    rec = manifest.get(name)
    if not rec:
        result["actions"].append("no injection record; nothing to do")
        result["ok"] = True
        return result
    if rec.get("backup") and os.path.isfile(rec["backup"]):
        shutil.copy2(rec["backup"], d.config_path)
        result["actions"].append("config restored from backup")
    else:
        # no backup (config did not exist before): remove our entry
        try:
            with open(d.config_path, encoding="utf-8") as fh:
                cfg = json.load(fh)
            d.entry_remove(cfg)
            _atomic_write(d.config_path,
                          json.dumps(cfg, ensure_ascii=False, indent=2).encode("utf-8"))
            result["actions"].append("mcp entry removed")
        except FileNotFoundError:
            result["actions"].append("config absent; nothing to remove")
    skill_dir = os.path.join(d.skills_dir, "cross-device-orchestrator")
    if os.path.isdir(skill_dir):
        shutil.rmtree(skill_dir)
        result["actions"].append("skill removed (%s)" % skill_dir)
    manifest.pop(name, None)
    _save_manifest(manifest)
    result["ok"] = True
    return result


def _manifest_path():
    return os.path.join(state_dir(), "manifest.json")


def _load_manifest():
    try:
        with open(_manifest_path(), encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def _save_manifest(manifest):
    os.makedirs(state_dir(), exist_ok=True)
    _atomic_write(_manifest_path(),
                  json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))
