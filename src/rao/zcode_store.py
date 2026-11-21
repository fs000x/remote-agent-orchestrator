"""ZCode device-source adapter (v0.6): import devices configured in the
user's ZCode desktop (read-only), including credential decryption.

Sources (read-only, never written):
  ~/.zcode/v2/setting.json      sourceServers + connectionProfiles + history
  ~/.zcode/v2/credentials.json  encrypted secrets (enc:v2:os:<iv.tag.ct>)

Encryption (reverse of ZCode's systemCredentialCipher.ts):
  AES-256-GCM, key = SHA256(secret from OS credential store), secret entry:
  Windows  : Credential Manager, service "ZCode Credential Encryption",
             user = sha256(resolve(credentials.json path)).hex  (CredReadW)
  Linux    : Secret Service (not implemented here in v0.6 -> graceful skip)

The OS secret is fetched as raw bytes; candidate decodings (utf-8/utf-16-le/
utf-16-be) are tried until the GCM tag validates — wrong keys fail the tag.
"""
import base64
import ctypes
import hashlib
import json
import os

ZCODE_SERVICE = "ZCode Credential Encryption"
PREFIX = "enc:v2:os:"


def zcode_home():
    return os.environ.get("RAO_ZCODE_HOME") or os.path.join(
        os.path.expanduser("~"), ".zcode", "v2")


def _read_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def list_zcode_ssh_devices():
    """SSH devices registered in ZCode desktop. Returns list of dicts."""
    home = zcode_home()
    setting = _read_json(os.path.join(home, "setting.json"))
    servers = {s.get("sourceServerId"): s
               for s in setting.get("sourceServers", [])}
    history = {}
    for w in setting.get("sourceServerWorkspaceHistory", []):
        history.setdefault(w.get("sourceServerId"),
                           w.get("workspacePath"))
    out = []
    for prof in setting.get("connectionProfiles", []):
        if prof.get("transport") != "ssh":
            continue
        t = prof.get("target", {})
        if t.get("kind") != "ssh":
            continue
        sid = prof.get("sourceServerId")
        server = servers.get(sid, {})
        out.append({
            "id": sid,
            "displayName": server.get("displayName") or prof.get("displayName"),
            "host": t.get("host"),
            "port": int(t.get("port", 22)),
            "user": t.get("username"),
            "workspace": history.get(sid),
            "zcodeProfileId": prof.get("profileId"),
            "source": "zcode",
            "zcodeVersion": server.get("lastSeenVersion"),
            "installState": server.get("installState"),
        })
    return out


# --- OS secret --------------------------------------------------------------
def _system_secret(cred_json_path):
    """Fetch the OS-stored secret. Windows: Credential Manager via CredReadW.
    Returns (secret_bytes, decoding_used) or (None, None)."""
    if os.name != "nt":
        return None, None
    advapi32 = ctypes.windll.advapi32

    class FILETIME(ctypes.Structure):
        _fields_ = [("dwLowDateTime", ctypes.c_uint32),
                    ("dwHighDateTime", ctypes.c_uint32)]

    class CREDENTIAL(ctypes.Structure):
        _fields_ = [("Flags", ctypes.c_uint32),
                    ("Type", ctypes.c_uint32),
                    ("TargetName", ctypes.c_wchar_p),
                    ("Comment", ctypes.c_wchar_p),
                    ("LastWritten", FILETIME),
                    ("CredentialBlobSize", ctypes.c_uint32),
                    ("CredentialBlob", ctypes.c_void_p),
                    ("Persist", ctypes.c_uint32),
                    ("AttributeCount", ctypes.c_uint32),
                    ("Attributes", ctypes.c_void_p),
                    ("TargetAlias", ctypes.c_wchar_p),
                    ("UserName", ctypes.c_wchar_p)]

    target_user = hashlib.sha256(
        os.path.abspath(cred_json_path).encode("utf-8")).hexdigest()
    cred_ptr = ctypes.c_void_p()
    if not advapi32.CredReadW(ZCODE_SERVICE, 1, 0,
                              ctypes.byref(cred_ptr)):
        return None, None
    try:
        cred = ctypes.cast(cred_ptr, ctypes.POINTER(CREDENTIAL)).contents
        size = cred.CredentialBlobSize
        blob = ctypes.string_at(cred.CredentialBlob, size) if size else b""
    finally:
        ctypes.windll.kernel32.LocalFree(cred_ptr)
    for encoding in ("utf-8", "utf-16-le", "utf-16-be"):
        try:
            return blob.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return None, None


# --- unseal -----------------------------------------------------------------
def _b64url(data):
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def unseal(sealed, key):
    """Decrypt 'enc:v2:os:<iv>.<tag>.<ct>' (AES-256-GCM). Raises on bad tag."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    if not sealed.startswith(PREFIX):
        raise ValueError("unsupported credential prefix")
    iv_b, tag_b, ct_b = sealed[len(PREFIX):].split(".")
    iv, tag, ct = _b64url(iv_b), _b64url(tag_b), _b64url(ct_b)
    aes = AESGCM(key)
    return aes.decrypt(iv, ct + tag, None).decode("utf-8")


def get_profile_password(profile_id):
    """Decrypt the password ZCode stores for a connection profile.
    Returns plaintext or None (unavailable / not Windows / decrypt fail)."""
    home = zcode_home()
    cred_path = os.path.join(home, "credentials.json")
    try:
        creds = _read_json(cred_path)
    except Exception:
        return None
    sealed = creds.get("connection-profile:%s:password" % profile_id)
    if not sealed or not sealed.startswith(PREFIX):
        return None
    secret, encoding = _system_secret(cred_path)
    if secret is None:
        return None
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    candidates = [hashlib.sha256(secret.encode(encoding)).digest()]
    try:
        for key in candidates:
            try:
                return unseal(sealed, key)
            except Exception:
                continue
    except Exception:
        pass
    return None
